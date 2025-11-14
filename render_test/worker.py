# worker.py
import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client

# ────────────────────────────── ENV ──────────────────────────────
TD_API_KEY   = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Allow override, but default to the table you requested
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# The 16 pairs to stream (same set you used before)
SYMBOLS = (
    "BTC/USD,ETH/USD,XRP/USD,XMR/USD,SOL/USD,BNB/USD,ADA/USD,DOGE/USD,"
    "XAU/USD,EUR/USD,GBP/USD,USD/CAD,GBP/AUD,AUD/CAD,EUR/GBP,USD/JPY"
)

WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

# Economic calendar scraping interval (in seconds)
ECON_SCRAPE_INTERVAL = 3600  # Scrape every hour

# ─────────────────────────── CLIENTS/LOG ─────────────────────────
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("tick-stream")

# ─────────────────────── BATCH / FLUSH SETTINGS ──────────────────
BATCH_MAX = 500           # flush when batch reaches this size
BATCH_FLUSH_SECS = 2      # or after this many seconds pass

_batch = []
_lock = asyncio.Lock()
_stop = asyncio.Event()

# ────────────────────────── HELPERS ──────────────────────────────
def _to_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _to_ts(v):
    """
    Return aware UTC datetime from epoch ms, epoch s, or ISO string.
    """
    if v is None:
        return datetime.now(timezone.utc)

    # 1) epoch ms / s
    try:
        t = float(v)
        if t > 10**12:  # looks like milliseconds
            t /= 1000.0
        return datetime.fromtimestamp(t, tz=timezone.utc)
    except Exception:
        pass

    # 2) ISO-like
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

async def _flush():
    global _batch
    async with _lock:
        if not _batch:
            return
        payload, _batch = _batch, []

    try:
        # You asked to write to 'tickdata' only
        sb.table(SUPABASE_TABLE).insert(payload).execute()
        log.info("✅ Inserted %d rows into %s", len(payload), SUPABASE_TABLE)
    except Exception:
        log.exception("❌ Insert failed, re-queuing %d rows", len(payload))
        async with _lock:
            # Prepend back to the front so we retry these first
            _batch[:0] = payload

async def _periodic_flush():
    while not _stop.is_set():
        try:
            await asyncio.wait_for(_stop.wait(), timeout=BATCH_FLUSH_SECS)
        except asyncio.TimeoutError:
            await _flush()

async def _handle(msg: dict):
    # We only care about real price events
    if msg.get("event") != "price":
        return

    row = {
        "symbol": msg.get("symbol"),
        "ts": _to_ts(msg.get("timestamp")).isoformat(),
        "price": _to_float(msg.get("price")),
        "bid": _to_float(msg.get("bid")),
        "ask": _to_float(msg.get("ask")),
        # different feeds vary the key name for daily volume
        "day_volume": (
            _to_float(msg.get("day_volume"))
            or _to_float(msg.get("dayVolume"))
            or _to_float(msg.get("volume"))
        ),
    }

    # Require symbol + price
    if not row["symbol"] or row["price"] is None:
        return

    async with _lock:
        _batch.append(row)
        if len(_batch) >= BATCH_MAX:
            await _flush()

# ────────────────────── ECONOMIC CALENDAR SCRAPER ────────────────────

def scrape_trading_economics():
    """
    Scrape upcoming economic events from Trading Economics calendar.
    Returns a list of event dictionaries.
    """
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        log.info("🌐 Fetching Trading Economics calendar...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        log.info(f"✅ Got response: {response.status_code}, Content-Length: {len(response.content)}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        events = []
        
        # Find the calendar table
        table = soup.find('table', {'id': 'calendar'})
        if not table:
            log.warning("⚠️ Could not find calendar table with id='calendar'")
            # Try to find any table with class containing 'calendar'
            table = soup.find('table', class_=lambda x: x and 'calendar' in x.lower())
            if not table:
                # Try to find the first large table on the page
                tables = soup.find_all('table')
                log.info(f"Found {len(tables)} tables on the page")
                if tables and len(tables) > 0:
                    table = tables[0]
                    log.info("Using first table found")
                else:
                    log.error("❌ No tables found on Trading Economics page")
                    return events
        
        tbody = table.find('tbody')
        if not tbody:
            log.warning("⚠️ No tbody found, trying to get rows directly from table")
            rows = table.find_all('tr')
        else:
            rows = tbody.find_all('tr')
        
        if not rows:
            log.error("❌ No rows found in table")
            return events
        
        for row in rows:
            try:
                cols = row.find_all('td')
                if len(cols) < 8:
                    continue
                
                # Extract data from columns
                date_str = cols[0].get_text(strip=True)
                time_str = cols[1].get_text(strip=True)
                country = cols[2].find('a')['title'] if cols[2].find('a') else cols[2].get_text(strip=True)
                event = cols[3].get_text(strip=True)
                actual = cols[4].get_text(strip=True)
                forecast = cols[5].get_text(strip=True)
                previous = cols[6].get_text(strip=True)
                
                # Extract importance (usually in first column or as a class/icon)
                importance = "Medium"  # Default
                # Check for importance indicators in the row
                if row.get('class'):
                    row_classes = ' '.join(row.get('class', []))
                    if 'high' in row_classes.lower() or 'calendar-important' in row_classes.lower():
                        importance = "High"
                    elif 'low' in row_classes.lower():
                        importance = "Low"
                # Also check for bull icons or stars in date column
                date_col = cols[0]
                bulls = date_col.find_all('i', class_='fa-star') or date_col.find_all('span', class_='bull')
                if len(bulls) >= 3:
                    importance = "High"
                elif len(bulls) == 2:
                    importance = "Medium"
                elif len(bulls) == 1:
                    importance = "Low"
                
                # Get day of week from date
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d")
                    day = event_date.strftime("%A")
                except:
                    day = ""
                
                event_data = {
                    'date': date_str,
                    'day': day,
                    'time': time_str,
                    'country': country,
                    'event': event,
                    'actual': actual if actual else None,
                    'forecast': forecast if forecast else None,
                    'previous': previous if previous else None,
                    'consensus': None,  # Trading Economics doesn't show consensus separately
                    'importance': importance
                }
                
                events.append(event_data)
                
            except Exception as e:
                log.warning(f"⚠️ Error parsing row: {e}")
                continue
        
        log.info(f"📊 Scraped {len(events)} events from Trading Economics")
        return events
        
    except Exception as e:
        log.error(f"❌ Error scraping Trading Economics: {e}")
        return []

def upsert_economic_events(events):
    """
    Insert or update economic events in Supabase.
    Uses date+time+country+event as unique key.
    """
    if not events:
        return
    
    try:
        # First, get existing events to check what's already there
        for event in events:
            try:
                # Check if event already exists
                existing = sb.table('Economic_calander').select('*').eq(
                    'date', event['date']
                ).eq(
                    'time', event['time']
                ).eq(
                    'country', event['country']
                ).eq(
                    'event', event['event']
                ).execute()
                
                if existing.data:
                    # Update existing event (mainly to update actual values)
                    sb.table('Economic_calander').update(event).eq(
                        'date', event['date']
                    ).eq(
                        'time', event['time']
                    ).eq(
                        'country', event['country']
                    ).eq(
                        'event', event['event']
                    ).execute()
                else:
                    # Insert new event
                    sb.table('Economic_calander').insert(event).execute()
                    
            except Exception as e:
                log.warning(f"⚠️ Error upserting event {event['event']}: {e}")
                continue
        
        log.info(f"✅ Successfully processed {len(events)} economic events")
        
    except Exception as e:
        log.error(f"❌ Error upserting events to Supabase: {e}")

async def economic_calendar_task():
    """
    Periodically scrape and update economic calendar.
    """
    log.info("🗓️ Economic calendar scraper started")
    
    while not _stop.is_set():
        try:
            # Run scraping in executor to avoid blocking
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, scrape_trading_economics)
            
            if events:
                await loop.run_in_executor(None, upsert_economic_events, events)
            
            # Wait for next scrape interval
            await asyncio.sleep(ECON_SCRAPE_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ Error in economic calendar task: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

# ────────────────────── TICK STREAMING ──────────────────────

async def _run_once():
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        max_queue=1000,
    ) as ws:
        # Subscribe to the symbols
        await ws.send(json.dumps({"action": "subscribe", "params": {"symbols": SYMBOLS}}))
        log.info("🚀 Subscribed to: %s", SYMBOLS)

        flusher = asyncio.create_task(_periodic_flush())
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except Exception:
                    # Ignore non-JSON blips
                    continue
                await _handle(data)
        finally:
            _stop.set()
            await _flush()
            flusher.cancel()

async def tick_streaming_task():
    """
    Main tick streaming task with reconnection logic.
    """
    backoff = 1
    while not _stop.is_set():
        try:
            await _run_once()
            backoff = 1
        except Exception as e:
            log.warning("⚠️ WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

# ────────────────────── MAIN ──────────────────────

async def main():
    """
    Run both tick streaming and economic calendar scraping in parallel.
    """
    log.info("🚀 Starting worker with tick streaming and economic calendar scraping")
    
    # Create tasks for both services
    tick_task = asyncio.create_task(tick_streaming_task())
    econ_task = asyncio.create_task(economic_calendar_task())
    
    try:
        # Run both tasks concurrently
        await asyncio.gather(tick_task, econ_task)
    except Exception as e:
        log.error(f"❌ Main loop error: {e}")
    finally:
        _stop.set()
        # Cancel tasks
        tick_task.cancel()
        econ_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
