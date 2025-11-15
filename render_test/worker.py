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

# ------------------------------ ENV ------------------------------
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

# --------------------------- CLIENTS/LOG -------------------------
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("tick-stream")

# ----------------------- BATCH / FLUSH SETTINGS ------------------
BATCH_MAX = 500           # flush when batch reaches this size
BATCH_FLUSH_SECS = 2      # or after this many seconds pass

_batch = []
_lock = asyncio.Lock()
_stop = asyncio.Event()

# -------------------------- HELPERS ------------------------------
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

# ---------------------- ECONOMIC CALENDAR SCRAPER -------------------
def scrape_trading_economics():
    """
    Scrape economic calendar data from Trading Economics
    Returns list of event dictionaries with proper UTC times
    """
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        log.info("🌐 Fetching Trading Economics calendar...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the calendar table
        table = soup.find('table', {'id': 'calendar'})
        if not table:
            log.error("❌ Could not find calendar table")
            return []
        
        events = []
        current_date = None
        current_day = None
        
        # Process all rows
        for row in table.find_all('tr'):
            # Check if this is a date header row
            thead = row.find_parent('thead')
            if thead and not row.find('td'):
                # Skip hidden header copies
                if 'hidden-head' in thead.get('class', []):
                    continue
                    
                # This is a visible header row with the date
                th = row.find('th')
                if th:
                    date_text = th.get_text(strip=True)
                    try:
                        date_obj = datetime.strptime(date_text, "%A %B %d %Y")
                        current_date = date_obj.strftime("%Y-%m-%d")
                        current_day = date_obj.strftime("%A")
                    except:
                        pass
                continue
            
            # Check if this is a data row
            cols = row.find_all('td')
            if len(cols) < 7 or not current_date:
                continue
            
            try:
                # Column 0: Time (UTC)
                time_str = cols[0].get_text(strip=True)
                
                # Column 3: Country code (2 letters)
                country = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                
                # Column 4: Event name
                event = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                
                # Column 5: Actual
                actual_elem = cols[5].find('span', id='actual') if len(cols) > 5 else None
                actual = actual_elem.get_text(strip=True) if actual_elem else ""
                
                # Column 6: Previous
                previous_elem = cols[6].find('span', id='previous') if len(cols) > 6 else None
                previous = previous_elem.get_text(strip=True) if previous_elem else ""
                
                # Column 7: Consensus
                consensus_elem = cols[7].find(id='consensus') if len(cols) > 7 else None
                consensus = consensus_elem.get_text(strip=True) if consensus_elem else ""
                
                # Forecast = consensus
                forecast = consensus if consensus else ""
                
                # Get importance from event class in time column
                importance = "Medium"
                event_class = cols[0].find('span')
                if event_class and event_class.get('class'):
                    classes = ' '.join(event_class.get('class'))
                    if 'event-3' in classes or 'event-2' in classes:
                        importance = "High"
                    elif 'event-0' in classes:
                        importance = "Low"
                
                # Validate required fields
                if not country or len(country) != 2:
                    continue
                if not event or len(event) < 3:
                    continue
                if not time_str:
                    continue
                
                event_data = {
                    'date': current_date,
                    'day': current_day,
                    'time': time_str,
                    'country': country.upper(),
                    'event': event,
                    'actual': actual if actual else None,
                    'forecast': forecast if forecast else None,
                    'previous': previous if previous else None,
                    'consensus': consensus if consensus else None,
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

# ---------------------- TICK STREAMING ----------------------

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

# ---------------------- MAIN ----------------------

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
