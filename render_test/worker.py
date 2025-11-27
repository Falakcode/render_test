import os
import json
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Optional, List, Dict
from contextlib import suppress

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser
import openai

# ------------------------------ ENV ------------------------------
TD_API_KEY   = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
FRED_API_KEY = os.environ.get("FRED_API_KEY")

SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# ==================== 202 TRADING PAIRS ====================
# Crypto (39 pairs)
CRYPTO_SYMBOLS = [
    "1INCH/USD", "AAVE/USD", "ADA/BTC", "ADA/USD", "ALGO/USD", "APE/USD", "ARB/USD",
    "ATOM/USD", "AVAX/BTC", "AVAX/USD", "BCH/USD", "BNB/USD", "BTC/USD", "COMP/USD",
    "CRV/USD", "DOGE/BTC", "DOGE/USD", "DOT/USD", "ETC/USD", "ETH/BTC", "ETH/USD",
    "FIL/USD", "GRT/USD", "LDO/USD", "LINK/BTC", "LINK/USD", "LTC/USD", "MANA/USD",
    "NEAR/USD", "OP/USD", "SAND/USD", "SHIB/USD", "SNX/USD", "SOL/BTC", "SOL/USD",
    "SUSHI/USD", "TRX/USD", "UNI/USD", "VET/USD", "XLM/USD", "XMR/USD", "XRP/BTC",
    "XRP/USD", "YFI/USD"
]

# Forex (51 pairs)
FOREX_SYMBOLS = [
    "AUD/CAD", "AUD/CHF", "AUD/JPY", "AUD/NZD", "AUD/SGD",
    "CAD/CHF", "CAD/JPY", "CHF/JPY",
    "EUR/AUD", "EUR/CAD", "EUR/CHF", "EUR/CZK", "EUR/DKK", "EUR/GBP", "EUR/HUF",
    "EUR/JPY", "EUR/NOK", "EUR/NZD", "EUR/PLN", "EUR/SEK", "EUR/TRY", "EUR/USD",
    "GBP/AUD", "GBP/CAD", "GBP/CHF", "GBP/JPY", "GBP/NOK", "GBP/NZD", "GBP/PLN",
    "GBP/SEK", "GBP/TRY", "GBP/USD", "GBP/ZAR",
    "NZD/CAD", "NZD/CHF", "NZD/JPY", "NZD/SGD",
    "USD/ARS", "USD/BRL", "USD/CAD", "USD/CLP", "USD/CNH", "USD/COP", "USD/CZK",
    "USD/DKK", "USD/EGP", "USD/HKD", "USD/HUF", "USD/IDR", "USD/ILS", "USD/INR",
    "USD/JPY", "USD/KRW", "USD/MXN", "USD/MYR", "USD/NOK", "USD/PHP", "USD/PKR",
    "USD/PLN", "USD/SEK", "USD/SGD", "USD/THB", "USD/TRY", "USD/TWD", "USD/ZAR"
]

# Commodities (5 pairs)
COMMODITIES_SYMBOLS = [
    "XAG/USD", "XAU/USD", "XPD/USD", "XPT/USD"
]

# Stocks (65 symbols)
STOCK_SYMBOLS = [
    "ABBV", "ABNB", "ABT", "ADBE", "AMD", "AVGO", "AXP", "BA", "BAC", "BKNG",
    "BLK", "C", "CAT", "COIN", "COP", "COST", "CRM", "CSCO", "CVS", "CVX",
    "DIS", "GE", "GS", "HD", "INTC", "INTU", "JNJ", "JPM", "KO", "LLY",
    "LOW", "MA", "MCD", "META", "MRK", "MS", "NFLX", "NKE", "NOW", "NVDA",
    "ORCL", "PEP", "PFE", "PG", "PYPL", "QCOM", "SBUX", "SCHW", "SNOW", "SQ",
    "TGT", "TMO", "TSLA", "TXN", "UBER", "UNH", "USB", "V", "WFC", "WMT", "XOM"
]

# ETFs & Indices (37 symbols)
ETF_INDEX_SYMBOLS = [
    "AEX", "ARKK", "BKX", "DIA", "FTSE", "GLD", "HSI", "HYG", "IBEX", "IWM",
    "NBI", "NDX", "SLV", "SMI", "SPX", "TLT", "USO", "UTY", "VOO", "VTI",
    "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"
]

# Combine all symbols
ALL_SYMBOLS = CRYPTO_SYMBOLS + FOREX_SYMBOLS + COMMODITIES_SYMBOLS + STOCK_SYMBOLS + ETF_INDEX_SYMBOLS

# For WebSocket subscription (comma-separated string)
SYMBOLS = ",".join(ALL_SYMBOLS)

WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"
TD_REST_URL = "https://api.twelvedata.com/time_series"

# Economic calendar scraping interval (in seconds)
ECON_SCRAPE_INTERVAL = 3600

# FRED API Base URL
FRED_API_BASE = "https://api.stlouisfed.org/fred"

# ==================== BULLETPROOF TICK STREAMING CONFIG ====================
BATCH_MAX = 500
BATCH_FLUSH_SECS = 2
WATCHDOG_TIMEOUT_SECS = 30      # Kill connection if no tick for 30s
CONNECTION_TIMEOUT_SECS = 15     # Timeout for initial connection
MAX_RECONNECT_DELAY = 30         # Max backoff delay
HEALTH_LOG_INTERVAL = 60         # Log health status every 60s

# Gap fill settings
GAP_SCAN_INTERVAL = 300          # Check for gaps every 5 minutes
MAX_GAP_HOURS = 4                # Look back 4 hours for gaps
MIN_GAP_MINUTES = 3              # Only fill gaps > 3 minutes
BACKFILL_RATE_LIMIT = 0.5        # Delay between REST API calls

# --------------------------- CLIENTS/LOG -------------------------
sb = create_client(SUPABASE_URL, SUPABASE_KEY)
deepseek_client = openai.OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com") if DEEPSEEK_API_KEY else None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("worker")

# ==================== BULLETPROOF TICK STREAMING ====================

class BulletproofTickStreamer:
    """
    Bulletproof WebSocket tick streamer with:
    - Watchdog timer (kills stale connections)
    - Heartbeat monitoring
    - Aggressive reconnection
    - Health logging
    - Graceful shutdown
    """
    
    def __init__(self):
        self._batch: List[dict] = []
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_tick_time: datetime = datetime.now(timezone.utc)
        self._tick_count: int = 0
        self._connection_count: int = 0
        self._last_health_log: datetime = datetime.now(timezone.utc)
        self._is_connected: bool = False
        
    def _to_float(self, x) -> Optional[float]:
        """Convert to float preserving precision."""
        if x is None:
            return None
        try:
            return float(Decimal(str(x)))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _to_ts(self, v) -> datetime:
        """Return aware UTC datetime from epoch ms, epoch s, or ISO string."""
        if v is None:
            return datetime.now(timezone.utc)

        try:
            t = float(v)
            if t > 10**12:
                t /= 1000.0
            return datetime.fromtimestamp(t, tz=timezone.utc)
        except Exception:
            pass

        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    async def _flush(self):
        """Flush batch to Supabase."""
        async with self._lock:
            if not self._batch:
                return
            payload, self._batch = self._batch, []

        try:
            sb.table(SUPABASE_TABLE).insert(payload).execute()
            log.info("✅ Inserted %d rows into %s", len(payload), SUPABASE_TABLE)
        except Exception as e:
            log.error("❌ Insert failed: %s - re-queuing %d rows", e, len(payload))
            async with self._lock:
                self._batch[:0] = payload

    async def _periodic_flush(self):
        """Flush batch every BATCH_FLUSH_SECS."""
        while not self._shutdown.is_set():
            await asyncio.sleep(BATCH_FLUSH_SECS)
            await self._flush()

    async def _handle(self, msg: dict):
        """Handle incoming tick message."""
        if msg.get("event") != "price":
            return

        row = {
            "symbol": msg.get("symbol"),
            "ts": self._to_ts(msg.get("timestamp")).isoformat(),
            "price": self._to_float(msg.get("price")),
            "bid": self._to_float(msg.get("bid")),
            "ask": self._to_float(msg.get("ask")),
            "day_volume": (
                self._to_float(msg.get("day_volume"))
                or self._to_float(msg.get("dayVolume"))
                or self._to_float(msg.get("volume"))
            ),
        }

        if not row["symbol"] or row["price"] is None:
            return

        # Update watchdog
        self._last_tick_time = datetime.now(timezone.utc)
        self._tick_count += 1

        async with self._lock:
            self._batch.append(row)
            if len(self._batch) >= BATCH_MAX:
                await self._flush()

    async def _watchdog(self):
        """
        WATCHDOG: Monitor for stale connections.
        If no tick received for WATCHDOG_TIMEOUT_SECS, force close the WebSocket.
        """
        log.info("🐕 Watchdog started (timeout: %ds)", WATCHDOG_TIMEOUT_SECS)
        
        while not self._shutdown.is_set():
            await asyncio.sleep(5)
            
            if not self._is_connected:
                continue
                
            elapsed = (datetime.now(timezone.utc) - self._last_tick_time).total_seconds()
            
            if elapsed > WATCHDOG_TIMEOUT_SECS:
                log.warning("🐕 WATCHDOG: No tick for %.1fs - forcing reconnection!", elapsed)
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                self._is_connected = False

    async def _health_logger(self):
        """Log health status periodically."""
        while not self._shutdown.is_set():
            await asyncio.sleep(HEALTH_LOG_INTERVAL)
            
            elapsed = (datetime.now(timezone.utc) - self._last_tick_time).total_seconds()
            status = "🟢 HEALTHY" if elapsed < 10 else "🟡 STALE" if elapsed < WATCHDOG_TIMEOUT_SECS else "🔴 DEAD"
            
            log.info(
                "💓 HEALTH: %s | Connected: %s | Ticks: %d | Last tick: %.1fs ago | Reconnects: %d | Symbols: %d",
                status,
                self._is_connected,
                self._tick_count,
                elapsed,
                self._connection_count,
                len(ALL_SYMBOLS)
            )

    async def _connect_and_stream(self):
        """Single connection attempt with proper error handling."""
        self._connection_count += 1
        log.info("🔌 Connection attempt #%d...", self._connection_count)
        
        try:
            # Connect with timeout
            self._ws = await asyncio.wait_for(
                websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ),
                timeout=CONNECTION_TIMEOUT_SECS
            )
            
            # Subscribe
            await self._ws.send(json.dumps({
                "action": "subscribe",
                "params": {"symbols": SYMBOLS}
            }))
            
            log.info("🚀 Connected and subscribed to %d symbols", len(ALL_SYMBOLS))
            self._is_connected = True
            self._last_tick_time = datetime.now(timezone.utc)
            
            # Stream messages
            async for raw in self._ws:
                if self._shutdown.is_set():
                    break
                try:
                    data = json.loads(raw)
                    await self._handle(data)
                except json.JSONDecodeError:
                    continue
                    
        except asyncio.TimeoutError:
            log.error("⏱️ Connection timeout after %ds", CONNECTION_TIMEOUT_SECS)
        except websockets.exceptions.ConnectionClosed as e:
            log.warning("🔌 Connection closed: code=%s reason=%s", e.code, e.reason)
        except Exception as e:
            log.error("❌ Connection error: %s", e)
        finally:
            self._is_connected = False
            if self._ws:
                with suppress(Exception):
                    await self._ws.close()
            self._ws = None

    async def run(self):
        """
        Main run loop with bulletproof reconnection.
        Never exits unless shutdown is requested.
        """
        log.info("=" * 60)
        log.info("🚀 BULLETPROOF TICK STREAMER STARTING")
        log.info("=" * 60)
        log.info("📊 Total symbols: %d", len(ALL_SYMBOLS))
        log.info("   Crypto: %d | Forex: %d | Commodities: %d", 
                 len(CRYPTO_SYMBOLS), len(FOREX_SYMBOLS), len(COMMODITIES_SYMBOLS))
        log.info("   Stocks: %d | ETFs/Indices: %d",
                 len(STOCK_SYMBOLS), len(ETF_INDEX_SYMBOLS))
        log.info("🐕 Watchdog timeout: %ds", WATCHDOG_TIMEOUT_SECS)
        log.info("🔄 Max reconnect delay: %ds", MAX_RECONNECT_DELAY)
        log.info("=" * 60)
        
        # Start background tasks
        flusher = asyncio.create_task(self._periodic_flush())
        watchdog = asyncio.create_task(self._watchdog())
        health = asyncio.create_task(self._health_logger())
        
        backoff = 1
        
        try:
            while not self._shutdown.is_set():
                try:
                    await self._connect_and_stream()
                    
                    if self._shutdown.is_set():
                        break
                    
                    # Connection ended - prepare to reconnect
                    log.info("🔄 Reconnecting in %ds...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(MAX_RECONNECT_DELAY, backoff * 2)
                    
                except Exception as e:
                    log.error("💥 Unexpected error in main loop: %s", e)
                    await asyncio.sleep(backoff)
                    backoff = min(MAX_RECONNECT_DELAY, backoff * 2)
                    
                # Reset backoff on successful long connection
                if self._tick_count > 0:
                    elapsed = (datetime.now(timezone.utc) - self._last_tick_time).total_seconds()
                    if elapsed < 5:
                        backoff = 1
                        
        finally:
            log.info("🛑 Shutting down tick streamer...")
            await self._flush()
            flusher.cancel()
            watchdog.cancel()
            health.cancel()
            with suppress(asyncio.CancelledError):
                await flusher
                await watchdog
                await health

    def shutdown(self):
        """Request graceful shutdown."""
        log.info("🛑 Shutdown requested")
        self._shutdown.set()


# Global instance
tick_streamer = BulletproofTickStreamer()


async def tick_streaming_task():
    """Entry point for tick streaming."""
    await tick_streamer.run()


# ====================== ECONOMIC CALENDAR ======================

def scrape_trading_economics():
    """Scrape economic calendar data from Trading Economics"""
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        log.info("📅 Fetching Trading Economics calendar...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': 'calendar'})
        if not table:
            log.error("❌ Could not find calendar table")
            return []

        events = []
        current_date = None
        current_day = None

        for row in table.find_all('tr'):
            thead = row.find_parent('thead')
            if thead and not row.find('td'):
                if 'hidden-head' in thead.get('class', []):
                    continue

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

            cols = row.find_all('td')
            if len(cols) < 7 or not current_date:
                continue

            try:
                time_str = cols[0].get_text(strip=True)
                country = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                event = cols[4].get_text(strip=True) if len(cols) > 4 else ""

                actual_elem = cols[5].find('span', id='actual') if len(cols) > 5 else None
                actual = actual_elem.get_text(strip=True) if actual_elem else ""

                previous_elem = cols[6].find('span', id='previous') if len(cols) > 6 else None
                previous = previous_elem.get_text(strip=True) if previous_elem else ""

                consensus_elem = cols[7].find(id='consensus') if len(cols) > 7 else None
                consensus = consensus_elem.get_text(strip=True) if consensus_elem else ""

                forecast = consensus if consensus else ""

                importance = "Medium"
                event_class = cols[0].find('span')
                if event_class and event_class.get('class'):
                    classes = ' '.join(event_class.get('class'))
                    if 'event-3' in classes or 'event-2' in classes:
                        importance = "High"
                    elif 'event-0' in classes:
                        importance = "Low"

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

        log.info(f"📋 Scraped {len(events)} events from Trading Economics")
        return events

    except Exception as e:
        log.error(f"❌ Error scraping Trading Economics: {e}")
        return []


def upsert_economic_events(events):
    """Insert or update economic events in Supabase."""
    if not events:
        return

    try:
        for event in events:
            try:
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
                    sb.table('Economic_calander').insert(event).execute()

            except Exception as e:
                log.warning(f"⚠️ Error upserting event {event['event']}: {e}")
                continue

        log.info(f"✅ Successfully processed {len(events)} economic events")

    except Exception as e:
        log.error(f"❌ Error upserting events to Supabase: {e}")


async def economic_calendar_task():
    """Periodically scrape and update economic calendar."""
    log.info("📅 Economic calendar scraper started")

    while not tick_streamer._shutdown.is_set():
        try:
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, scrape_trading_economics)

            if events:
                await loop.run_in_executor(None, upsert_economic_events, events)

            await asyncio.sleep(ECON_SCRAPE_INTERVAL)

        except Exception as e:
            log.error(f"❌ Error in economic calendar task: {e}")
            await asyncio.sleep(60)


# ==================== FINANCIAL NEWS SCRAPER ====================
# EXPANDED RSS FEEDS - 30+ sources for comprehensive macro coverage

RSS_FEEDS = [
    # --- BBC News ---
    {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC News (Main)"},
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC News (Business)"},
    {"url": "http://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC News (World)"},
    
    # --- CNBC ---
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC (Top News)"},
    {"url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "name": "CNBC (World)"},
    {"url": "https://www.cnbc.com/id/15837362/device/rss/rss.html", "name": "CNBC (US News)"},
    {"url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "name": "CNBC (Economy)"},
    {"url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "name": "CNBC (Finance)"},
    {"url": "https://www.cnbc.com/id/10001147/device/rss/rss.html", "name": "CNBC (Earnings)"},
    {"url": "https://www.cnbc.com/id/15839135/device/rss/rss.html", "name": "CNBC (Commodities)"},
    {"url": "https://www.cnbc.com/id/19836768/device/rss/rss.html", "name": "CNBC (Energy)"},
    
    # --- MarketWatch ---
    {"url": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines", "name": "MarketWatch (Real-time)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories", "name": "MarketWatch (Top Stories)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/marketpulse", "name": "MarketWatch (Market Pulse)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/StockstoWatch", "name": "MarketWatch (Stocks)"},
    
    # --- Crypto ---
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk"},
    {"url": "https://cointelegraph.com/rss", "name": "Cointelegraph"},
    {"url": "https://bitcoinmagazine.com/.rss/full/", "name": "Bitcoin Magazine"},
    {"url": "https://decrypt.co/feed", "name": "Decrypt"},
    {"url": "https://thedefiant.io/feed", "name": "The Defiant"},
    
    # --- The Guardian ---
    {"url": "https://www.theguardian.com/world/rss", "name": "The Guardian (World)"},
    {"url": "https://www.theguardian.com/uk/business/rss", "name": "The Guardian (Business)"},
    {"url": "https://www.theguardian.com/business/economics/rss", "name": "The Guardian (Economics)"},
    {"url": "https://www.theguardian.com/business/stock-markets/rss", "name": "The Guardian (Markets)"},
    
    # --- Al Jazeera ---
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera"},
    {"url": "https://www.aljazeera.com/economy/rss.xml", "name": "Al Jazeera (Economy)"},
    
    # --- NPR ---
    {"url": "https://feeds.npr.org/1001/rss.xml", "name": "NPR (News)"},
    {"url": "https://feeds.npr.org/1006/rss.xml", "name": "NPR (Business)"},
    {"url": "https://feeds.npr.org/1014/rss.xml", "name": "NPR (Politics)"},
    
    # --- Yahoo Finance ---
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance"},
    
    # --- Investing.com ---
    {"url": "https://www.investing.com/rss/news.rss", "name": "Investing.com (News)"},
    {"url": "https://www.investing.com/rss/news_25.rss", "name": "Investing.com (Economy)"},
    {"url": "https://www.investing.com/rss/news_14.rss", "name": "Investing.com (Forex)"},
    {"url": "https://www.investing.com/rss/news_285.rss", "name": "Investing.com (Commodities)"},
    {"url": "https://www.investing.com/rss/news_301.rss", "name": "Investing.com (Crypto)"},
    
    # --- Economic Policy / Central Banks ---
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve"},
    {"url": "https://www.ecb.europa.eu/rss/press.html", "name": "European Central Bank"},
    {"url": "https://www.bankofengland.co.uk/rss/news", "name": "Bank of England"},
    
    # --- FXStreet (Forex Focus) ---
    {"url": "https://www.fxstreet.com/rss/news", "name": "FXStreet (News)"},
    {"url": "https://www.fxstreet.com/rss/analysis", "name": "FXStreet (Analysis)"},
    
    # --- DailyFX ---
    {"url": "https://www.dailyfx.com/feeds/all", "name": "DailyFX"},
    
    # --- Seeking Alpha ---
    {"url": "https://seekingalpha.com/market_currents.xml", "name": "Seeking Alpha (Market Currents)"},
    {"url": "https://seekingalpha.com/tag/macro-view.xml", "name": "Seeking Alpha (Macro)"},
    
    # --- Zero Hedge (Macro/Alternative) ---
    {"url": "https://feeds.feedburner.com/zerohedge/feed", "name": "Zero Hedge"},
    
    # --- Politico (Policy News) ---
    {"url": "https://www.politico.com/rss/economy.xml", "name": "Politico (Economy)"},
    {"url": "https://www.politico.com/rss/politicopicks.xml", "name": "Politico (Top)"},
    
    # --- The Economist ---
    {"url": "https://www.economist.com/finance-and-economics/rss.xml", "name": "The Economist (Finance)"},
    {"url": "https://www.economist.com/business/rss.xml", "name": "The Economist (Business)"},
    
    # --- Barrons ---
    {"url": "https://www.barrons.com/xml/rss/3_7031.xml", "name": "Barrons"},
    
    # --- Oil & Energy ---
    {"url": "https://oilprice.com/rss/main", "name": "OilPrice.com"},
    
    # --- Metals/Gold ---
    {"url": "https://www.kitco.com/rss/kitco_gold.xml", "name": "Kitco (Gold)"},
]

INCLUDE_KEYWORDS = [
    # Geopolitical
    "war", "attack", "invasion", "military", "strike", "missile", "conflict", "terrorism", "coup",
    "sanctions", "embargo", "nuclear", "drone", "ceasefire", "escalation",
    
    # Political
    "trump", "biden", "election", "president", "prime minister", "government", "resign",
    "impeachment", "congress", "parliament", "vote", "referendum", "policy",
    
    # Central Banks & Monetary Policy
    "fed", "federal reserve", "powell", "ecb", "lagarde", "bank of england", "boe", "bailey",
    "bank of japan", "boj", "ueda", "pboc", "rba", "rate", "interest", "monetary", "qe",
    "quantitative", "tightening", "easing", "hawkish", "dovish", "fomc", "rate hike", "rate cut",
    "basis points", "bps",
    
    # Economic Indicators
    "inflation", "cpi", "ppi", "gdp", "unemployment", "jobs", "nfp", "payroll", "retail sales",
    "economic", "recession", "pce", "manufacturing", "pmi", "ism", "consumer confidence",
    "housing", "jobless claims", "labor market", "growth", "contraction", "deficit", "surplus",
    "trade balance", "current account", "budget",
    
    # Trade & Tariffs
    "tariff", "sanction", "trade war", "import", "export", "ban", "quota", "wto", "trade deal",
    "trade agreement", "supply chain", "protectionism",
    
    # Crypto
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency", "blockchain", "stablecoin",
    "defi", "nft", "altcoin", "binance", "coinbase", "sec crypto", "regulation crypto",
    "bitcoin etf", "halving", "mining",
    
    # Market Movements
    "crash", "plunge", "surge", "rally", "collapse", "soar", "tumble", "selloff", "correction",
    "bull market", "bear market", "all-time high", "ath", "volatility", "vix", "circuit breaker",
    "margin call", "liquidation",
    
    # Commodities
    "oil", "opec", "gold", "crude", "brent", "wti", "commodity", "natural gas", "lng",
    "copper", "silver", "platinum", "palladium", "wheat", "corn", "soybean", "agriculture",
    "opec+", "production cut", "barrel",
    
    # Banking & Finance
    "bank", "banking", "bailout", "liquidity", "credit", "lending", "mortgage", "bond",
    "yield", "treasury", "debt ceiling", "default", "credit rating", "downgrade", "upgrade",
    "bankruptcy", "insolvency", "stress test", "capital requirements", "basel",
    
    # Crisis & Disasters
    "earthquake", "hurricane", "disaster", "emergency", "crisis", "pandemic", "outbreak",
    "flood", "wildfire", "typhoon", "drought", "climate", "shutdown", "blackout",
    
    # Stocks & Earnings
    "earnings", "revenue", "profit", "guidance", "forecast", "miss", "beat", "ipo", "buyback",
    "dividend", "merger", "acquisition", "m&a", "spinoff", "layoff", "restructuring",
    
    # Tech Giants (Market Movers)
    "nvidia", "apple", "microsoft", "amazon", "google", "meta", "tesla", "ai", "artificial intelligence",
    "chip", "semiconductor", "data center",
]

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "cricket", "tennis", "basketball", "baseball", "nfl", "nba",
    "celebrity", "actor", "actress", "movie", "film", "tv show", "television", "entertainment",
    "recipe", "cooking", "fashion", "style", "beauty", "wedding", "divorce", "dating",
    "horoscope", "lottery", "game show", "reality tv", "red carpet",
]


def get_news_scan_interval():
    """Smart scheduling based on market hours."""
    now = datetime.utcnow()
    day_of_week = now.weekday()
    hour_utc = now.hour
    
    # Weekend - scan less frequently
    if day_of_week >= 5:
        return 7200, "weekend"  # 2 hours
    
    # US Market Hours (14:30-21:00 UTC) or Asian Session (23:00-08:00 UTC)
    if (14 <= hour_utc < 21) or (23 <= hour_utc or hour_utc < 8):
        return 600, "active"  # 10 minutes
    
    # European Session (08:00-16:30 UTC)
    if (8 <= hour_utc < 17):
        return 900, "active"  # 15 minutes
    
    return 1800, "quiet"  # 30 minutes


def should_prefilter_article(title: str, summary: str) -> bool:
    """Pre-filter articles based on keywords."""
    text = f"{title} {summary}".lower()

    for exclude_word in EXCLUDE_KEYWORDS:
        if exclude_word in text:
            if "royal bank" in text:
                continue
            return False

    for include_word in INCLUDE_KEYWORDS:
        if include_word in text:
            return True

    return False


def calculate_title_similarity(title1: str, title2: str) -> float:
    return SequenceMatcher(None, title1.lower(), title2.lower()).ratio()


def optimize_articles_for_cost(articles: list) -> list:
    """Optimize articles to reduce AI classification costs."""
    if not articles:
        return []

    log.info(f"💰 Cost optimization: Starting with {len(articles)} articles")

    # Filter articles with images
    articles_with_images = [a for a in articles if a.get('image_url')]
    removed_no_images = len(articles) - len(articles_with_images)
    log.info(f"   Removed {removed_no_images} articles without images")

    # Remove duplicates
    unique_articles = []
    seen_topics = []

    for article in articles_with_images:
        is_duplicate = False
        for seen_title in seen_topics:
            similarity = calculate_title_similarity(article['title'], seen_title)
            if similarity >= 0.70:  # 70% similarity threshold
                is_duplicate = True
                break

        if not is_duplicate:
            unique_articles.append(article)
            seen_topics.append(article['title'])

    removed_similar = len(articles_with_images) - len(unique_articles)
    log.info(f"   Removed {removed_similar} similar articles")

    # Category limits (increased for more coverage)
    category_counts = {'crypto': 0, 'political': 0, 'central_bank': 0, 'market': 0, 'commodity': 0, 'macro': 0}
    category_limits = {'crypto': 4, 'political': 4, 'central_bank': 4, 'market': 4, 'commodity': 3, 'macro': 4}

    crypto_kw = ['bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'solana', 'xrp', 'binance', 'coinbase']
    political_kw = ['trump', 'biden', 'election', 'president', 'white house', 'congress', 'parliament']
    central_bank_kw = ['fed', 'powell', 'ecb', 'lagarde', 'rate', 'monetary', 'fomc', 'boe', 'boj']
    market_kw = ['stock', 'nasdaq', 's&p', 'dow', 'market', 'rally', 'crash', 'earnings']
    commodity_kw = ['oil', 'gold', 'opec', 'crude', 'commodity', 'copper', 'silver']
    macro_kw = ['gdp', 'inflation', 'cpi', 'unemployment', 'jobs', 'pmi', 'recession']

    final_articles = []
    for article in unique_articles:
        text = f"{article['title']} {article['summary']}".lower()

        category = None
        if any(kw in text for kw in crypto_kw):
            category = 'crypto'
        elif any(kw in text for kw in central_bank_kw):
            category = 'central_bank'
        elif any(kw in text for kw in macro_kw):
            category = 'macro'
        elif any(kw in text for kw in political_kw):
            category = 'political'
        elif any(kw in text for kw in commodity_kw):
            category = 'commodity'
        elif any(kw in text for kw in market_kw):
            category = 'market'

        if category and category in category_limits:
            if category_counts[category] >= category_limits[category]:
                continue
            category_counts[category] += 1

        final_articles.append(article)

    removed_by_limit = len(unique_articles) - len(final_articles)
    total_removed = len(articles) - len(final_articles)
    savings_pct = int((total_removed / len(articles)) * 100) if articles else 0

    log.info(f"   Category breakdown: {dict(category_counts)}")
    log.info(f"   💰 Cost savings: {savings_pct}% ({total_removed} articles filtered)")
    log.info(f"   ✅ Final articles to classify: {len(final_articles)}")

    return final_articles


def fetch_rss_feed(feed_url: str, source_name: str) -> list:
    """Fetch and parse RSS feed."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; LondonStrategicEdge/1.0)'}
        response = requests.get(feed_url, headers=headers, timeout=15)
        response.raise_for_status()

        feed = feedparser.parse(response.content)
        articles = []
        cutoff_date = datetime.now() - timedelta(hours=24)

        for entry in feed.entries:
            pub_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])

            if pub_date and pub_date < cutoff_date:
                continue

            title = entry.get('title', '').strip()
            url = entry.get('link', '').strip()
            summary = entry.get('summary', entry.get('description', '')).strip()[:500]

            if not title or not url:
                continue

            image_url = None

            if hasattr(entry, 'media_content') and entry.media_content:
                image_url = entry.media_content[0].get('url')

            if not image_url and hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
                image_url = entry.media_thumbnail[0].get('url')

            if not image_url and hasattr(entry, 'enclosures') and entry.enclosures:
                for enclosure in entry.enclosures:
                    if enclosure.get('type', '').startswith('image/'):
                        image_url = enclosure.get('href') or enclosure.get('url')
                        break

            if not image_url and hasattr(entry, 'links'):
                for link in entry.links:
                    if link.get('type', '').startswith('image/'):
                        image_url = link.get('href')
                        break

            articles.append({
                'title': title,
                'url': url,
                'source': source_name,
                'published': pub_date.isoformat() if pub_date else None,
                'summary': summary,
                'image_url': image_url
            })

        return articles

    except Exception as e:
        log.warning(f"⚠️ RSS feed error ({source_name}): {e}")
        return []


def classify_article_with_deepseek(article: dict) -> dict:
    """Classify article using DeepSeek API."""
    if not deepseek_client:
        log.warning("⚠️ DeepSeek API key not configured")
        return None
        
    try:
        prompt = f"""You are a professional financial news analyst. Analyze this article and determine if it will impact financial markets.

TITLE: {article['title']}
SUMMARY: {article['summary']}
SOURCE: {article['source']}

Respond with ONLY valid JSON (no markdown, no explanation):
{{"is_important": true, "event_type": "war", "impact_level": "high", "affected_symbols": ["BTC/USD", "XAU/USD"], "sentiment": "bearish", "summary": "2-3 sentence market impact summary", "keywords": ["keyword1", "keyword2"]}}

Valid event_types: war, political_shock, economic_data, central_bank, trade_war, commodity_shock, market_crash, crypto_crash, crypto_rally, natural_disaster, bank_crisis, energy_crisis, pandemic_health, market_rally, earnings, regulatory
Valid impact_levels: critical, high, medium, low
Valid sentiments: bullish, bearish, neutral"""

        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.1
        )

        response_text = response.choices[0].message.content.strip()
        
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()
        if response_text.endswith("```"):
            response_text = response_text[:-3].strip()

        return json.loads(response_text)

    except json.JSONDecodeError as e:
        log.warning(f"⚠️ DeepSeek JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"⚠️ DeepSeek API error: {e}")
        return None


def store_news_article(article: dict, classification: dict) -> bool:
    """Store classified news article in Supabase."""
    try:
        data = {
            'title': article['title'],
            'summary': classification['summary'],
            'url': article['url'],
            'source': article['source'],
            'published_at': article['published'],
            'image_url': article.get('image_url'),
            'event_type': classification['event_type'],
            'impact_level': classification['impact_level'],
            'affected_symbols': classification['affected_symbols'],
            'sentiment': classification['sentiment'],
            'keywords': classification['keywords'],
            'is_breaking': classification['impact_level'] in ['critical', 'high']
        }

        sb.table('financial_news').insert(data).execute()
        return True

    except Exception as e:
        if 'duplicate key value' in str(e).lower() or 'unique constraint' in str(e).lower():
            return False
        else:
            log.warning(f"⚠️ Database error storing news: {e}")
            return False


async def financial_news_task():
    """Financial news scraping task."""
    log.info("📰 Financial news scraper started")
    log.info(f"   RSS feeds: {len(RSS_FEEDS)}")

    while not tick_streamer._shutdown.is_set():
        try:
            interval, session_name = get_news_scan_interval()
            session_emoji = "🔥" if session_name == "active" else "😴" if session_name == "quiet" else "🌙"
            
            log.info(f"📰 Starting news scan ({session_emoji} {session_name} session)")

            all_articles = []
            loop = asyncio.get_event_loop()

            for feed in RSS_FEEDS:
                articles = await loop.run_in_executor(None, fetch_rss_feed, feed['url'], feed['name'])
                all_articles.extend(articles)
                await asyncio.sleep(0.3)  # Be nice to servers

            log.info(f"📋 Fetched {len(all_articles)} total articles")

            # Deduplicate by URL
            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article['url'] not in seen_urls:
                    seen_urls.add(article['url'])
                    unique_articles.append(article)

            log.info(f"🔍 After deduplication: {len(unique_articles)} unique articles")

            # Pre-filter
            filtered_articles = [
                a for a in unique_articles
                if should_prefilter_article(a['title'], a['summary'])
            ]

            log.info(f"🎯 After pre-filtering: {len(filtered_articles)} articles")

            # Cost optimization
            filtered_articles = optimize_articles_for_cost(filtered_articles)

            # Classify and store
            stored_count = 0
            classified_count = 0

            for article in filtered_articles:
                classification = await loop.run_in_executor(None, classify_article_with_deepseek, article)
                classified_count += 1

                if not classification:
                    continue

                if not classification.get('is_important') or classification.get('impact_level') not in ['critical', 'high']:
                    continue

                impact_emoji = "🚨" if classification['impact_level'] == 'critical' else "⚡"
                log.info(f"{impact_emoji} IMPORTANT: {article['title'][:60]}... ({classification['impact_level']})")

                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored_count += 1

                await asyncio.sleep(1)

            log.info(f"✅ News cycle complete: {stored_count} stored, {classified_count} classified")
            log.info(f"😴 Next scan in {interval // 60} minutes ({session_emoji} {session_name})")
            
            await asyncio.sleep(interval)

        except Exception as e:
            log.error(f"❌ Error in financial news task: {e}")
            await asyncio.sleep(60)


# ==================== GAP FILL SERVICE ====================

def symbol_to_table(symbol: str) -> str:
    """Convert symbol to table name: BTC/USD -> candles_btc_usd, NVDA -> candles_nvda"""
    return f"candles_{symbol.lower().replace('/', '_')}"


def detect_gaps(symbol: str, lookback_hours: int = MAX_GAP_HOURS) -> List[tuple]:
    """Detect gaps in candle data for a symbol."""
    table_name = symbol_to_table(symbol)
    
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        
        result = sb.table(table_name).select('timestamp').gte(
            'timestamp', since
        ).order('timestamp', desc=False).execute()
        
        if not result.data or len(result.data) < 2:
            return []
        
        gaps = []
        timestamps = [datetime.fromisoformat(r['timestamp'].replace('Z', '+00:00')) for r in result.data]
        
        for i in range(1, len(timestamps)):
            prev_ts = timestamps[i - 1]
            curr_ts = timestamps[i]
            gap_minutes = (curr_ts - prev_ts).total_seconds() / 60
            
            if gap_minutes > MIN_GAP_MINUTES:
                gap_start = prev_ts + timedelta(minutes=1)
                gap_end = curr_ts - timedelta(minutes=1)
                gaps.append((gap_start, gap_end))
                
        return gaps
        
    except Exception as e:
        # Table might not exist yet
        return []


def fetch_candles_rest(symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
    """Fetch historical candles from TwelveData REST API."""
    try:
        params = {
            "symbol": symbol,
            "interval": "1min",
            "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
            "apikey": TD_API_KEY,
            "format": "JSON",
            "timezone": "UTC"
        }
        
        response = requests.get(TD_REST_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "values" not in data:
            if "message" in data:
                log.warning(f"⚠️ TwelveData: {data.get('message', 'No data')}")
            return []
        
        candles = []
        for v in data["values"]:
            try:
                candles.append({
                    "timestamp": datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
                    "open": float(Decimal(str(v["open"]))),
                    "high": float(Decimal(str(v["high"]))),
                    "low": float(Decimal(str(v["low"]))),
                    "close": float(Decimal(str(v["close"]))),
                    "symbol": symbol
                })
            except Exception:
                continue
        
        return candles
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            log.warning(f"⚠️ Rate limited by TwelveData")
        else:
            log.error(f"❌ TwelveData API error: {e}")
        return []
    except Exception as e:
        log.error(f"❌ Error fetching candles for {symbol}: {e}")
        return []


def insert_candles(symbol: str, candles: List[Dict]) -> int:
    """Insert candles using upsert."""
    if not candles:
        return 0
    
    table_name = symbol_to_table(symbol)
    
    try:
        rows = [
            {
                "timestamp": c["timestamp"].isoformat(),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "symbol": c["symbol"]
            }
            for c in candles
        ]
        
        sb.table(table_name).upsert(rows, on_conflict="timestamp").execute()
        return len(rows)
        
    except Exception as e:
        log.error(f"❌ Error inserting candles for {symbol}: {e}")
        return 0


async def fill_gaps_for_symbol(symbol: str, gaps: List[tuple]) -> int:
    """Fill all detected gaps for a symbol."""
    total_filled = 0
    
    for gap_start, gap_end in gaps:
        gap_minutes = int((gap_end - gap_start).total_seconds() / 60) + 1
        log.info(f"🔧 Filling {symbol}: {gap_start.strftime('%H:%M')}-{gap_end.strftime('%H:%M')} ({gap_minutes}min)")
        
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(None, fetch_candles_rest, symbol, gap_start, gap_end)
        
        if candles:
            inserted = await loop.run_in_executor(None, insert_candles, symbol, candles)
            total_filled += inserted
            log.info(f"✅ Filled {inserted} candles for {symbol}")
        
        await asyncio.sleep(BACKFILL_RATE_LIMIT)
    
    return total_filled


async def startup_backfill(hours: int = 2):
    """On startup, backfill last N hours to catch gaps from downtime."""
    log.info(f"🚀 Running startup backfill for last {hours} hours...")
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    
    total_candles = 0
    loop = asyncio.get_event_loop()
    
    # Only backfill a subset of important symbols to avoid rate limits
    priority_symbols = CRYPTO_SYMBOLS[:8] + FOREX_SYMBOLS[:8] + COMMODITIES_SYMBOLS + STOCK_SYMBOLS[:10]
    
    for symbol in priority_symbols:
        log.info(f"⬇️ Backfilling {symbol}...")
        
        candles = await loop.run_in_executor(None, fetch_candles_rest, symbol, start_time, end_time)
        
        if candles:
            inserted = await loop.run_in_executor(None, insert_candles, symbol, candles)
            total_candles += inserted
        
        await asyncio.sleep(BACKFILL_RATE_LIMIT)
    
    log.info(f"🎉 Startup backfill complete: {total_candles} total candles")


async def gap_fill_task():
    """Periodic gap detection and filling."""
    log.info("🔧 Gap fill service started")
    log.info(f"   Scan interval: {GAP_SCAN_INTERVAL}s | Lookback: {MAX_GAP_HOURS}h")
    
    # Run startup backfill first
    await startup_backfill(hours=2)
    
    while not tick_streamer._shutdown.is_set():
        try:
            log.info("🔍 Scanning for gaps...")
            
            total_gaps = 0
            total_filled = 0
            
            for symbol in ALL_SYMBOLS:
                gaps = detect_gaps(symbol)
                
                if gaps:
                    total_gaps += len(gaps)
                    filled = await fill_gaps_for_symbol(symbol, gaps)
                    total_filled += filled
                    await asyncio.sleep(BACKFILL_RATE_LIMIT)
            
            if total_gaps > 0:
                log.info(f"📊 Gap scan: {total_gaps} gaps found, {total_filled} candles filled")
            else:
                log.info("✅ No gaps detected")
            
            await asyncio.sleep(GAP_SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ Gap fill error: {e}")
            await asyncio.sleep(60)


# ==================== FRED MACRO DATA ====================

def fetch_fred_series_no_vintage(
    series_id: str,
    observation_start: str,
    observation_end: str,
    limit: Optional[int] = None
) -> List[Dict]:
    """Fetch FRED data WITHOUT vintage constraints."""
    if not FRED_API_KEY:
        log.error("❌ FRED_API_KEY not set!")
        return []
    
    url = f"{FRED_API_BASE}/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': observation_start,
        'observation_end': observation_end,
        'sort_order': 'desc'
    }
    
    if limit:
        params['limit'] = limit
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            log.warning(f"No observations found for {series_id}")
            return []
        
        observations = [
            {
                'date': obs['date'], 
                'value': obs['value']
            }
            for obs in data['observations']
            if obs['value'] != '.'
        ]
        
        return observations
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            log.warning(f"⚠️ FRED API 400 error for {series_id}")
        else:
            log.error(f"Error fetching FRED series {series_id}: {e}")
        return []
    except Exception as e:
        log.error(f"Error fetching FRED series {series_id}: {e}")
        return []


def get_active_indicators() -> List[Dict]:
    """Get all active macro indicators from metadata table"""
    try:
        result = sb.table('macro_indicator_metadata').select('*').eq('is_active', True).execute()
        return result.data if result.data else []
    except Exception as e:
        log.error(f"Error fetching indicator metadata: {e}")
        return []


async def macro_data_task():
    """Event-driven macro data updater."""
    log.info("📊 Macro data task started")
    
    while not tick_streamer._shutdown.is_set():
        try:
            log.info("🔍 Checking for macro data updates...")
            await asyncio.sleep(3600)  # Check hourly
        except Exception as e:
            log.error(f"❌ Error in macro data task: {e}")
            await asyncio.sleep(300)


# ====================== MAIN ======================

async def main():
    """Run all tasks in parallel with graceful shutdown."""
    log.info("=" * 70)
    log.info("🚀 LONDON STRATEGIC EDGE - WORKER")
    log.info("=" * 70)
    log.info("   1️⃣ Tick streaming (TwelveData) - %d symbols", len(ALL_SYMBOLS))
    log.info("   2️⃣ Economic calendar (Trading Economics)")
    log.info("   3️⃣ Financial news (%d RSS feeds + DeepSeek)", len(RSS_FEEDS))
    log.info("   4️⃣ Macro data (FRED API)")
    log.info("   5️⃣ Gap detection & backfill (AUTO-HEALING)")
    log.info("=" * 70)
    log.info("📊 Symbol breakdown:")
    log.info("   Crypto: %d | Forex: %d | Commodities: %d",
             len(CRYPTO_SYMBOLS), len(FOREX_SYMBOLS), len(COMMODITIES_SYMBOLS))
    log.info("   Stocks: %d | ETFs/Indices: %d",
             len(STOCK_SYMBOLS), len(ETF_INDEX_SYMBOLS))
    log.info("=" * 70)

    # Setup graceful shutdown
    def signal_handler(sig, frame):
        log.info(f"🛑 Received signal {sig}, initiating shutdown...")
        tick_streamer.shutdown()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Create tasks
    tasks = [
        asyncio.create_task(tick_streaming_task(), name="tick_stream"),
        asyncio.create_task(economic_calendar_task(), name="econ_calendar"),
        asyncio.create_task(financial_news_task(), name="news"),
        asyncio.create_task(macro_data_task(), name="macro"),
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log.error(f"❌ Main loop error: {e}")
    finally:
        tick_streamer.shutdown()
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        log.info("👋 Worker shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("👋 Goodbye!")
        sys.exit(0)
