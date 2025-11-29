#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v5.0
================================================================================
Production-grade data pipeline with AGGRESSIVE self-healing.

FIXES in v5.0:
  - Watchdog now FORCES immediate reconnect (no passive waiting)
  - Added recv() timeout to prevent infinite blocking
  - Added heartbeat sending to keep connection alive
  - Better connection state tracking
  - Reconnect immediately on ANY error (no long backoff on first retry)

TASKS:
  1. Tick Streaming     - TwelveData WebSocket (237 symbols)
  2. Economic Calendar  - Trading Economics (double-scrape every 15 min)
  3. Financial News     - 50+ RSS feeds + DeepSeek AI classification
  4. Gap Fill Service   - Auto-detect and repair missing candle data
  5. Bond Yields        - G20 sovereign yields (every 30 minutes)
================================================================================
"""

import os
import json
import asyncio
import logging
import signal
import sys
import re
import time
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

# ============================================================================
#                              ENVIRONMENT
# ============================================================================

TD_API_KEY = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")

SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# ============================================================================
#                           TRADING SYMBOLS (237)
# ============================================================================

# Crypto (44 pairs)
CRYPTO_SYMBOLS = [
    "1INCH/USD", "AAVE/USD", "ADA/BTC", "ADA/USD", "ALGO/USD", "APE/USD", "ARB/USD",
    "ATOM/USD", "AVAX/BTC", "AVAX/USD", "BCH/USD", "BNB/USD", "BTC/USD", "COMP/USD",
    "CRV/USD", "DOGE/BTC", "DOGE/USD", "DOT/USD", "ETC/USD", "ETH/BTC", 
    "ETH/USD", "FIL/USD", "GRT/USD", "LDO/USD", "LINK/BTC", "LINK/USD", "LTC/USD", 
    "MANA/USD", "NEAR/USD", "OP/USD", "SAND/USD", "SHIB/USD", "SNX/USD", "SOL/BTC", 
    "SOL/USD", "SUSHI/USD", "TRX/USD", "UNI/USD", "VET/USD", "XLM/USD", "XMR/USD", 
    "XRP/BTC", "XRP/USD", "YFI/USD"
]

# Forex (69 pairs) - Added: AUD/USD, HKD/JPY, NZD/USD, SGD/JPY, USD/CHF
FOREX_SYMBOLS = [
    "AUD/CAD", "AUD/CHF", "AUD/JPY", "AUD/NZD", "AUD/SGD", "AUD/USD",
    "CAD/CHF", "CAD/JPY", "CHF/JPY",
    "EUR/AUD", "EUR/CAD", "EUR/CHF", "EUR/CZK", "EUR/DKK", "EUR/GBP", "EUR/HUF",
    "EUR/JPY", "EUR/NOK", "EUR/NZD", "EUR/PLN", "EUR/SEK", "EUR/TRY", "EUR/USD",
    "GBP/AUD", "GBP/CAD", "GBP/CHF", "GBP/JPY", "GBP/NOK", "GBP/NZD", "GBP/PLN",
    "GBP/SEK", "GBP/TRY", "GBP/USD", "GBP/ZAR",
    "HKD/JPY",
    "NZD/CAD", "NZD/CHF", "NZD/JPY", "NZD/SGD", "NZD/USD",
    "SGD/JPY",
    "USD/ARS", "USD/BRL", "USD/CAD", "USD/CHF", "USD/CLP", "USD/CNH", "USD/COP", 
    "USD/CZK", "USD/DKK", "USD/EGP", "USD/HKD", "USD/HUF", "USD/IDR", "USD/ILS", 
    "USD/INR", "USD/JPY", "USD/KRW", "USD/MXN", "USD/MYR", "USD/NOK", "USD/PHP", 
    "USD/PKR", "USD/PLN", "USD/SEK", "USD/SGD", "USD/THB", "USD/TRY", "USD/TWD", 
    "USD/ZAR"
]

# Commodities (4 pairs)
COMMODITIES_SYMBOLS = [
    "XAG/USD", "XAU/USD", "XPD/USD", "XPT/USD"
]

# Stocks (92 symbols) - Added 31: AAPL, AMAT, AMGN, AMZN, ARM, ASML, BMY, DASH, DDOG, DE, GOOG, 
#                       GOOGL, HON, KLAC, LRCX, LYFT, MDB, MSFT, MSTR, MU, NET, OXY, 
#                       PLTR, QQQ, ROKU, SHOP, SLB, SPY, TSM, ZM
STOCK_SYMBOLS = [
    "AAPL", "ABBV", "ABNB", "ABT", "ADBE", "AMAT", "AMD", "AMGN", "AMZN", "ARM",
    "ASML", "AVGO", "AXP", "BA", "BAC", "BKNG", "BLK", "BMY", "C", "CAT",
    "COIN", "COP", "COST", "CRM", "CSCO", "CVS", "CVX", "DASH", "DDOG", "DE", 
    "DIS", "GE", "GOOG", "GOOGL", "GS", "HD", "HON", "INTC", "INTU", "JNJ", 
    "JPM", "KLAC", "KO", "LLY", "LOW", "LRCX", "LYFT", "MA", "MCD", "MDB", 
    "META", "MRK", "MS", "MSFT", "MSTR", "MU", "NET", "NFLX", "NKE", "NOW", 
    "NVDA", "ORCL", "OXY", "PEP", "PFE", "PG", "PLTR", "PYPL", "QCOM", "QQQ", 
    "ROKU", "SBUX", "SCHW", "SHOP", "SLB", "SNOW", "SQ", "SPY", "TGT", "TLT", 
    "TMO", "TSLA", "TSM", "TXN", "UBER", "UNH", "USB", "V", "WFC", "WMT", 
    "XOM", "ZM"
]

# TO (32 symbols - added EEM, LQD, SHY, VIX):
ETF_INDEX_SYMBOLS = [
    "AEX", "ARKK", "BKX", "DIA", "EEM", "FTSE", "GLD", "HSI", "HYG", "IBEX", 
    "IWM", "LQD", "NBI", "NDX", "SHY", "SLV", "SMI", "SPX", "USO", "UTY", 
    "VIX", "VOO", "VTI", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"
]

# Combined symbols
ALL_SYMBOLS = CRYPTO_SYMBOLS + FOREX_SYMBOLS + COMMODITIES_SYMBOLS + STOCK_SYMBOLS + ETF_INDEX_SYMBOLS
SYMBOLS = ",".join(ALL_SYMBOLS)

# TwelveData URLs
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"
TD_REST_URL = "https://api.twelvedata.com/time_series"

# ============================================================================
#                              CONFIGURATION
# ============================================================================

# Tick Streaming - AGGRESSIVE SETTINGS
BATCH_MAX = 500
BATCH_FLUSH_SECS = 2
WATCHDOG_TIMEOUT_SECS = 30          # Kill connection if no tick for 30s
RECV_TIMEOUT_SECS = 15              # Timeout for each recv() call - NEW!
CONNECTION_TIMEOUT_SECS = 15
MAX_RECONNECT_DELAY = 30
HEALTH_LOG_INTERVAL = 60
HEARTBEAT_INTERVAL_SECS = 10        # Send heartbeat every 10s - NEW!

# Gap Fill
GAP_SCAN_INTERVAL = 300
MAX_GAP_HOURS = 4
MIN_GAP_MINUTES = 3
BACKFILL_RATE_LIMIT = 0.5

# Bond Yields
BOND_SCRAPE_INTERVAL = 1800  # 30 minutes

# ============================================================================
#                           CLIENTS & LOGGING
# ============================================================================

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
deepseek_client = openai.OpenAI(
    api_key=DEEPSEEK_API_KEY, 
    base_url="https://api.deepseek.com"
) if DEEPSEEK_API_KEY else None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("worker")


# ============================================================================
#                    TASK 1: BULLETPROOF TICK STREAMING v5.0
# ============================================================================

class BulletproofTickStreamer:
    """
    Bulletproof WebSocket tick streamer v5.0 with AGGRESSIVE reconnection:
    
    FIXES:
    - Watchdog forces IMMEDIATE reconnect (sets _force_reconnect flag)
    - recv() has timeout to prevent infinite blocking
    - Heartbeat task keeps connection alive
    - Any error = instant reconnect (no long backoff on first try)
    - Tracks last activity (not just last tick) to handle quiet markets
    """
    
    def __init__(self):
        self._batch: List[dict] = []
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_tick_time: datetime = datetime.now(timezone.utc)
        self._last_activity_time: datetime = datetime.now(timezone.utc)  # NEW: Any WS activity
        self._tick_count: int = 0
        self._connection_count: int = 0
        self._last_health_log: datetime = datetime.now(timezone.utc)
        self._is_connected: bool = False
        self._force_reconnect: bool = False  # NEW: Watchdog sets this to force reconnect
        
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
        """Handle incoming WebSocket message."""
        event_type = msg.get("event")
        
        # Update activity time for ANY message (not just price)
        self._last_activity_time = datetime.now(timezone.utc)
        
        # Handle different event types
        if event_type == "subscribe-status":
            success = msg.get("success", [])
            fails = msg.get("fails", [])
            log.info("📡 Subscribe status: %d success, %d fails", len(success), len(fails))
            if fails:
                log.warning("⚠️ Failed symbols: %s", fails[:10])  # Show first 10
            return
            
        if event_type == "heartbeat":
            log.debug("💓 Heartbeat received")
            return
            
        if event_type != "price":
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

        # Update watchdog timer for price events
        self._last_tick_time = datetime.now(timezone.utc)
        self._tick_count += 1

        async with self._lock:
            self._batch.append(row)
            if len(self._batch) >= BATCH_MAX:
                await self._flush()

    async def _watchdog(self):
        """
        AGGRESSIVE WATCHDOG: Force reconnect if connection appears dead.
        
        Checks both:
        - Last tick time (for active trading)
        - Last activity time (for any WS message)
        
        If both are stale, FORCE immediate reconnect.
        """
        log.info("🐕 Watchdog started (timeout: %ds)", WATCHDOG_TIMEOUT_SECS)
        
        while not self._shutdown.is_set():
            await asyncio.sleep(5)  # Check every 5 seconds
            
            if not self._is_connected:
                continue
            
            now = datetime.now(timezone.utc)
            tick_elapsed = (now - self._last_tick_time).total_seconds()
            activity_elapsed = (now - self._last_activity_time).total_seconds()
            
            # If no activity at all for timeout period, connection is dead
            if activity_elapsed > WATCHDOG_TIMEOUT_SECS:
                log.warning("🐕 WATCHDOG TRIGGERED: No activity for %.1fs - FORCING RECONNECT!", activity_elapsed)
                self._force_reconnect = True  # Signal main loop to reconnect
                self._is_connected = False
                
                # Force close the WebSocket
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None
                    
            # If we have activity but no ticks for a long time, log warning (might be market closed)
            elif tick_elapsed > WATCHDOG_TIMEOUT_SECS * 2:
                log.warning("🐕 WATCHDOG: No ticks for %.1fs (but connection active - market may be closed)", tick_elapsed)

    async def _health_logger(self):
        """Log health status periodically."""
        while not self._shutdown.is_set():
            await asyncio.sleep(HEALTH_LOG_INTERVAL)
            
            tick_elapsed = (datetime.now(timezone.utc) - self._last_tick_time).total_seconds()
            activity_elapsed = (datetime.now(timezone.utc) - self._last_activity_time).total_seconds()
            
            if activity_elapsed < 10:
                status = "🟢 HEALTHY"
            elif activity_elapsed < WATCHDOG_TIMEOUT_SECS:
                status = "🟡 STALE"
            else:
                status = "🔴 DEAD"
            
            log.info(
                "💓 HEALTH: %s | Connected: %s | Ticks: %d | Last tick: %.1fs ago | Last activity: %.1fs ago | Reconnects: %d | Symbols: %d",
                status,
                self._is_connected,
                self._tick_count,
                tick_elapsed,
                activity_elapsed,
                self._connection_count,
                len(ALL_SYMBOLS)
            )

    async def _heartbeat_sender(self):
        """
        Send periodic heartbeat to keep connection alive.
        TwelveData may drop idle connections - this prevents that.
        """
        while not self._shutdown.is_set():
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECS)
            
            if self._is_connected and self._ws:
                try:
                    await self._ws.send(json.dumps({"action": "heartbeat"}))
                    log.debug("💓 Heartbeat sent")
                except Exception as e:
                    log.warning("⚠️ Failed to send heartbeat: %s", e)

    async def _connect_and_stream(self):
        """
        Single connection attempt with AGGRESSIVE error handling.
        Uses recv() timeout to prevent infinite blocking.
        """
        self._connection_count += 1
        self._force_reconnect = False  # Reset flag
        
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
            self._last_activity_time = datetime.now(timezone.utc)
            
            # Stream messages with TIMEOUT on each recv()
            while not self._shutdown.is_set() and not self._force_reconnect:
                try:
                    # Use timeout on recv() to prevent infinite blocking
                    raw = await asyncio.wait_for(
                        self._ws.recv(),
                        timeout=RECV_TIMEOUT_SECS
                    )
                    data = json.loads(raw)
                    await self._handle(data)
                    
                except asyncio.TimeoutError:
                    # No message received within timeout - this is OK during quiet markets
                    # But update activity time to show we're still trying
                    log.debug("⏱️ recv() timeout - connection still alive, no new data")
                    # Send a heartbeat to check if connection is alive
                    try:
                        await self._ws.send(json.dumps({"action": "heartbeat"}))
                        self._last_activity_time = datetime.now(timezone.utc)
                    except Exception:
                        log.warning("⚠️ Heartbeat failed after recv timeout - connection may be dead")
                        break
                        
                except json.JSONDecodeError:
                    continue
                    
        except asyncio.TimeoutError:
            log.error("⏱️ Connection timeout after %ds", CONNECTION_TIMEOUT_SECS)
        except websockets.exceptions.ConnectionClosed as e:
            log.warning("🔌 Connection closed: code=%s reason=%s", e.code, e.reason)
        except websockets.exceptions.ConnectionClosedError as e:
            log.warning("🔌 Connection closed with error: code=%s reason=%s", e.code, e.reason)
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
        Main run loop with AGGRESSIVE reconnection.
        
        KEY FIXES:
        - First reconnect attempt is IMMEDIATE (no backoff)
        - Watchdog can force reconnect via _force_reconnect flag
        - Never exits unless shutdown is requested
        """
        log.info("=" * 60)
        log.info("🚀 BULLETPROOF TICK STREAMER v5.0 STARTING")
        log.info("=" * 60)
        log.info("📊 Total symbols: %d", len(ALL_SYMBOLS))
        log.info("   Crypto: %d | Forex: %d | Commodities: %d", 
                 len(CRYPTO_SYMBOLS), len(FOREX_SYMBOLS), len(COMMODITIES_SYMBOLS))
        log.info("   Stocks: %d | ETFs/Indices: %d",
                 len(STOCK_SYMBOLS), len(ETF_INDEX_SYMBOLS))
        log.info("🐕 Watchdog timeout: %ds", WATCHDOG_TIMEOUT_SECS)
        log.info("⏱️ Recv timeout: %ds", RECV_TIMEOUT_SECS)
        log.info("💓 Heartbeat interval: %ds", HEARTBEAT_INTERVAL_SECS)
        log.info("🔄 Max reconnect delay: %ds", MAX_RECONNECT_DELAY)
        log.info("=" * 60)
        
        # Start background tasks
        flusher = asyncio.create_task(self._periodic_flush())
        watchdog = asyncio.create_task(self._watchdog())
        health = asyncio.create_task(self._health_logger())
        heartbeat = asyncio.create_task(self._heartbeat_sender())
        
        backoff = 1
        consecutive_failures = 0
        
        try:
            while not self._shutdown.is_set():
                try:
                    start_time = datetime.now(timezone.utc)
                    
                    await self._connect_and_stream()
                    
                    if self._shutdown.is_set():
                        break
                    
                    # Calculate how long the connection lasted
                    connection_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                    
                    # If connection lasted more than 60 seconds, reset backoff
                    if connection_duration > 60:
                        backoff = 1
                        consecutive_failures = 0
                        log.info("✅ Connection lasted %.0fs - resetting backoff", connection_duration)
                    else:
                        consecutive_failures += 1
                        
                    # AGGRESSIVE RECONNECTION:
                    # - First 3 attempts: immediate retry (1 second)
                    # - After that: exponential backoff
                    if consecutive_failures <= 3:
                        log.info("🔄 Quick reconnect in 1s (attempt %d)...", consecutive_failures)
                        await asyncio.sleep(1)
                    else:
                        log.info("🔄 Reconnecting in %ds (attempt %d)...", backoff, consecutive_failures)
                        await asyncio.sleep(backoff)
                        backoff = min(MAX_RECONNECT_DELAY, backoff * 2)
                    
                except Exception as e:
                    log.error("💥 Unexpected error in main loop: %s", e)
                    consecutive_failures += 1
                    await asyncio.sleep(min(backoff, 5))  # Quick retry on unexpected errors
                        
        finally:
            log.info("🛑 Shutting down tick streamer...")
            await self._flush()
            flusher.cancel()
            watchdog.cancel()
            health.cancel()
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await flusher
                await watchdog
                await health
                await heartbeat

    def shutdown(self):
        """Request graceful shutdown."""
        log.info("🛑 Shutdown requested")
        self._shutdown.set()


# Global instance
tick_streamer = BulletproofTickStreamer()


async def tick_streaming_task():
    """Entry point for tick streaming."""
    await tick_streamer.run()


# ============================================================================
#                    TASK 2: ECONOMIC CALENDAR (DOUBLE-SCRAPE)
# ============================================================================

def scrape_trading_economics() -> List[Dict]:
    """Scrape economic calendar data from Trading Economics."""
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

                # Determine importance from CSS classes
                importance = "Medium"
                event_class = cols[0].find('span')
                if event_class and event_class.get('class'):
                    classes = ' '.join(event_class.get('class'))
                    if 'event-3' in classes or 'event-2' in classes:
                        importance = "High"
                    elif 'event-0' in classes:
                        importance = "Low"

                # Validation
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


def upsert_economic_events(events: List[Dict]) -> int:
    """
    Insert or update economic events in Supabase.
    Returns count of NEW events inserted.
    """
    if not events:
        return 0

    new_count = 0
    
    try:
        for event in events:
            try:
                # Check if event already exists
                existing = sb.table('Economic_calander').select('actual').eq(
                    'date', event['date']
                ).eq(
                    'time', event['time']
                ).eq(
                    'country', event['country']
                ).eq(
                    'event', event['event']
                ).execute()

                if existing.data:
                    # Update only if actual value changed
                    old_actual = existing.data[0].get('actual')
                    if old_actual != event['actual'] and event['actual']:
                        sb.table('Economic_calander').update({
                            'actual': event['actual'],
                            'forecast': event['forecast'],
                            'previous': event['previous'],
                            'consensus': event['consensus']
                        }).eq(
                            'date', event['date']
                        ).eq(
                            'time', event['time']
                        ).eq(
                            'country', event['country']
                        ).eq(
                            'event', event['event']
                        ).execute()
                        log.info(f"📊 Updated: {event['country']} {event['event']} = {event['actual']}")
                else:
                    # Insert new event
                    sb.table('Economic_calander').insert(event).execute()
                    new_count += 1

            except Exception as e:
                log.warning(f"⚠️ Error upserting event {event['event']}: {e}")
                continue

        if new_count > 0:
            log.info(f"✅ Inserted {new_count} new economic events")

    except Exception as e:
        log.error(f"❌ Error upserting events to Supabase: {e}")

    return new_count


async def economic_calendar_task():
    """
    Economic calendar with DOUBLE-SCRAPE pattern.
    Scrapes at :00, :15, :30, :45 AND again at :05, :20, :35, :50
    This ensures we catch delayed data releases.
    """
    log.info("📅 Economic calendar scraper started (double-scrape pattern)")
    log.info("   Schedule: :00/:05, :15/:20, :30/:35, :45/:50")
    
    # Scrape minutes: primary at :00/:15/:30/:45, follow-up at :05/:20/:35/:50
    scrape_minutes = [0, 5, 15, 20, 30, 35, 45, 50]

    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            current_minute = now.minute
            current_second = now.second
            
            # Check if we're AT a scrape minute (within first 30 seconds)
            if current_minute in scrape_minutes and current_second < 30:
                # Execute scrape now
                is_followup = current_minute in [5, 20, 35, 50]
                scrape_type = "follow-up" if is_followup else "primary"
                
                log.info(f"📅 Running {scrape_type} scrape at :{current_minute:02d}...")
                
                loop = asyncio.get_event_loop()
                events = await loop.run_in_executor(None, scrape_trading_economics)

                if events:
                    new_count = await loop.run_in_executor(None, upsert_economic_events, events)
                    log.info(f"📅 {scrape_type.capitalize()} scrape complete: {len(events)} events, {new_count} new")
                
                # Wait until next minute to avoid re-triggering
                await asyncio.sleep(60 - current_second + 5)
            else:
                # Find next scrape minute
                next_scrape_minute = None
                for sm in scrape_minutes:
                    if sm > current_minute:
                        next_scrape_minute = sm
                        break
                
                if next_scrape_minute is None:
                    # Next scrape is in the next hour at :00
                    next_scrape_minute = 0
                    wait_seconds = (60 - current_minute) * 60 - current_second
                else:
                    wait_seconds = (next_scrape_minute - current_minute) * 60 - current_second
                
                # Ensure positive wait time (minimum 1 second)
                wait_seconds = max(1, wait_seconds)
                
                log.info(f"📅 Next calendar scrape in {wait_seconds // 60}m {wait_seconds % 60}s (at :{next_scrape_minute:02d})")
                await asyncio.sleep(wait_seconds)

        except Exception as e:
            log.error(f"❌ Error in economic calendar task: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 3: FINANCIAL NEWS SCRAPER
# ============================================================================

# 50+ RSS Feeds for comprehensive coverage
RSS_FEEDS = [
    # BBC News
    {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC News (Main)"},
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC News (Business)"},
    {"url": "http://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC News (World)"},
    
    # CNBC
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC (Top News)"},
    {"url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "name": "CNBC (World)"},
    {"url": "https://www.cnbc.com/id/15837362/device/rss/rss.html", "name": "CNBC (US News)"},
    {"url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "name": "CNBC (Economy)"},
    {"url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "name": "CNBC (Finance)"},
    {"url": "https://www.cnbc.com/id/10001147/device/rss/rss.html", "name": "CNBC (Earnings)"},
    {"url": "https://www.cnbc.com/id/15839135/device/rss/rss.html", "name": "CNBC (Commodities)"},
    {"url": "https://www.cnbc.com/id/19836768/device/rss/rss.html", "name": "CNBC (Energy)"},
    
    # MarketWatch
    {"url": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines", "name": "MarketWatch (Real-time)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories", "name": "MarketWatch (Top Stories)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/marketpulse", "name": "MarketWatch (Market Pulse)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/StockstoWatch", "name": "MarketWatch (Stocks)"},
    
    # Crypto
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk"},
    {"url": "https://cointelegraph.com/rss", "name": "Cointelegraph"},
    {"url": "https://bitcoinmagazine.com/.rss/full/", "name": "Bitcoin Magazine"},
    {"url": "https://decrypt.co/feed", "name": "Decrypt"},
    {"url": "https://thedefiant.io/feed", "name": "The Defiant"},
    
    # The Guardian
    {"url": "https://www.theguardian.com/world/rss", "name": "The Guardian (World)"},
    {"url": "https://www.theguardian.com/uk/business/rss", "name": "The Guardian (Business)"},
    {"url": "https://www.theguardian.com/business/economics/rss", "name": "The Guardian (Economics)"},
    {"url": "https://www.theguardian.com/business/stock-markets/rss", "name": "The Guardian (Markets)"},
    
    # Al Jazeera
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera"},
    {"url": "https://www.aljazeera.com/economy/rss.xml", "name": "Al Jazeera (Economy)"},
    
    # NPR
    {"url": "https://feeds.npr.org/1001/rss.xml", "name": "NPR (News)"},
    {"url": "https://feeds.npr.org/1006/rss.xml", "name": "NPR (Business)"},
    {"url": "https://feeds.npr.org/1014/rss.xml", "name": "NPR (Politics)"},
    
    # Yahoo Finance
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance"},
    
    # Investing.com
    {"url": "https://www.investing.com/rss/news.rss", "name": "Investing.com (News)"},
    {"url": "https://www.investing.com/rss/news_25.rss", "name": "Investing.com (Economy)"},
    {"url": "https://www.investing.com/rss/news_14.rss", "name": "Investing.com (Forex)"},
    {"url": "https://www.investing.com/rss/news_285.rss", "name": "Investing.com (Commodities)"},
    {"url": "https://www.investing.com/rss/news_301.rss", "name": "Investing.com (Crypto)"},
    
    # Central Banks
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve"},
    {"url": "https://www.ecb.europa.eu/rss/press.html", "name": "European Central Bank"},
    {"url": "https://www.bankofengland.co.uk/rss/news", "name": "Bank of England"},
    
    # FXStreet
    {"url": "https://www.fxstreet.com/rss/news", "name": "FXStreet (News)"},
    {"url": "https://www.fxstreet.com/rss/analysis", "name": "FXStreet (Analysis)"},
    
    # DailyFX
    {"url": "https://www.dailyfx.com/feeds/all", "name": "DailyFX"},
    
    # Seeking Alpha
    {"url": "https://seekingalpha.com/market_currents.xml", "name": "Seeking Alpha (Market Currents)"},
    {"url": "https://seekingalpha.com/tag/macro-view.xml", "name": "Seeking Alpha (Macro)"},
    
    # Zero Hedge
    {"url": "https://feeds.feedburner.com/zerohedge/feed", "name": "Zero Hedge"},
    
    # Politico
    {"url": "https://www.politico.com/rss/economy.xml", "name": "Politico (Economy)"},
    {"url": "https://www.politico.com/rss/politicopicks.xml", "name": "Politico (Top)"},
    
    # The Economist
    {"url": "https://www.economist.com/finance-and-economics/rss.xml", "name": "The Economist (Finance)"},
    {"url": "https://www.economist.com/business/rss.xml", "name": "The Economist (Business)"},
    
    # Barrons
    {"url": "https://www.barrons.com/xml/rss/3_7031.xml", "name": "Barrons"},
    
    # Oil & Energy
    {"url": "https://oilprice.com/rss/main", "name": "OilPrice.com"},
    
    # Metals/Gold
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
    
    # Tech Giants
    "nvidia", "apple", "microsoft", "amazon", "google", "meta", "tesla", "ai", "artificial intelligence",
    "chip", "semiconductor", "data center",
]

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "cricket", "tennis", "basketball", "baseball", "nfl", "nba",
    "celebrity", "actor", "actress", "movie", "film", "tv show", "television", "entertainment",
    "recipe", "cooking", "fashion", "style", "beauty", "wedding", "divorce", "dating",
    "horoscope", "lottery", "game show", "reality tv", "red carpet",
]


def get_news_scan_interval() -> tuple:
    """Smart scheduling based on market hours."""
    now = datetime.now(timezone.utc)
    day_of_week = now.weekday()
    hour_utc = now.hour
    
    # Weekend
    if day_of_week >= 5:
        return 7200, "weekend"  # 2 hours
    
    # US Market Hours or Asian Session
    if (14 <= hour_utc < 21) or (23 <= hour_utc or hour_utc < 8):
        return 600, "active"  # 10 minutes
    
    # European Session
    if 8 <= hour_utc < 17:
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
            if similarity >= 0.70:
                is_duplicate = True
                break

        if not is_duplicate:
            unique_articles.append(article)
            seen_topics.append(article['title'])

    removed_similar = len(articles_with_images) - len(unique_articles)
    log.info(f"   Removed {removed_similar} similar articles")

    # Category limits
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


def classify_article_with_deepseek(article: dict) -> Optional[dict]:
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
                await asyncio.sleep(0.3)

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


# ============================================================================
#                    TASK 4: GAP FILL SERVICE
# ============================================================================

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
        
    except Exception:
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
            return []
        
        candles = []
        for v in data["values"]:
            candles.append({
                "timestamp": v["datetime"],
                "open": float(v["open"]),
                "high": float(v["high"]),
                "low": float(v["low"]),
                "close": float(v["close"]),
                "volume": float(v.get("volume", 0)) if v.get("volume") else None
            })
        
        return candles
        
    except Exception as e:
        log.warning(f"⚠️ REST API error for {symbol}: {e}")
        return []


def insert_candles(symbol: str, candles: List[Dict]) -> int:
    """Insert candles into symbol-specific table."""
    if not candles:
        return 0
    
    table_name = symbol_to_table(symbol)
    
    try:
        # Prepare candles with volume
        prepared_candles = []
        for c in candles:
            prepared_candles.append({
                "timestamp": c["timestamp"],
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c.get("volume"),  # Include volume (nullable)
                "symbol": symbol
            })
        
        # Upsert to handle duplicates
        sb.table(table_name).upsert(prepared_candles, on_conflict="timestamp").execute()
        return len(prepared_candles)
        
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
    
    # Priority symbols for backfill
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


# ============================================================================
#                    TASK 5: G20 BOND YIELD SCRAPER
# ============================================================================

# G20 Bond URLs - 6M, 1Y, 2Y, 10Y maturities
G20_BONDS = {
    "US": {
        "name": "United States",
        "flag": "🇺🇸",
        "6m": "https://www.investing.com/rates-bonds/u.s.-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/u.s.-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/u.s.-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
    },
    "GB": {
        "name": "United Kingdom",
        "flag": "🇬🇧",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/uk-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/uk-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/uk-10-year-bond-yield"
    },
    "DE": {
        "name": "Germany",
        "flag": "🇩🇪",
        "6m": "https://www.investing.com/rates-bonds/germany-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/germany-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/germany-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/germany-10-year-bond-yield"
    },
    "JP": {
        "name": "Japan",
        "flag": "🇯🇵",
        "6m": "https://www.investing.com/rates-bonds/japan-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/japan-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/japan-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield"
    },
    "FR": {
        "name": "France",
        "flag": "🇫🇷",
        "6m": "https://www.investing.com/rates-bonds/france-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/france-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/france-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/france-10-year-bond-yield"
    },
    "IT": {
        "name": "Italy",
        "flag": "🇮🇹",
        "6m": "https://www.investing.com/rates-bonds/italy-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/italy-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/italy-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/italy-10-year-bond-yield"
    },
    "CA": {
        "name": "Canada",
        "flag": "🇨🇦",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/canada-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/canada-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/canada-10-year-bond-yield"
    },
    "AU": {
        "name": "Australia",
        "flag": "🇦🇺",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/australia-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/australia-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/australia-10-year-bond-yield"
    },
    "KR": {
        "name": "South Korea",
        "flag": "🇰🇷",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/south-korea-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/south-korea-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield"
    },
    "CN": {
        "name": "China",
        "flag": "🇨🇳",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/china-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/china-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/china-10-year-bond-yield"
    },
    "IN": {
        "name": "India",
        "flag": "🇮🇳",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/india-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/india-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/india-10-year-bond-yield"
    },
    "BR": {
        "name": "Brazil",
        "flag": "🇧🇷",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/brazil-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/brazil-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/brazil-10-year-bond-yield"
    },
    "MX": {
        "name": "Mexico",
        "flag": "🇲🇽",
        "6m": None,
        "1y": None,
        "2y": None,
        "10y": "https://www.investing.com/rates-bonds/mexico-10-year"
    },
    "RU": {
        "name": "Russia",
        "flag": "🇷🇺",
        "6m": None,
        "1y": "https://www.investing.com/rates-bonds/russia-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/russia-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/russia-10-year-bond-yield"
    },
    "ZA": {
        "name": "South Africa",
        "flag": "🇿🇦",
        "6m": None,
        "1y": None,
        "2y": None,
        "10y": "https://www.investing.com/rates-bonds/south-africa-10-year-bond-yield"
    },
    "TR": {
        "name": "Turkey",
        "flag": "🇹🇷",
        "6m": None,
        "1y": None,
        "2y": "https://www.investing.com/rates-bonds/turkey-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/turkey-10-year-bond-yield"
    },
    "ID": {
        "name": "Indonesia",
        "flag": "🇮🇩",
        "6m": None,
        "1y": None,
        "2y": None,
        "10y": "https://www.investing.com/rates-bonds/indonesia-10-year-bond-yield"
    }
}


def scrape_bond_yield(url: str, session: requests.Session, max_retries: int = 2) -> Optional[Dict]:
    """Scrape yield and change from Investing.com."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=20)
            response.encoding = 'utf-8'
            
            if response.status_code == 404:
                return None
            
            if response.status_code != 200:
                if attempt < max_retries:
                    time.sleep(1)
                    continue
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract yield value
            yield_elem = soup.find('div', {'data-test': 'instrument-price-last'})
            if not yield_elem:
                if attempt < max_retries:
                    time.sleep(1)
                    continue
                return None
            
            yield_text = yield_elem.get_text(strip=True)
            yield_text = re.sub(r'<!--.*?-->', '', yield_text)
            yield_value = float(yield_text.replace(',', ''))
            
            # Extract change value
            change_value = None
            change_elem = soup.find('div', {'data-test': 'instrument-price-change'})
            if change_elem:
                change_text = change_elem.get_text(strip=True)
                change_text = re.sub(r'<!--.*?-->', '', change_text)
                try:
                    change_value = float(change_text.replace(',', '').replace('+', ''))
                except:
                    pass
            
            return {
                'yield': yield_value,
                'change': change_value
            }
            
        except Exception as e:
            if attempt < max_retries:
                time.sleep(1)
                continue
            return None
    
    return None


def scrape_all_g20_yields() -> List[Dict]:
    """Scrape yields for all G20 countries."""
    results = []
    session = requests.Session()
    
    log.info("🌍 Starting G20 bond yield scrape...")
    
    for country_code, config in G20_BONDS.items():
        country_data = {
            'country_code': country_code,
            'country_name': config['name'],
            'flag': config['flag'],
            'yield_6m': None,
            'yield_1y': None,
            'yield_2y': None,
            'yield_10y': None,
            'change_6m': None,
            'change_1y': None,
            'change_2y': None,
            'change_10y': None,
            'spread_10y_2y': None,
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Scrape each maturity
        for maturity in ['6m', '1y', '2y', '10y']:
            url = config.get(maturity)
            
            if url is None:
                continue
            
            data = scrape_bond_yield(url, session)
            
            if data:
                country_data[f'yield_{maturity}'] = data['yield']
                country_data[f'change_{maturity}'] = data['change']
            
            time.sleep(1.5)  # Rate limiting
        
        # Calculate spread (10Y - 2Y)
        if country_data['yield_10y'] is not None and country_data['yield_2y'] is not None:
            country_data['spread_10y_2y'] = round(country_data['yield_10y'] - country_data['yield_2y'], 3)
        
        # Log result
        y10 = country_data['yield_10y']
        spread = country_data['spread_10y_2y']
        if y10:
            spread_str = f"spread {spread:+.2f}%" if spread else ""
            inverted = "🔴 INVERTED" if spread and spread < 0 else ""
            log.info(f"   {config['flag']} {country_code}: 10Y={y10:.2f}% {spread_str} {inverted}")
        
        results.append(country_data)
    
    return results


def store_bond_yields(yields: List[Dict]) -> int:
    """Store bond yields in Supabase."""
    if not yields:
        return 0
    
    try:
        # Filter to only countries with at least 10Y data
        valid_yields = [y for y in yields if y['yield_10y'] is not None]
        
        if valid_yields:
            sb.table('sovereign_yields').insert(valid_yields).execute()
            log.info(f"✅ Stored {len(valid_yields)} bond yield records")
            return len(valid_yields)
    
    except Exception as e:
        log.error(f"❌ Error storing bond yields: {e}")
    
    return 0


async def bond_yield_task():
    """Periodically scrape and store G20 bond yields."""
    log.info("📈 Bond yield scraper started")
    log.info(f"   Interval: {BOND_SCRAPE_INTERVAL // 60} minutes")
    log.info(f"   Countries: {len(G20_BONDS)}")
    log.info(f"   Maturities: 6M, 1Y, 2Y, 10Y")
    
    first_run = True
    
    while not tick_streamer._shutdown.is_set():
        try:
            # Run immediately on first iteration, then wait after each run
            if not first_run:
                log.info(f"😴 Next bond scrape in {BOND_SCRAPE_INTERVAL // 60} minutes...")
                await asyncio.sleep(BOND_SCRAPE_INTERVAL)
            
            first_run = False
            
            loop = asyncio.get_event_loop()
            yields = await loop.run_in_executor(None, scrape_all_g20_yields)
            
            if yields:
                # Stats
                has_10y = sum(1 for y in yields if y['yield_10y'] is not None)
                has_2y = sum(1 for y in yields if y['yield_2y'] is not None)
                inverted = sum(1 for y in yields if y['spread_10y_2y'] is not None and y['spread_10y_2y'] < 0)
                
                log.info(f"📊 Bond scrape complete: {has_10y}/17 10Y, {has_2y}/17 2Y, {inverted} inverted")
                
                await loop.run_in_executor(None, store_bond_yields, yields)
        
        except Exception as e:
            log.error(f"❌ Error in bond yield task: {e}")
            await asyncio.sleep(300)


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    """Run all tasks in parallel with graceful shutdown."""
    log.info("=" * 70)
    log.info("🚀 LONDON STRATEGIC EDGE - BULLETPROOF WORKER")
    log.info("=" * 70)
    log.info("   1️⃣  Tick streaming (TwelveData) - %d symbols", len(ALL_SYMBOLS))
    log.info("   2️⃣  Economic calendar (Trading Economics) - double-scrape")
    log.info("   3️⃣  Financial news (%d RSS feeds + DeepSeek)", len(RSS_FEEDS))
    log.info("   4️⃣  Gap detection & backfill (AUTO-HEALING)")
    log.info("   5️⃣  Bond yields (G20 sovereign bonds) - 30 min")
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
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
        asyncio.create_task(bond_yield_task(), name="bond_yields"),
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
