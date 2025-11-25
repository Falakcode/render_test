"""
=============================================================================
TOKEN CHARTED - BULLETPROOF PRODUCTION WORKER v2.0
=============================================================================
A mission-critical, self-healing data pipeline for real-time financial data.

Features:
- 🔄 Auto-reconnecting WebSocket with exponential backoff
- 🏥 HTTP health check endpoint (prevents Render sleep)
- 🔍 Automatic gap detection and backfill
- 📊 TwelveData REST API integration for missing candles
- 💾 Robust database operations with retry logic
- 📡 Real-time tick streaming for 16 trading pairs
- 📰 Financial news scraping with AI classification
- 📅 Economic calendar scraping
- 💓 Heartbeat monitoring and status reporting
- 🛡️ Graceful shutdown and error recovery

Author: Token Charted
Version: 2.0.0 (Bulletproof Edition)
=============================================================================
"""

import os
import json
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Set, Tuple
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from dataclasses import dataclass, field
from enum import Enum

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser
from anthropic import Anthropic

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Centralized configuration management."""
    
    # API Keys (from environment)
    TD_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    
    # Database
    TICK_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")
    
    # Health check server
    HEALTH_PORT = int(os.environ.get("PORT", 10000))
    
    # The 16 trading pairs
    SYMBOLS = [
        # Crypto (8)
        "BTC/USD", "ETH/USD", "XRP/USD", "XMR/USD", 
        "SOL/USD", "BNB/USD", "ADA/USD", "DOGE/USD",
        # Forex (7)
        "EUR/USD", "GBP/USD", "USD/CAD", "GBP/AUD", 
        "AUD/CAD", "EUR/GBP", "USD/JPY",
        # Commodities (1)
        "XAU/USD"
    ]
    SYMBOLS_STR = ",".join(SYMBOLS)
    
    # WebSocket
    WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"
    WS_PING_INTERVAL = 20
    WS_PING_TIMEOUT = 20
    WS_MAX_QUEUE = 1000
    
    # Reconnection
    RECONNECT_MIN_DELAY = 1
    RECONNECT_MAX_DELAY = 60
    RECONNECT_MULTIPLIER = 2
    
    # Batch settings
    BATCH_MAX_SIZE = 500
    BATCH_FLUSH_INTERVAL = 2  # seconds
    
    # Gap detection
    GAP_CHECK_INTERVAL = 300  # 5 minutes
    GAP_THRESHOLD_MINUTES = 2  # Consider gap if > 2 minutes missing
    MAX_BACKFILL_HOURS = 24  # Don't backfill more than 24 hours
    
    # TwelveData REST API
    TD_REST_BASE = "https://api.twelvedata.com"
    TD_RATE_LIMIT_DELAY = 1.2  # seconds between API calls
    
    # Task intervals
    ECON_SCRAPE_INTERVAL = 3600  # 1 hour
    HEARTBEAT_INTERVAL = 60  # 1 minute
    
    # News scheduling
    NEWS_ACTIVE_INTERVAL = 900    # 15 min during active hours
    NEWS_QUIET_INTERVAL = 1800    # 30 min during quiet hours
    NEWS_WEEKEND_INTERVAL = 14400 # 4 hours on weekends


# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

log = logging.getLogger("worker")
log_ws = logging.getLogger("websocket")
log_gap = logging.getLogger("gap-detector")
log_news = logging.getLogger("news")
log_econ = logging.getLogger("econ-calendar")
log_health = logging.getLogger("health")


# =============================================================================
# CONNECTION STATE TRACKING
# =============================================================================

class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class WorkerStatus:
    """Global worker status for health monitoring."""
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    websocket_state: ConnectionState = ConnectionState.DISCONNECTED
    last_tick_received: Optional[datetime] = None
    ticks_received_total: int = 0
    ticks_inserted_total: int = 0
    reconnection_count: int = 0
    last_gap_check: Optional[datetime] = None
    gaps_detected: int = 0
    gaps_filled: int = 0
    last_news_scan: Optional[datetime] = None
    last_econ_scrape: Optional[datetime] = None
    errors_total: int = 0
    
    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at.isoformat(),
            "uptime_seconds": (datetime.now(timezone.utc) - self.started_at).total_seconds(),
            "websocket_state": self.websocket_state.value,
            "last_tick_received": self.last_tick_received.isoformat() if self.last_tick_received else None,
            "ticks_received_total": self.ticks_received_total,
            "ticks_inserted_total": self.ticks_inserted_total,
            "reconnection_count": self.reconnection_count,
            "last_gap_check": self.last_gap_check.isoformat() if self.last_gap_check else None,
            "gaps_detected": self.gaps_detected,
            "gaps_filled": self.gaps_filled,
            "last_news_scan": self.last_news_scan.isoformat() if self.last_news_scan else None,
            "last_econ_scrape": self.last_econ_scrape.isoformat() if self.last_econ_scrape else None,
            "errors_total": self.errors_total,
            "symbols_count": len(Config.SYMBOLS),
            "health": "healthy" if self._is_healthy() else "degraded"
        }
    
    def _is_healthy(self) -> bool:
        """Check if worker is healthy."""
        if self.websocket_state != ConnectionState.CONNECTED:
            return False
        if self.last_tick_received:
            age = (datetime.now(timezone.utc) - self.last_tick_received).total_seconds()
            if age > 60:  # No ticks for 1 minute
                return False
        return True


# Global instances
status = WorkerStatus()
_stop_event = asyncio.Event()
_batch: List[dict] = []
_batch_lock = asyncio.Lock()


# =============================================================================
# DATABASE CLIENT
# =============================================================================

def get_supabase_client():
    """Create Supabase client with validation."""
    if not Config.SUPABASE_URL or not Config.SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

sb = get_supabase_client()


# =============================================================================
# HEALTH CHECK HTTP SERVER
# =============================================================================

class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health checks."""
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == "/health" or self.path == "/":
            self._send_health()
        elif self.path == "/status":
            self._send_status()
        elif self.path == "/ready":
            self._send_ready()
        else:
            self.send_error(404)
    
    def _send_health(self):
        """Simple health check endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        response = {
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "websocket": status.websocket_state.value
        }
        self.wfile.write(json.dumps(response).encode())
    
    def _send_status(self):
        """Detailed status endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(status.to_dict(), indent=2).encode())
    
    def _send_ready(self):
        """Kubernetes-style readiness check."""
        if status.websocket_state == ConnectionState.CONNECTED:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ready")
        else:
            self.send_response(503)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"not ready")


def run_health_server():
    """Run the health check HTTP server in a separate thread."""
    server = HTTPServer(("0.0.0.0", Config.HEALTH_PORT), HealthHandler)
    log_health.info(f"🏥 Health server started on port {Config.HEALTH_PORT}")
    server.serve_forever()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def to_float(x) -> Optional[float]:
    """Safely convert to float."""
    try:
        return float(x) if x is not None else None
    except (ValueError, TypeError):
        return None


def to_timestamp(v) -> datetime:
    """Convert various timestamp formats to UTC datetime."""
    if v is None:
        return datetime.now(timezone.utc)
    
    # Try epoch (seconds or milliseconds)
    try:
        t = float(v)
        if t > 1e12:  # Milliseconds
            t /= 1000.0
        return datetime.fromtimestamp(t, tz=timezone.utc)
    except (ValueError, TypeError):
        pass
    
    # Try ISO format
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        pass
    
    return datetime.now(timezone.utc)


def symbol_to_table(symbol: str) -> str:
    """Convert symbol to candle table name. E.g., 'BTC/USD' -> 'candles_btc_usd'"""
    return f"candles_{symbol.lower().replace('/', '_')}"


def get_market_session() -> Tuple[int, str]:
    """Get current market session and appropriate scan interval."""
    now = datetime.utcnow()
    day_of_week = now.weekday()
    hour_utc = now.hour
    
    # Weekend
    if day_of_week >= 5:
        return Config.NEWS_WEEKEND_INTERVAL, "weekend"
    
    # Active hours (8 AM - 8 PM UTC, or late night 0-1 AM)
    if (8 <= hour_utc < 20) or (0 <= hour_utc < 1):
        return Config.NEWS_ACTIVE_INTERVAL, "active"
    
    # Quiet hours
    return Config.NEWS_QUIET_INTERVAL, "quiet"


# =============================================================================
# TICK STREAMING
# =============================================================================

async def flush_batch():
    """Flush tick batch to database with retry logic."""
    global _batch
    
    async with _batch_lock:
        if not _batch:
            return
        payload = _batch.copy()
        _batch = []
    
    # Retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            sb.table(Config.TICK_TABLE).insert(payload).execute()
            status.ticks_inserted_total += len(payload)
            log.info(f"✅ Inserted {len(payload)} ticks")
            return
        except Exception as e:
            status.errors_total += 1
            if attempt < max_retries - 1:
                log.warning(f"⚠️ Insert failed (attempt {attempt + 1}): {e}")
                await asyncio.sleep(1 * (attempt + 1))
            else:
                log.error(f"❌ Insert failed after {max_retries} attempts: {e}")
                # Re-queue failed batch
                async with _batch_lock:
                    _batch = payload + _batch


async def periodic_flush():
    """Periodically flush tick batch."""
    while not _stop_event.is_set():
        try:
            await asyncio.wait_for(
                _stop_event.wait(), 
                timeout=Config.BATCH_FLUSH_INTERVAL
            )
        except asyncio.TimeoutError:
            await flush_batch()


async def handle_tick(msg: dict):
    """Process incoming tick message."""
    if msg.get("event") != "price":
        return
    
    symbol = msg.get("symbol")
    price = to_float(msg.get("price"))
    
    if not symbol or price is None:
        return
    
    row = {
        "symbol": symbol,
        "ts": to_timestamp(msg.get("timestamp")).isoformat(),
        "price": price,
        "bid": to_float(msg.get("bid")),
        "ask": to_float(msg.get("ask")),
        "day_volume": (
            to_float(msg.get("day_volume")) or
            to_float(msg.get("dayVolume")) or
            to_float(msg.get("volume"))
        ),
    }
    
    status.ticks_received_total += 1
    status.last_tick_received = datetime.now(timezone.utc)
    
    async with _batch_lock:
        _batch.append(row)
        if len(_batch) >= Config.BATCH_MAX_SIZE:
            await flush_batch()


async def websocket_connection():
    """Manage a single WebSocket connection."""
    status.websocket_state = ConnectionState.CONNECTING
    
    async with websockets.connect(
        Config.WS_URL,
        ping_interval=Config.WS_PING_INTERVAL,
        ping_timeout=Config.WS_PING_TIMEOUT,
        max_queue=Config.WS_MAX_QUEUE,
    ) as ws:
        # Subscribe to symbols
        subscribe_msg = {
            "action": "subscribe",
            "params": {"symbols": Config.SYMBOLS_STR}
        }
        await ws.send(json.dumps(subscribe_msg))
        
        status.websocket_state = ConnectionState.CONNECTED
        log_ws.info(f"🚀 Connected! Subscribed to {len(Config.SYMBOLS)} symbols")
        
        # Start periodic flusher
        flusher = asyncio.create_task(periodic_flush())
        
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw)
                    await handle_tick(data)
                except json.JSONDecodeError:
                    continue
        finally:
            status.websocket_state = ConnectionState.DISCONNECTED
            await flush_batch()
            flusher.cancel()
            try:
                await flusher
            except asyncio.CancelledError:
                pass


async def tick_streaming_task():
    """Main tick streaming task with robust reconnection."""
    backoff = Config.RECONNECT_MIN_DELAY
    
    while not _stop_event.is_set():
        try:
            await websocket_connection()
            backoff = Config.RECONNECT_MIN_DELAY  # Reset on clean exit
            
        except websockets.exceptions.ConnectionClosedError as e:
            status.reconnection_count += 1
            status.websocket_state = ConnectionState.RECONNECTING
            log_ws.warning(f"⚠️ Connection closed: {e}; reconnecting in {backoff}s")
            
        except websockets.exceptions.ConnectionClosedOK:
            status.reconnection_count += 1
            status.websocket_state = ConnectionState.RECONNECTING
            log_ws.info(f"🔄 Connection closed normally; reconnecting in {backoff}s")
            
        except Exception as e:
            status.reconnection_count += 1
            status.errors_total += 1
            status.websocket_state = ConnectionState.ERROR
            log_ws.error(f"❌ WebSocket error: {e}; reconnecting in {backoff}s")
        
        if not _stop_event.is_set():
            await asyncio.sleep(backoff)
            backoff = min(Config.RECONNECT_MAX_DELAY, backoff * Config.RECONNECT_MULTIPLIER)


# =============================================================================
# GAP DETECTION AND BACKFILL
# =============================================================================

def detect_gaps(symbol: str, hours_back: int = 1) -> List[Tuple[datetime, datetime]]:
    """
    Detect gaps in candle data for a symbol.
    Returns list of (gap_start, gap_end) tuples.
    """
    table_name = symbol_to_table(symbol)
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=hours_back)
    
    try:
        result = sb.table(table_name).select("timestamp").gte(
            "timestamp", start_time.isoformat()
        ).order("timestamp", desc=False).execute()
        
        if not result.data:
            # No data at all - entire period is a gap
            return [(start_time, now)]
        
        gaps = []
        timestamps = [datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) 
                     for r in result.data]
        
        # Check for initial gap
        if timestamps and (timestamps[0] - start_time).total_seconds() > Config.GAP_THRESHOLD_MINUTES * 60:
            gaps.append((start_time, timestamps[0]))
        
        # Check between candles
        for i in range(1, len(timestamps)):
            gap_seconds = (timestamps[i] - timestamps[i-1]).total_seconds()
            if gap_seconds > Config.GAP_THRESHOLD_MINUTES * 60:
                gaps.append((timestamps[i-1], timestamps[i]))
        
        # Check for trailing gap (only if significant)
        if timestamps:
            trailing_gap = (now - timestamps[-1]).total_seconds()
            if trailing_gap > Config.GAP_THRESHOLD_MINUTES * 60:
                gaps.append((timestamps[-1], now))
        
        return gaps
        
    except Exception as e:
        log_gap.error(f"Error detecting gaps for {symbol}: {e}")
        return []


def fetch_twelvedata_candles(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: str = "1min"
) -> List[dict]:
    """
    Fetch historical candles from TwelveData REST API.
    """
    # Calculate output size (number of candles)
    minutes_diff = int((end_time - start_time).total_seconds() / 60)
    output_size = min(max(minutes_diff + 5, 10), 5000)  # TwelveData max is 5000
    
    url = f"{Config.TD_REST_BASE}/time_series"
    params = {
        "apikey": Config.TD_API_KEY,
        "symbol": symbol,
        "interval": interval,
        "outputsize": output_size,
        "start_date": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "UTC"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "values" not in data:
            if "message" in data:
                log_gap.warning(f"TwelveData API message for {symbol}: {data['message']}")
            return []
        
        candles = []
        for v in data["values"]:
            candles.append({
                "timestamp": v["datetime"],
                "open": float(v["open"]),
                "high": float(v["high"]),
                "low": float(v["low"]),
                "close": float(v["close"]),
                "symbol": symbol
            })
        
        return candles
        
    except requests.exceptions.RequestException as e:
        log_gap.error(f"Error fetching candles for {symbol}: {e}")
        return []


def insert_candles(symbol: str, candles: List[dict]) -> int:
    """
    Insert candles into the appropriate table with upsert logic.
    Returns number of candles inserted/updated.
    """
    if not candles:
        return 0
    
    table_name = symbol_to_table(symbol)
    
    # Prepare rows for insertion
    rows = []
    for c in candles:
        # Parse timestamp
        ts = c["timestamp"]
        if isinstance(ts, str):
            # TwelveData returns "YYYY-MM-DD HH:MM:SS" format
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                ts = dt.isoformat()
            except ValueError:
                ts = to_timestamp(ts).isoformat()
        
        rows.append({
            "timestamp": ts,
            "symbol": symbol,
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    
    try:
        # Use upsert to handle duplicates
        sb.table(table_name).upsert(
            rows,
            on_conflict="timestamp"
        ).execute()
        
        return len(rows)
        
    except Exception as e:
        log_gap.error(f"Error inserting candles into {table_name}: {e}")
        return 0


async def backfill_symbol_gaps(symbol: str, gaps: List[Tuple[datetime, datetime]]) -> int:
    """
    Backfill gaps for a single symbol using TwelveData REST API.
    Returns total candles filled.
    """
    total_filled = 0
    
    for gap_start, gap_end in gaps:
        # Skip very recent gaps (< 2 minutes) - data might just be delayed
        if (datetime.now(timezone.utc) - gap_end).total_seconds() < 120:
            continue
        
        # Skip very old gaps (> 24 hours)
        if (datetime.now(timezone.utc) - gap_start).total_seconds() > Config.MAX_BACKFILL_HOURS * 3600:
            log_gap.info(f"⏭️ Skipping old gap for {symbol}: {gap_start} to {gap_end}")
            continue
        
        gap_minutes = (gap_end - gap_start).total_seconds() / 60
        log_gap.info(f"🔄 Backfilling {symbol}: {gap_start.isoformat()} to {gap_end.isoformat()} ({gap_minutes:.0f} min)")
        
        # Fetch candles from TwelveData
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None,
            fetch_twelvedata_candles,
            symbol,
            gap_start,
            gap_end
        )
        
        if candles:
            inserted = await loop.run_in_executor(
                None,
                insert_candles,
                symbol,
                candles
            )
            total_filled += inserted
            log_gap.info(f"✅ Filled {inserted} candles for {symbol}")
        else:
            log_gap.warning(f"⚠️ No candles returned for {symbol} gap")
        
        # Rate limiting
        await asyncio.sleep(Config.TD_RATE_LIMIT_DELAY)
    
    return total_filled


async def gap_detection_task():
    """
    Periodic task to detect and fill gaps in candle data.
    Runs every 5 minutes.
    """
    log_gap.info("🔍 Gap detector started")
    
    # Initial delay to let streaming establish
    await asyncio.sleep(60)
    
    while not _stop_event.is_set():
        try:
            status.last_gap_check = datetime.now(timezone.utc)
            log_gap.info("🔍 Starting gap detection scan...")
            
            total_gaps = 0
            total_filled = 0
            
            for symbol in Config.SYMBOLS:
                # Detect gaps in last hour
                gaps = detect_gaps(symbol, hours_back=1)
                
                if gaps:
                    total_gaps += len(gaps)
                    status.gaps_detected += len(gaps)
                    
                    # Log significant gaps
                    for gap_start, gap_end in gaps:
                        gap_minutes = (gap_end - gap_start).total_seconds() / 60
                        if gap_minutes > 5:  # Only log gaps > 5 minutes
                            log_gap.warning(f"🕳️ Gap detected: {symbol} from {gap_start} to {gap_end} ({gap_minutes:.0f} min)")
                    
                    # Backfill
                    filled = await backfill_symbol_gaps(symbol, gaps)
                    total_filled += filled
                    status.gaps_filled += filled
                
                # Small delay between symbols
                await asyncio.sleep(0.1)
            
            if total_gaps > 0:
                log_gap.info(f"📊 Gap scan complete: {total_gaps} gaps detected, {total_filled} candles filled")
            else:
                log_gap.info("✅ Gap scan complete: no gaps detected")
            
            # Wait for next check
            await asyncio.sleep(Config.GAP_CHECK_INTERVAL)
            
        except Exception as e:
            status.errors_total += 1
            log_gap.error(f"❌ Error in gap detection: {e}")
            await asyncio.sleep(60)


# =============================================================================
# ECONOMIC CALENDAR SCRAPER
# =============================================================================

def scrape_trading_economics() -> List[dict]:
    """Scrape economic calendar from Trading Economics."""
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        log_econ.info("🌐 Fetching Trading Economics calendar...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': 'calendar'})
        
        if not table:
            log_econ.error("❌ Could not find calendar table")
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
                    except ValueError:
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
                
                events.append({
                    'date': current_date,
                    'day': current_day,
                    'time': time_str,
                    'country': country.upper(),
                    'event': event,
                    'actual': actual if actual else None,
                    'forecast': consensus if consensus else None,
                    'previous': previous if previous else None,
                    'consensus': consensus if consensus else None,
                    'importance': importance
                })
                
            except Exception as e:
                continue
        
        log_econ.info(f"📊 Scraped {len(events)} events")
        return events
        
    except Exception as e:
        log_econ.error(f"❌ Error scraping: {e}")
        return []


def upsert_economic_events(events: List[dict]):
    """Insert or update economic events."""
    if not events:
        return
    
    inserted = 0
    updated = 0
    
    for event in events:
        try:
            # Check if exists
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
                updated += 1
            else:
                sb.table('Economic_calander').insert(event).execute()
                inserted += 1
                
        except Exception as e:
            continue
    
    log_econ.info(f"✅ Events: {inserted} inserted, {updated} updated")


async def economic_calendar_task():
    """Periodic economic calendar scraper."""
    log_econ.info("📅 Economic calendar scraper started")
    
    while not _stop_event.is_set():
        try:
            status.last_econ_scrape = datetime.now(timezone.utc)
            
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, scrape_trading_economics)
            
            if events:
                await loop.run_in_executor(None, upsert_economic_events, events)
            
            await asyncio.sleep(Config.ECON_SCRAPE_INTERVAL)
            
        except Exception as e:
            status.errors_total += 1
            log_econ.error(f"❌ Error: {e}")
            await asyncio.sleep(60)


# =============================================================================
# FINANCIAL NEWS SCRAPER
# =============================================================================

RSS_FEEDS = [
    {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC News (Main)"},
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC News (Business)"},
    {"url": "http://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC News (World)"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC (Top News)"},
    {"url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "name": "CNBC (World)"},
    {"url": "https://www.cnbc.com/id/15837362/device/rss/rss.html", "name": "CNBC (US News)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines", "name": "MarketWatch"},
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories", "name": "MarketWatch (Top)"},
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk"},
    {"url": "https://cointelegraph.com/rss", "name": "Cointelegraph"},
    {"url": "https://www.theguardian.com/world/rss", "name": "The Guardian (World)"},
    {"url": "https://www.theguardian.com/uk/business/rss", "name": "The Guardian (Business)"},
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera"},
]

INCLUDE_KEYWORDS = [
    "war", "attack", "invasion", "military", "strike", "missile", "conflict",
    "trump", "biden", "election", "president", "prime minister",
    "fed", "federal reserve", "powell", "ecb", "lagarde", "bank of england",
    "rate", "interest", "inflation", "cpi", "gdp", "unemployment", "jobs",
    "tariff", "sanction", "trade war", "bitcoin", "btc", "ethereum", "eth",
    "crypto", "crash", "plunge", "surge", "rally", "collapse", "soar",
    "oil", "opec", "gold", "crude", "brent", "bank", "bailout", "crisis",
]

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "cricket", "tennis", "basketball",
    "celebrity", "actor", "actress", "movie", "film", "tv show",
    "recipe", "cooking", "fashion", "wedding", "divorce", "dating",
]


def fetch_rss_feed(feed_url: str, source_name: str) -> List[dict]:
    """Fetch articles from an RSS feed."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'}
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
            
            # Extract image URL
            image_url = None
            if hasattr(entry, 'media_content') and entry.media_content:
                image_url = entry.media_content[0].get('url')
            if not image_url and hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
                image_url = entry.media_thumbnail[0].get('url')
            if not image_url and hasattr(entry, 'enclosures') and entry.enclosures:
                for enc in entry.enclosures:
                    if enc.get('type', '').startswith('image/'):
                        image_url = enc.get('href') or enc.get('url')
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
        log_news.warning(f"⚠️ RSS error ({source_name}): {e}")
        return []


def should_prefilter_article(title: str, summary: str) -> bool:
    """Pre-filter articles by keywords."""
    text = f"{title} {summary}".lower()
    
    for exclude in EXCLUDE_KEYWORDS:
        if exclude in text:
            return False
    
    for include in INCLUDE_KEYWORDS:
        if include in text:
            return True
    
    return False


def classify_article_with_claude(article: dict, anthropic_client) -> Optional[dict]:
    """Classify article importance using Claude."""
    try:
        prompt = f"""You are a financial news analyst. Analyze this article and determine market impact.

TITLE: {article['title']}
SUMMARY: {article['summary']}
SOURCE: {article['source']}

Respond with ONLY a valid JSON object:
{{
  "is_important": true/false,
  "event_type": "war/political_shock/economic_data/central_bank/trade_war/commodity_shock/market_crash/crypto_crash/natural_disaster/bank_crisis",
  "impact_level": "critical/high/medium/low",
  "affected_symbols": ["BTC/USD", "XAU/USD", etc],
  "sentiment": "bullish/bearish/neutral",
  "summary": "2-3 sentence market impact summary",
  "keywords": ["keyword1", "keyword2"]
}}"""

        response = anthropic_client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text.strip()
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()
        
        return json.loads(response_text)
        
    except Exception as e:
        log_news.warning(f"⚠️ Claude error: {e}")
        return None


def store_news_article(article: dict, classification: dict) -> bool:
    """Store classified news article."""
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
        if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
            return False
        log_news.warning(f"⚠️ DB error: {e}")
        return False


async def financial_news_task():
    """Periodic financial news scraper with smart scheduling."""
    log_news.info("📰 Financial news scraper started")
    
    # Initialize Anthropic client
    try:
        anthropic_client = Anthropic(api_key=Config.ANTHROPIC_API_KEY)
    except Exception as e:
        log_news.error(f"❌ Failed to initialize Anthropic client: {e}")
        return
    
    while not _stop_event.is_set():
        try:
            interval, session = get_market_session()
            session_emoji = "🔥" if session == "active" else "😴" if session == "quiet" else "🌙"
            
            status.last_news_scan = datetime.now(timezone.utc)
            log_news.info(f"🔍 Starting news scan ({session_emoji} {session} session)")
            
            # Fetch from all RSS feeds
            all_articles = []
            loop = asyncio.get_event_loop()
            
            for feed in RSS_FEEDS:
                articles = await loop.run_in_executor(
                    None, fetch_rss_feed, feed['url'], feed['name']
                )
                all_articles.extend(articles)
                await asyncio.sleep(0.3)
            
            log_news.info(f"📊 Fetched {len(all_articles)} articles")
            
            # Deduplicate by URL
            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article['url'] not in seen_urls:
                    seen_urls.add(article['url'])
                    unique_articles.append(article)
            
            # Pre-filter by keywords
            filtered_articles = [
                a for a in unique_articles
                if should_prefilter_article(a['title'], a['summary'])
            ]
            
            # Further filter: require image
            filtered_articles = [a for a in filtered_articles if a.get('image_url')]
            
            log_news.info(f"🎯 {len(filtered_articles)} articles to classify")
            
            # Classify and store
            stored = 0
            for article in filtered_articles[:10]:  # Limit to top 10 per scan
                classification = await loop.run_in_executor(
                    None, classify_article_with_claude, article, anthropic_client
                )
                
                if not classification:
                    continue
                
                if not classification['is_important']:
                    continue
                
                if classification['impact_level'] not in ['critical', 'high']:
                    continue
                
                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored += 1
                    impact_emoji = "🚨" if classification['impact_level'] == 'critical' else "⚡"
                    log_news.info(f"{impact_emoji} {article['title'][:60]}...")
                
                await asyncio.sleep(0.5)
            
            log_news.info(f"✅ News scan complete: {stored} articles stored")
            log_news.info(f"😴 Next scan in {interval // 60} minutes")
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            status.errors_total += 1
            log_news.error(f"❌ Error: {e}")
            await asyncio.sleep(60)


# =============================================================================
# HEARTBEAT MONITOR
# =============================================================================

async def heartbeat_task():
    """Periodic heartbeat logging for monitoring."""
    while not _stop_event.is_set():
        try:
            await asyncio.sleep(Config.HEARTBEAT_INTERVAL)
            
            # Log heartbeat
            uptime = (datetime.now(timezone.utc) - status.started_at).total_seconds()
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            
            log.info(
                f"💓 Heartbeat | Uptime: {hours}h {minutes}m | "
                f"WS: {status.websocket_state.value} | "
                f"Ticks: {status.ticks_received_total} | "
                f"Reconnects: {status.reconnection_count} | "
                f"Gaps filled: {status.gaps_filled} | "
                f"Errors: {status.errors_total}"
            )
            
        except Exception as e:
            log.error(f"Heartbeat error: {e}")


# =============================================================================
# GRACEFUL SHUTDOWN
# =============================================================================

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    log.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    _stop_event.set()


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Main entry point."""
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Log startup
    log.info("=" * 70)
    log.info("🚀 TOKEN CHARTED WORKER v2.0 (Bulletproof Edition)")
    log.info("=" * 70)
    log.info(f"   📊 Symbols: {len(Config.SYMBOLS)} trading pairs")
    log.info(f"   🏥 Health check: http://0.0.0.0:{Config.HEALTH_PORT}/health")
    log.info(f"   🔄 Gap detection: every {Config.GAP_CHECK_INTERVAL // 60} minutes")
    log.info(f"   📰 News scanning: smart schedule (15/30/240 min)")
    log.info(f"   📅 Economic calendar: hourly")
    log.info("=" * 70)
    
    # Create tasks
    tasks = [
        asyncio.create_task(tick_streaming_task(), name="tick_streaming"),
        asyncio.create_task(gap_detection_task(), name="gap_detection"),
        asyncio.create_task(economic_calendar_task(), name="econ_calendar"),
        asyncio.create_task(financial_news_task(), name="financial_news"),
        asyncio.create_task(heartbeat_task(), name="heartbeat"),
    ]
    
    try:
        # Wait for all tasks
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        log.error(f"❌ Main loop error: {e}")
        
    finally:
        # Shutdown
        log.info("🛑 Shutting down...")
        _stop_event.set()
        
        # Cancel all tasks
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Final flush
        await flush_batch()
        
        log.info("👋 Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
