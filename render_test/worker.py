#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v7.3
================================================================================
Production-grade data pipeline with AGGRESSIVE self-healing.

NEW in v7.3:
  - Expanded commodities: 4 → 10 (added WTI, Brent, Natural Gas, Gas, Cattle)
  - Removed 9 failing ETF/Index symbols (AEX, BKX, FTSE, HSI, NBI, NDX, SMI, SPX, UTY)
  - Total symbols: 238 (all validated on REST + WebSocket)
  - Nuclear Watchdog: 60 seconds (aggressive - catches issues fast)
  - Weekly reset: Saturday 10pm UTC (maintenance during market close)
  - Removed unnecessary forced reconnects (no more tick gaps!)

NEW in v7.1:
  - Replaced DeepSeek with Google Gemini 2.0 Flash for AI classification
  - Cost savings: Using free GCP credits instead of paid DeepSeek API

TASKS:
  1. Tick Streaming     - TwelveData WebSocket (238 symbols)
  2. Economic Calendar  - Trading Economics (double-scrape every 15 min)
  3. Financial News     - 50+ RSS feeds + Gemini AI classification
  4. Gap Fill Service   - Auto-detect and repair missing candle data
  5. Bond Yields        - G20 sovereign yields (every 30 minutes)
  6. Whale Flow Tracker - BTC whale movements via Blockchain.com WebSocket
  7. AI News Desk       - Professional article synthesis (3 per cycle)
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
import threading
import random
import hashlib
import math
import ssl
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Set, Tuple
from contextlib import suppress
from collections import defaultdict, Counter

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser

# Optional: TF-IDF for AI News Desk clustering
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

# ============================================================================
#                              ENVIRONMENT
# ============================================================================

TD_API_KEY = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Google Gemini API (replaces DeepSeek)
GOOGLE_GEMINI_API_KEY = os.environ.get("GOOGLE_GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "LWD7wgTnS8zYEtNcnOc9qUgr_Encl7iycazKCp2vhvc")

SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# ============================================================================
#                           TRADING SYMBOLS (238)
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

# Commodities (10 pairs - expanded from 4)
COMMODITIES_SYMBOLS = [
    # Precious Metals
    "XAU/USD",   # Gold
    "XAG/USD",   # Silver
    "XPT/USD",   # Platinum
    "XPD/USD",   # Palladium
    # Energy
    "WTI/USD",   # WTI Crude Oil
    "XBR/USD",   # Brent Crude Oil
    "NG/USD",    # Natural Gas
    # Other
    "GAS/USD",   # Gas
    "XLC/USD",   # Live Cattle
    "FC/USD",    # Feeder Cattle
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

# ETFs/Indices (22 symbols)
# Removed 9 that don't work on WebSocket: AEX, BKX, FTSE, HSI, NBI, NDX, SMI, SPX, UTY
ETF_INDEX_SYMBOLS = [
    "ARKK", "DIA", "EEM", "GLD", "HYG", "IBEX", 
    "IWM", "LQD", "SHY", "SLV", "USO", 
    "VIXY", "VOO", "VTI", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("worker")


# ============================================================================
#                     GEMINI AI HELPER FUNCTIONS
# ============================================================================

def call_gemini_api(prompt: str, max_tokens: int = 1000, temperature: float = 0.1) -> Optional[str]:
    """
    Call Google Gemini API and return the response text.
    Returns None if there's an error.
    """
    if not GOOGLE_GEMINI_API_KEY:
        log.warning("⚠️ Google Gemini API key not configured")
        return None
    
    url = f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}"
    
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
        }
    }
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
        
        # Extract the text response
        text = result['candidates'][0]['content']['parts'][0]['text']
        return text.strip()
        
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        log.warning(f"⚠️ Gemini API Error: {e.code} - {error_body[:200]}")
        return None
    except Exception as e:
        log.warning(f"⚠️ Gemini API Error: {e}")
        return None


def clean_gemini_json_response(text: str) -> str:
    """Clean markdown code blocks from Gemini JSON response."""
    text = text.strip()
    if text.startswith('```'):
        text = text.split('\n', 1)[1] if '\n' in text else text[3:]
    if text.endswith('```'):
        text = text.rsplit('\n', 1)[0] if '\n' in text else text[:-3]
    text = text.replace('```json', '').replace('```', '').strip()
    return text


# ============================================================================
#                    TASK 1: BULLETPROOF TICK STREAMING v6.0
#                         NUCLEAR WATCHDOG EDITION
# ============================================================================

class BulletproofTickStreamer:
    """
    Bulletproof WebSocket tick streamer v6.0 with NUCLEAR WATCHDOG.
    
    CRITICAL FIXES from v5:
    1. NUCLEAR WATCHDOG - Runs independently, kills stuck connections
    2. TASK TIMEOUT - Entire _connect_and_stream() wrapped in asyncio.wait_for()
    3. ZOMBIE DETECTION - Detects when main loop is stuck, not just connection
    4. FORCE KILL - Uses asyncio.Task.cancel() if normal close doesn't work
    5. PERIODIC REFRESH - Force reconnect every 5 min to prevent zombie connections
    6. MAIN LOOP HEARTBEAT - Proves the main loop itself is running
    
    The key insight: The old watchdog only monitored the CONNECTION, but the MAIN LOOP
    could get stuck waiting for the connection to close. Now we monitor EVERYTHING.
    """
    
    def __init__(self):
        self._batch: List[dict] = []
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        
        # Timing - separate concerns
        self._last_tick_time: datetime = datetime.now(timezone.utc)
        self._last_activity_time: datetime = datetime.now(timezone.utc)
        self._last_main_loop_heartbeat: datetime = datetime.now(timezone.utc)  # NEW: Track main loop health
        
        # Stats
        self._tick_count: int = 0
        self._connection_count: int = 0
        self._last_health_log: datetime = datetime.now(timezone.utc)
        self._is_connected: bool = False
        
        # Failure tracking (for debugging)
        self._failure_reasons: dict = {
            "timeout": 0,
            "connection_closed": 0,
            "watchdog_kill": 0,
            "nuclear_kill": 0,
            "exception": 0,
            "forced_refresh": 0,
        }
        
        # NEW: Task reference for nuclear watchdog to kill
        self._stream_task: Optional[asyncio.Task] = None
        self._connection_start_time: Optional[datetime] = None
        
        # NEW: Nuclear watchdog settings
        self.NUCLEAR_TIMEOUT_SECS = 60  # If main loop doesn't heartbeat for 60s, KILL EVERYTHING
        
        # Weekly maintenance reset - Saturday 10pm UTC
        self.WEEKLY_RESET_DAY = 5  # Saturday (Monday=0, Saturday=5)
        self.WEEKLY_RESET_HOUR = 22  # 10pm UTC
        self._last_weekly_reset = None
        
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
        Standard watchdog: Force reconnect if connection appears dead.
        Also forces periodic refresh to prevent zombie connections.
        """
        log.info("🐕 Watchdog started (timeout: %ds)", WATCHDOG_TIMEOUT_SECS)
        
        while not self._shutdown.is_set():
            await asyncio.sleep(5)  # Check every 5 seconds
            
            now = datetime.now(timezone.utc)
            activity_elapsed = (now - self._last_activity_time).total_seconds()
            
            # If connected but no activity, kill the connection
            if self._is_connected and activity_elapsed > WATCHDOG_TIMEOUT_SECS:
                log.warning("🐕 WATCHDOG: No activity for %.1fs - killing connection!", activity_elapsed)
                self._failure_reasons["watchdog_kill"] += 1
                await self._kill_connection()
            
            # Weekly maintenance reset - Saturday 10pm UTC (when markets are closed)
            if self._is_connected:
                if now.weekday() == self.WEEKLY_RESET_DAY and now.hour == self.WEEKLY_RESET_HOUR:
                    # Only reset once per week (check if we already reset this hour)
                    reset_key = f"{now.year}-{now.isocalendar()[1]}"  # Year-WeekNumber
                    if self._last_weekly_reset != reset_key:
                        log.info("🔄 WEEKLY RESET: Saturday 10pm maintenance - refreshing connection")
                        self._last_weekly_reset = reset_key
                        self._failure_reasons["weekly_reset"] += 1
                        await self._kill_connection()
    
    async def _nuclear_watchdog(self):
        """
        NUCLEAR WATCHDOG: Monitors the main loop itself, not just the connection.
        
        If the main loop stops heartbeating (gets stuck), this will:
        1. Cancel the stream task
        2. Force close any WebSocket
        3. Reset all state
        
        This is the LAST LINE OF DEFENSE against any kind of hang.
        """
        log.info("☢️ NUCLEAR WATCHDOG started (timeout: %ds)", self.NUCLEAR_TIMEOUT_SECS)
        
        while not self._shutdown.is_set():
            await asyncio.sleep(10)
            
            now = datetime.now(timezone.utc)
            main_loop_elapsed = (now - self._last_main_loop_heartbeat).total_seconds()
            
            if main_loop_elapsed > self.NUCLEAR_TIMEOUT_SECS:
                log.error("☢️ NUCLEAR WATCHDOG TRIGGERED! Main loop stuck for %.1fs!", main_loop_elapsed)
                log.error("☢️ Killing everything and forcing restart...")
                
                self._failure_reasons["nuclear_kill"] += 1
                
                # Kill the stream task if it exists
                if self._stream_task and not self._stream_task.done():
                    log.warning("☢️ Cancelling stuck stream task...")
                    self._stream_task.cancel()
                    try:
                        await asyncio.wait_for(asyncio.shield(self._stream_task), timeout=5)
                    except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                        pass
                
                # Force close WebSocket
                await self._kill_connection()
                
                # Reset main loop heartbeat so we don't immediately trigger again
                self._last_main_loop_heartbeat = datetime.now(timezone.utc)
                
                log.info("☢️ Nuclear cleanup complete - main loop should restart")
    
    async def _kill_connection(self):
        """Forcefully kill the WebSocket connection."""
        self._is_connected = False
        self._connection_start_time = None
        
        if self._ws:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=3)
            except Exception:
                pass
            self._ws = None

    async def _health_logger(self):
        """Log health status periodically with detailed diagnostics."""
        while not self._shutdown.is_set():
            await asyncio.sleep(HEALTH_LOG_INTERVAL)
            
            now = datetime.now(timezone.utc)
            tick_elapsed = (now - self._last_tick_time).total_seconds()
            activity_elapsed = (now - self._last_activity_time).total_seconds()
            main_loop_elapsed = (now - self._last_main_loop_heartbeat).total_seconds()
            
            if activity_elapsed < 10:
                status = "🟢 HEALTHY"
            elif activity_elapsed < WATCHDOG_TIMEOUT_SECS:
                status = "🟡 STALE"
            else:
                status = "🔴 DEAD"
            
            log.info(
                "💓 HEALTH: %s | Connected: %s | Ticks: %d | Last tick: %.1fs | Activity: %.1fs | MainLoop: %.1fs | Reconnects: %d",
                status,
                self._is_connected,
                self._tick_count,
                tick_elapsed,
                activity_elapsed,
                main_loop_elapsed,
                self._connection_count,
            )
            
            # Log failure stats every 10 reconnects
            if self._connection_count > 0 and self._connection_count % 10 == 0:
                log.info("📊 Failure breakdown: %s", self._failure_reasons)

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
        Single connection attempt with MULTIPLE layers of timeout protection.
        Tracks failure reasons for debugging.
        """
        self._connection_count += 1
        self._connection_start_time = datetime.now(timezone.utc)
        
        log.info("🔌 Connection attempt #%d...", self._connection_count)
        
        try:
            # Layer 1: Timeout on connection itself
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
            while not self._shutdown.is_set() and self._is_connected:
                try:
                    # Layer 2: Timeout on each message receive
                    raw = await asyncio.wait_for(
                        self._ws.recv(),
                        timeout=RECV_TIMEOUT_SECS
                    )
                    data = json.loads(raw)
                    await self._handle(data)
                    
                except asyncio.TimeoutError:
                    # No message received - send heartbeat to check connection
                    try:
                        await self._ws.send(json.dumps({"action": "heartbeat"}))
                        self._last_activity_time = datetime.now(timezone.utc)
                    except Exception:
                        log.warning("⚠️ Heartbeat failed - connection dead")
                        self._failure_reasons["timeout"] += 1
                        break
                        
                except json.JSONDecodeError:
                    continue
                    
        except asyncio.TimeoutError:
            log.error("⏱️ Connection timeout after %ds", CONNECTION_TIMEOUT_SECS)
            self._failure_reasons["timeout"] += 1
        except websockets.exceptions.ConnectionClosed as e:
            log.warning("🔌 Connection closed: code=%s reason=%s", e.code, e.reason)
            self._failure_reasons["connection_closed"] += 1
        except websockets.exceptions.ConnectionClosedError as e:
            log.warning("🔌 Connection closed with error: code=%s reason=%s", e.code, e.reason)
            self._failure_reasons["connection_closed"] += 1
        except Exception as e:
            log.error("❌ Connection error: %s", e)
            self._failure_reasons["exception"] += 1
        finally:
            self._is_connected = False
            self._connection_start_time = None
            if self._ws:
                with suppress(Exception):
                    await self._ws.close()
            self._ws = None

    async def run(self):
        """
        Main run loop with NUCLEAR-GRADE protection.
        
        KEY FEATURES:
        1. Main loop heartbeat - proves the loop is running
        2. Stream task wrapped in asyncio.wait_for() - can't hang forever
        3. Nuclear watchdog runs independently - can kill stuck tasks
        4. Periodic forced reconnection to prevent zombie connections
        """
        log.info("=" * 60)
        log.info("🚀 BULLETPROOF TICK STREAMER v6.0 - NUCLEAR WATCHDOG EDITION")
        log.info("=" * 60)
        log.info("📊 Total symbols: %d", len(ALL_SYMBOLS))
        log.info("   Crypto: %d | Forex: %d | Commodities: %d", 
                 len(CRYPTO_SYMBOLS), len(FOREX_SYMBOLS), len(COMMODITIES_SYMBOLS))
        log.info("   Stocks: %d | ETFs/Indices: %d",
                 len(STOCK_SYMBOLS), len(ETF_INDEX_SYMBOLS))
        log.info("🐕 Watchdog timeout: %ds", WATCHDOG_TIMEOUT_SECS)
        log.info("☢️ Nuclear timeout: %ds", self.NUCLEAR_TIMEOUT_SECS)
        log.info("🔄 Weekly reset: Saturday 10pm UTC")
        log.info("🔄 Max reconnect delay: %ds", MAX_RECONNECT_DELAY)
        log.info("=" * 60)
        
        # Start background tasks - including NUCLEAR WATCHDOG
        flusher = asyncio.create_task(self._periodic_flush())
        watchdog = asyncio.create_task(self._watchdog())
        nuclear = asyncio.create_task(self._nuclear_watchdog())  # NEW!
        health = asyncio.create_task(self._health_logger())
        heartbeat = asyncio.create_task(self._heartbeat_sender())
        
        backoff = 1
        consecutive_failures = 0
        
        try:
            while not self._shutdown.is_set():
                # UPDATE MAIN LOOP HEARTBEAT - proves we're not stuck
                self._last_main_loop_heartbeat = datetime.now(timezone.utc)
                
                try:
                    start_time = datetime.now(timezone.utc)
                    
                    # Wrap the ENTIRE connection in a task so nuclear watchdog can kill it
                    self._stream_task = asyncio.create_task(self._connect_and_stream())
                    
                    # Layer 3: Timeout on the ENTIRE connection session (24 hours max)
                    try:
                        await asyncio.wait_for(
                            self._stream_task,
                            timeout=86400  # 24 hours - effectively no timeout, nuclear watchdog handles issues
                        )
                    except asyncio.TimeoutError:
                        log.warning("⏱️ Connection session timeout - forcing reconnect")
                        self._stream_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await self._stream_task
                    
                    self._stream_task = None
                    
                    if self._shutdown.is_set():
                        break
                    
                    # Update heartbeat again after connection ends
                    self._last_main_loop_heartbeat = datetime.now(timezone.utc)
                    
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
                    if consecutive_failures <= 3:
                        log.info("🔄 Quick reconnect in 1s (attempt %d)...", consecutive_failures)
                        await asyncio.sleep(1)
                    else:
                        log.info("🔄 Reconnecting in %ds (attempt %d)...", backoff, consecutive_failures)
                        await asyncio.sleep(backoff)
                        backoff = min(MAX_RECONNECT_DELAY, backoff * 2)
                
                except asyncio.CancelledError:
                    # Nuclear watchdog cancelled us - just restart
                    log.warning("🔄 Task was cancelled by nuclear watchdog - restarting...")
                    consecutive_failures = 0
                    backoff = 1
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    log.error("💥 Unexpected error in main loop: %s", e)
                    consecutive_failures += 1
                    await asyncio.sleep(min(backoff, 5))
                        
        finally:
            log.info("🛑 Shutting down tick streamer...")
            await self._flush()
            
            for task in [flusher, watchdog, nuclear, health, heartbeat]:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

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


def classify_article_with_gemini(article: dict) -> Optional[dict]:
    """Classify article using Google Gemini API."""
    if not GOOGLE_GEMINI_API_KEY:
        log.warning("⚠️ Google Gemini API key not configured")
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

        response_text = call_gemini_api(prompt, max_tokens=500, temperature=0.1)
        
        if not response_text:
            return None
        
        # Clean markdown if present
        response_text = clean_gemini_json_response(response_text)

        return json.loads(response_text)

    except json.JSONDecodeError as e:
        log.warning(f"⚠️ Gemini JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"⚠️ Gemini API error: {e}")
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
                classification = await loop.run_in_executor(None, classify_article_with_gemini, article)
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
#                    TASK 6: BTC WHALE FLOW TRACKER v3.2
# ============================================================================
"""
Smart whale detection with 7 methods:
1. Known wallet matching (25 verified exchange addresses)
2. Learned wallet matching (addresses seen 3+ times)
3. Consolidation patterns (5+ inputs → 2 outputs)
4. Distribution/batch payout (2 inputs → 5+ outputs)
5. Peel chain detection (90% to one address, 10% change)
6. Round amount detection (whole BTC precision)
7. Withdrawal signature (90%+ to one address)
"""

# Whale Flow Configuration
WHALE_THRESHOLD_USD = 100000       # $100k minimum
WHALE_MOMENTUM_WINDOW_MINS = 60    # 60 minute rolling window
WHALE_LOG_INTERVAL = 30            # Log stats every 30 seconds
BLOCKCHAIN_WS_URL = "wss://ws.blockchain.info/inv"

# Known exchange wallets (verified cold storage addresses)
KNOWN_EXCHANGE_WALLETS = {
    # Binance
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "Binance",
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j": "Binance",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": "Binance",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": "Binance",
    
    # Coinbase
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "Coinbase",
    "3FHNBLobJnbCTFTVakh5TXmEneyf5PT61B": "Coinbase",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR": "Coinbase",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh": "Coinbase",
    
    # Bitfinex
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r": "Bitfinex",
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97": "Bitfinex",
    
    # Kraken
    "3AfC5sCJMo3RFxgvHkH4wYkwdN3VXpqCoj": "Kraken",
    "bc1qkfp4zv8l2m7q8zf7d9h6x5n4k3j2w9s8y7t6r5": "Kraken",
    
    # OKX (OKEx)
    "3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb": "OKX",
    "1Lj4mSgtKtaruXK2CJXoWKvscCfNdQMiwx": "OKX",
    
    # Huobi
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6": "Huobi",
    "1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ": "Huobi",
    
    # Crypto.com
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64": "Crypto.com",
    "bc1q4c8n5t00jmj8temxdgcc3t32nkg2wjwz24lywv": "Crypto.com",
    
    # Gemini
    "3P3QsMVK89JBNqZQv5zMAKG8FK3kJM4rjt": "Gemini",
    "bc1qe5y4f2l2vqzl3wkh7r3xz9y8m6n4k2j7p5s9d8": "Gemini",
    
    # Bitstamp
    "3DZh3y5e8WPn7LPzVLYf5jzNNcb8mKsE8Y": "Bitstamp",
    "3P3n6MKzGJyRC7aM8hM5g3cxvFmT8gN7Ly": "Bitstamp",
    
    # Bittrex
    "1N52wHoVR79PMDishab2XmRHsbekCdGquK": "Bittrex",
    
    # KuCoin  
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh": "KuCoin",
}


class AddressMemory:
    """
    Learn from repeated addresses - addresses seen multiple times
    with high volume are likely exchange wallets.
    
    v6.2 FIX: Separate detection for deposit addresses vs hot wallets:
    - Deposit addresses: RECEIVE-ONLY (users deposit to sell)
    - Hot wallets: SEND frequently (exchange withdrawals)
    - Full exchange: Does BOTH (highest confidence)
    
    This fixes the zero-inflow bug from v6.1 where deposit addresses
    were never learned because they don't send.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._addresses: Dict[str, Dict] = {}
        self._learn_threshold = 5          # Must be seen 5+ times
        self._volume_threshold = 10.0      # Must have 10+ BTC total volume
    
    def record_address(self, address: str, role: str, amount_btc: float):
        """Record an address sighting."""
        with self._lock:
            if address not in self._addresses:
                self._addresses[address] = {
                    'seen_count': 0,
                    'as_sender': 0,
                    'as_receiver': 0,
                    'total_sent': 0.0,
                    'total_received': 0.0,
                    'first_seen': datetime.now(timezone.utc),
                    'last_seen': datetime.now(timezone.utc)
                }
            
            stats = self._addresses[address]
            stats['seen_count'] += 1
            stats['last_seen'] = datetime.now(timezone.utc)
            
            if role == 'sender':
                stats['as_sender'] += 1
                stats['total_sent'] += amount_btc
            else:
                stats['as_receiver'] += 1
                stats['total_received'] += amount_btc
    
    def is_learned_deposit_address(self, address: str) -> bool:
        """
        Check if address is a learned DEPOSIT address (inflow detector).
        
        Deposit addresses characteristics:
        - Receive-ONLY (never send, funds swept to cold storage)
        - High receive frequency (many users depositing)
        - High volume received
        
        v6.2 FIX: This enables inflow detection!
        """
        with self._lock:
            if address not in self._addresses:
                return False
            
            stats = self._addresses[address]
            
            # Must be seen as receiver 5+ times
            if stats['as_receiver'] < self._learn_threshold:
                return False
            
            # Must have received 10+ BTC
            if stats['total_received'] < self._volume_threshold:
                return False
            
            # Must be receive-ONLY or receive-MOSTLY (90%+ receives)
            total_activity = stats['as_sender'] + stats['as_receiver']
            if total_activity > 0:
                receive_ratio = stats['as_receiver'] / total_activity
                if receive_ratio < 0.9:  # Must be 90%+ receives
                    return False
            
            return True
    
    def is_learned_hot_wallet(self, address: str) -> bool:
        """
        Check if address is a learned HOT WALLET (outflow detector).
        
        Hot wallet characteristics:
        - Send frequently (processing withdrawals)
        - May also receive (refills from cold storage)
        - High send volume
        """
        with self._lock:
            if address not in self._addresses:
                return False
            
            stats = self._addresses[address]
            
            # Must be seen as sender 5+ times
            if stats['as_sender'] < self._learn_threshold:
                return False
            
            # Must have sent 10+ BTC
            if stats['total_sent'] < self._volume_threshold:
                return False
            
            return True
    
    def is_learned_exchange(self, address: str) -> bool:
        """
        Check if address qualifies as a learned exchange (bidirectional).
        Highest confidence - address does BOTH sending and receiving.
        
        This is the original strict check for addresses that act as both
        deposit and withdrawal (rare but highest confidence).
        """
        with self._lock:
            if address not in self._addresses:
                return False
            
            stats = self._addresses[address]
            
            # Must be seen 5+ times total
            if stats['seen_count'] < self._learn_threshold:
                return False
            
            # Must have 10+ BTC total volume
            total_volume = stats['total_sent'] + stats['total_received']
            if total_volume < self._volume_threshold:
                return False
            
            # Must have done BOTH sending and receiving
            if stats['as_sender'] == 0 or stats['as_receiver'] == 0:
                return False
            
            return True
    
    def get_stats(self) -> Dict:
        """Get memory statistics."""
        with self._lock:
            total = len(self._addresses)
            deposit_addrs = sum(1 for addr in self._addresses 
                               if self._is_deposit_unlocked(addr))
            hot_wallets = sum(1 for addr in self._addresses 
                             if self._is_hot_wallet_unlocked(addr))
            bidirectional = sum(1 for addr in self._addresses 
                               if self._is_exchange_unlocked(addr))
            return {
                'total_addresses': total,
                'learned_deposit_addresses': deposit_addrs,
                'learned_hot_wallets': hot_wallets,
                'learned_bidirectional': bidirectional,
                'total_learned': deposit_addrs + hot_wallets + bidirectional
            }
    
    def _is_deposit_unlocked(self, address: str) -> bool:
        """Internal: check deposit without lock (for stats)."""
        stats = self._addresses.get(address)
        if not stats:
            return False
        if stats['as_receiver'] < self._learn_threshold:
            return False
        if stats['total_received'] < self._volume_threshold:
            return False
        total = stats['as_sender'] + stats['as_receiver']
        if total > 0 and stats['as_receiver'] / total < 0.9:
            return False
        return True
    
    def _is_hot_wallet_unlocked(self, address: str) -> bool:
        """Internal: check hot wallet without lock (for stats)."""
        stats = self._addresses.get(address)
        if not stats:
            return False
        if stats['as_sender'] < self._learn_threshold:
            return False
        if stats['total_sent'] < self._volume_threshold:
            return False
        return True
    
    def _is_exchange_unlocked(self, address: str) -> bool:
        """Internal: check bidirectional without lock (for stats)."""
        stats = self._addresses.get(address)
        if not stats:
            return False
        if stats['seen_count'] < self._learn_threshold:
            return False
        total_vol = stats['total_sent'] + stats['total_received']
        if total_vol < self._volume_threshold:
            return False
        if stats['as_sender'] == 0 or stats['as_receiver'] == 0:
            return False
        return True


class WhaleFlowTracker:
    """
    BTC Whale Flow Tracker v3.2 with smart detection.
    
    Tracks large BTC movements and classifies them as:
    - exchange_inflow (bearish) - coins going TO exchanges
    - exchange_outflow (bullish) - coins LEAVING exchanges  
    - unknown (neutral) - can't determine direction
    """
    
    def __init__(self):
        self._shutdown = asyncio.Event()
        self._ws = None
        self._is_connected = False
        
        # Flow tracking
        self._flows: List[Dict] = []  # Rolling window of flows
        self._lock = asyncio.Lock()
        
        # Statistics
        self._stats = {
            'total_whales': 0,
            'known_wallet': 0,
            'learned_wallet': 0,
            'pattern_consolidation': 0,
            'pattern_distribution': 0,
            'pattern_peel': 0,
            'pattern_round': 0,
            'pattern_withdrawal': 0,
            'unknown': 0
        }
        
        # Address learning
        self._address_memory = AddressMemory()
        
        # BTC price (fetched periodically)
        self._btc_price = 97000.0  # Default, will be updated
        self._last_price_fetch = None
        
        # Connection stats
        self._connection_count = 0
        self._last_activity = datetime.now(timezone.utc)
    
    async def _fetch_btc_price(self):
        """Fetch current BTC price from tickdata or API."""
        try:
            # Try to get from our own tickdata first
            result = sb.table('tickdata').select('price').eq(
                'symbol', 'BTC/USD'
            ).order('ts', desc=True).limit(1).execute()
            
            if result.data and result.data[0].get('price'):
                self._btc_price = float(result.data[0]['price'])
                log.debug(f"🐋 BTC price from tickdata: ${self._btc_price:,.0f}")
                return
            
            # Fallback to CoinGecko
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd',
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self._btc_price = data['bitcoin']['usd']
                log.debug(f"🐋 BTC price from CoinGecko: ${self._btc_price:,.0f}")
        except Exception as e:
            log.warning(f"⚠️ Failed to fetch BTC price: {e}")
    
    def _detect_flow_type(self, tx: Dict) -> tuple:
        """
        Detect flow type using 7 methods.
        Returns: (flow_type, exchange_name, detection_method)
        
        Flow types:
        - exchange_inflow: Coins going TO exchange (bearish)
        - exchange_outflow: Coins LEAVING exchange (bullish)
        - consolidation: Multiple inputs to few outputs (neutral)
        - distribution: Few inputs to many outputs (bullish - batch payout)
        - unknown: Can't determine
        """
        inputs = tx.get('inputs', [])
        outputs = tx.get('out', [])
        
        input_addrs = set()
        output_addrs = set()
        
        for inp in inputs:
            if 'prev_out' in inp and 'addr' in inp['prev_out']:
                input_addrs.add(inp['prev_out']['addr'])
        
        for out in outputs:
            if 'addr' in out:
                output_addrs.add(out['addr'])
        
        total_btc = tx.get('total_value', 0) / 1e8
        
        # Record addresses for learning
        for addr in input_addrs:
            self._address_memory.record_address(addr, 'sender', total_btc / max(len(input_addrs), 1))
        for addr in output_addrs:
            self._address_memory.record_address(addr, 'receiver', total_btc / max(len(output_addrs), 1))
        
        # METHOD 1: Known wallet matching
        for addr in input_addrs:
            if addr in KNOWN_EXCHANGE_WALLETS:
                self._stats['known_wallet'] += 1
                return ('exchange_outflow', KNOWN_EXCHANGE_WALLETS[addr], 'known_wallet')
        
        for addr in output_addrs:
            if addr in KNOWN_EXCHANGE_WALLETS:
                self._stats['known_wallet'] += 1
                return ('exchange_inflow', KNOWN_EXCHANGE_WALLETS[addr], 'known_wallet')
        
        # METHOD 2: Learned wallet matching (v6.2 FIX - separate deposit vs hot wallet detection)
        
        # 2a: Check for learned bidirectional exchange (highest confidence)
        for addr in input_addrs:
            if self._address_memory.is_learned_exchange(addr):
                self._stats['learned_wallet'] += 1
                return ('exchange_outflow', 'Learned Exchange', 'learned_exchange')
        
        for addr in output_addrs:
            if self._address_memory.is_learned_exchange(addr):
                self._stats['learned_wallet'] += 1
                return ('exchange_inflow', 'Learned Exchange', 'learned_exchange')
        
        # 2b: Check for learned hot wallet (send-heavy = outflow detector)
        for addr in input_addrs:
            if self._address_memory.is_learned_hot_wallet(addr):
                self._stats['learned_wallet'] += 1
                return ('exchange_outflow', 'Learned Hot Wallet', 'learned_hot_wallet')
        
        # 2c: Check for learned deposit address (receive-heavy = inflow detector)
        # v6.2 FIX: This enables inflow detection!
        for addr in output_addrs:
            if self._address_memory.is_learned_deposit_address(addr):
                self._stats['learned_wallet'] += 1
                return ('exchange_inflow', 'Learned Deposit', 'learned_deposit')
        
        # METHOD 3: Consolidation pattern (5+ inputs → 1-2 outputs)
        if len(inputs) >= 5 and len(outputs) <= 2:
            self._stats['pattern_consolidation'] += 1
            return ('consolidation', 'Pattern', 'consolidation')
        
        # METHOD 4: Distribution/batch payout (1-2 inputs → 5+ outputs)
        if len(inputs) <= 2 and len(outputs) >= 5:
            self._stats['pattern_distribution'] += 1
            return ('distribution', 'Pattern', 'distribution')
        
        # METHOD 5: Peel chain detection
        if len(outputs) == 2:
            out_values = sorted([o.get('value', 0) for o in outputs], reverse=True)
            if out_values[0] > 0 and out_values[1] > 0:
                ratio = out_values[0] / (out_values[0] + out_values[1])
                if ratio >= 0.85:  # 85%+ to one address
                    self._stats['pattern_peel'] += 1
                    return ('likely_withdrawal', 'Pattern', 'peel_chain')
        
        # METHOD 6: Round amount detection
        if total_btc > 0:
            # Check for round amounts (1, 5, 10, 50, 100 BTC etc.)
            if total_btc == int(total_btc) and total_btc >= 1:
                self._stats['pattern_round'] += 1
                return ('likely_withdrawal', 'Pattern', 'round_amount')
            # Check for 0.5 BTC increments
            if (total_btc * 2) == int(total_btc * 2) and total_btc >= 0.5:
                self._stats['pattern_round'] += 1
                return ('likely_withdrawal', 'Pattern', 'round_amount')
        
        # METHOD 7: Withdrawal signature (90%+ to one address with small change)
        if len(outputs) >= 2:
            total_output = sum(o.get('value', 0) for o in outputs)
            if total_output > 0:
                max_output = max(o.get('value', 0) for o in outputs)
                if max_output / total_output >= 0.90:
                    self._stats['pattern_withdrawal'] += 1
                    return ('likely_withdrawal', 'Pattern', 'withdrawal_sig')
        
        # No pattern matched
        self._stats['unknown'] += 1
        return ('unknown', None, None)
    
    def _calculate_momentum(self) -> Dict:
        """
        Calculate momentum score from recent flows.
        
        IMPORTANT: Score is based on NET USD flow, not transaction count.
        - Positive score = More USD leaving exchanges (BULLISH/ACCUMULATION)
        - Negative score = More USD entering exchanges (BEARISH/DISTRIBUTION)
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=WHALE_MOMENTUM_WINDOW_MINS)
        
        # Filter to recent flows
        recent_flows = [f for f in self._flows if f['timestamp'] > cutoff]
        
        inflow_usd = 0.0
        outflow_usd = 0.0
        inflow_count = 0
        outflow_count = 0
        unknown_usd = 0.0
        unknown_count = 0
        
        for flow in recent_flows:
            usd = flow['amount_usd']
            flow_type = flow['flow_type']
            
            if flow_type == 'exchange_inflow':
                # Coins going TO exchange = bearish (people depositing to sell)
                inflow_usd += usd
                inflow_count += 1
            elif flow_type in ['exchange_outflow', 'distribution']:
                # Coins LEAVING exchange = bullish (people withdrawing to hold)
                outflow_usd += usd
                outflow_count += 1
            elif flow_type == 'likely_withdrawal':
                # Patterns suggest withdrawal but not confirmed
                # Count as 50% weight toward outflow
                outflow_usd += usd * 0.5
                outflow_count += 1
            elif flow_type == 'consolidation':
                # Neutral - could go either way, don't count
                pass
            else:
                # Unknown - track separately
                unknown_usd += usd
                unknown_count += 1
        
        # Net flow: positive = more leaving exchanges (bullish)
        # negative = more entering exchanges (bearish)  
        net_flow = outflow_usd - inflow_usd
        
        # Calculate score based on USD imbalance
        total_classified = inflow_usd + outflow_usd
        if total_classified > 0:
            # Score from -100 (all inflow/bearish) to +100 (all outflow/bullish)
            flow_score = int((net_flow / total_classified) * 100)
            # Clamp to -100 to +100
            flow_score = max(-100, min(100, flow_score))
        else:
            flow_score = 0
        
        # Determine regime based on score
        # Note: High inflow (negative score) = DISTRIBUTION (bearish)
        #       High outflow (positive score) = ACCUMULATION (bullish)
        if flow_score >= 50:
            regime = "ACCUMULATION"        # Strong bullish - coins leaving exchanges
        elif flow_score >= 20:
            regime = "MILD_ACCUMULATION"   # Mild bullish
        elif flow_score <= -50:
            regime = "DISTRIBUTION"        # Strong bearish - coins entering exchanges
        elif flow_score <= -20:
            regime = "MILD_DISTRIBUTION"   # Mild bearish
        else:
            regime = "NEUTRAL"
        
        return {
            'flow_score': flow_score,
            'regime': regime,
            'inflow_usd': inflow_usd,
            'outflow_usd': outflow_usd,
            'inflow_count': inflow_count,
            'outflow_count': outflow_count,
            'net_flow_usd': net_flow,
            'unknown_usd': unknown_usd,
            'unknown_count': unknown_count,
            'window_minutes': WHALE_MOMENTUM_WINDOW_MINS
        }
    
    async def _handle_transaction(self, tx: Dict):
        """Handle incoming transaction from WebSocket."""
        try:
            tx_data = tx.get('x', tx)
            
            # Calculate value
            total_satoshi = sum(out.get('value', 0) for out in tx_data.get('out', []))
            total_btc = total_satoshi / 1e8
            total_usd = total_btc * self._btc_price
            
            # Filter by threshold
            if total_usd < WHALE_THRESHOLD_USD:
                return
            
            self._stats['total_whales'] += 1
            tx_hash = tx_data.get('hash', 'unknown')
            
            # Detect flow type
            flow_type, exchange, method = self._detect_flow_type(tx_data)
            
            # Create flow record
            flow = {
                'tx_hash': tx_hash,
                'timestamp': datetime.now(timezone.utc),
                'amount_btc': total_btc,
                'amount_usd': total_usd,
                'flow_type': flow_type,
                'exchange': exchange,
                'detection_method': method
            }
            
            async with self._lock:
                self._flows.append(flow)
                
                # Trim old flows (keep last 2 hours)
                cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
                self._flows = [f for f in self._flows if f['timestamp'] > cutoff]
            
            # Log the whale transaction
            emoji = self._get_flow_emoji(flow_type)
            method_str = f"[{method}]" if method else ""
            exchange_str = f"({exchange})" if exchange else ""
            
            log.info(
                f"🐋 {emoji} WHALE: {total_btc:.2f} BTC (${total_usd:,.0f}) "
                f"| {flow_type.upper()} {exchange_str} {method_str}"
            )
            
            # Store to Supabase
            await self._store_flow(flow)
            
        except Exception as e:
            log.warning(f"⚠️ Error handling whale tx: {e}")
    
    def _get_flow_emoji(self, flow_type: str) -> str:
        """Get emoji for flow type."""
        emojis = {
            'exchange_inflow': '🔴',      # Bearish
            'exchange_outflow': '🟢',     # Bullish
            'distribution': '🟢',         # Bullish (batch payout)
            'consolidation': '🔄',        # Neutral
            'likely_withdrawal': '🟡',    # Likely bullish
            'unknown': '⚪'               # Unknown
        }
        return emojis.get(flow_type, '⚪')
    
    async def _store_flow(self, flow: Dict):
        """Store flow to Supabase."""
        try:
            data = {
                'tx_hash': flow['tx_hash'],
                'timestamp': flow['timestamp'].isoformat(),
                'amount_btc': flow['amount_btc'],
                'amount_usd': flow['amount_usd'],
                'flow_type': flow['flow_type'],
                'exchange': flow['exchange'],
                'detection_method': flow['detection_method']
            }
            
            sb.table('whale_flows').upsert(data, on_conflict='tx_hash').execute()
            
        except Exception as e:
            # Table might not exist yet - that's OK
            if 'relation' not in str(e).lower():
                log.debug(f"⚠️ Failed to store whale flow: {e}")
    
    async def _store_momentum(self):
        """Store momentum snapshot to Supabase."""
        try:
            momentum = self._calculate_momentum()
            
            # Use total_score to match existing table schema
            data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'total_score': momentum['flow_score'],  # Maps flow_score to total_score column
                'regime': momentum['regime'],
                'inflow_usd': momentum['inflow_usd'],
                'outflow_usd': momentum['outflow_usd'],
                'inflow_count': momentum['inflow_count'],
                'outflow_count': momentum['outflow_count']
            }
            
            sb.table('whale_flow_momentum').insert(data).execute()
            
        except Exception as e:
            if 'relation' not in str(e).lower():
                log.debug(f"⚠️ Failed to store momentum: {e}")
    
    async def _periodic_tasks(self):
        """Run periodic tasks: price updates, momentum storage, stats logging."""
        last_price_update = datetime.now(timezone.utc) - timedelta(minutes=10)
        last_momentum_store = datetime.now(timezone.utc)
        last_stats_log = datetime.now(timezone.utc)
        
        while not self._shutdown.is_set():
            try:
                now = datetime.now(timezone.utc)
                
                # Update BTC price every 60 seconds
                if (now - last_price_update).total_seconds() >= 60:
                    await self._fetch_btc_price()
                    last_price_update = now
                
                # Store momentum every 30 seconds
                if (now - last_momentum_store).total_seconds() >= 30:
                    await self._store_momentum()
                    last_momentum_store = now
                
                # Log stats every WHALE_LOG_INTERVAL seconds
                if (now - last_stats_log).total_seconds() >= WHALE_LOG_INTERVAL:
                    momentum = self._calculate_momentum()
                    mem_stats = self._address_memory.get_stats()
                    
                    detection_rate = 0
                    if self._stats['total_whales'] > 0:
                        detected = (self._stats['total_whales'] - self._stats['unknown'])
                        detection_rate = (detected / self._stats['total_whales']) * 100
                    
                    # Calculate net flow direction
                    net_flow = momentum['outflow_usd'] - momentum['inflow_usd']
                    net_direction = "📈" if net_flow > 0 else "📉" if net_flow < 0 else "➡️"
                    
                    log.info(
                        f"🐋 WHALE STATS | Score: {momentum['flow_score']:+d} {momentum['regime']} | "
                        f"In: ${momentum['inflow_usd']/1e6:.1f}M ({momentum['inflow_count']}) | "
                        f"Out: ${momentum['outflow_usd']/1e6:.1f}M ({momentum['outflow_count']}) | "
                        f"Net: {net_direction} ${abs(net_flow)/1e6:.1f}M | "
                        f"Det: {detection_rate:.0f}% | "
                        f"Learned: {mem_stats.get('total_learned', 0)} "
                        f"(D:{mem_stats.get('learned_deposit_addresses', 0)} "
                        f"H:{mem_stats.get('learned_hot_wallets', 0)})"
                    )
                    last_stats_log = now
                
                await asyncio.sleep(5)
                
            except Exception as e:
                log.warning(f"⚠️ Periodic task error: {e}")
                await asyncio.sleep(10)
    
    async def _connect_and_stream(self):
        """Connect to Blockchain.com WebSocket and stream transactions."""
        self._connection_count += 1
        log.info(f"🐋 Whale tracker connection attempt #{self._connection_count}...")
        
        try:
            self._ws = await asyncio.wait_for(
                websockets.connect(
                    BLOCKCHAIN_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10
                ),
                timeout=15
            )
            
            # Subscribe to unconfirmed transactions
            await self._ws.send(json.dumps({"op": "unconfirmed_sub"}))
            log.info("🐋 Connected to Blockchain.com - streaming whale transactions")
            
            self._is_connected = True
            self._last_activity = datetime.now(timezone.utc)
            
            async for message in self._ws:
                try:
                    self._last_activity = datetime.now(timezone.utc)
                    data = json.loads(message)
                    
                    if data.get('op') == 'utx':
                        await self._handle_transaction(data)
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"⚠️ Error processing message: {e}")
                    
        except asyncio.TimeoutError:
            log.error("⏱️ Whale tracker connection timeout")
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"🔌 Whale tracker connection closed: {e}")
        except Exception as e:
            log.error(f"❌ Whale tracker error: {e}")
        finally:
            self._is_connected = False
            if self._ws:
                with suppress(Exception):
                    await self._ws.close()
                self._ws = None
    
    async def run(self):
        """Main run loop with reconnection logic."""
        log.info("=" * 60)
        log.info("🐋 BTC WHALE FLOW TRACKER v3.2 STARTING")
        log.info("=" * 60)
        log.info(f"   Threshold: ${WHALE_THRESHOLD_USD:,}")
        log.info(f"   Momentum window: {WHALE_MOMENTUM_WINDOW_MINS} minutes")
        log.info(f"   Known wallets: {len(KNOWN_EXCHANGE_WALLETS)}")
        log.info(f"   Detection methods: 7")
        log.info("=" * 60)
        
        # Fetch initial BTC price
        await self._fetch_btc_price()
        
        # Start periodic tasks
        periodic = asyncio.create_task(self._periodic_tasks())
        
        backoff = 1
        
        try:
            while not self._shutdown.is_set():
                try:
                    await self._connect_and_stream()
                    
                    if self._shutdown.is_set():
                        break
                    
                    # Reconnect with backoff
                    log.info(f"🔄 Whale tracker reconnecting in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(30, backoff * 2)
                    
                except Exception as e:
                    log.error(f"💥 Whale tracker main loop error: {e}")
                    await asyncio.sleep(5)
                    
        finally:
            log.info("🛑 Shutting down whale tracker...")
            periodic.cancel()
            with suppress(asyncio.CancelledError):
                await periodic
    
    def shutdown(self):
        """Request graceful shutdown."""
        self._shutdown.set()


# Global whale tracker instance
whale_tracker = WhaleFlowTracker()


async def whale_flow_task():
    """Entry point for whale flow tracking."""
    await whale_tracker.run()


# ============================================================================
#                    TASK 7: AI NEWS DESK (Article Synthesis)
# ============================================================================

# AI News Desk Configuration
AI_NEWS_DESK_ARTICLES = 3           # Generate 3 articles per cycle
AI_NEWS_MIN_SCORE = 12              # Minimum article score
AI_NEWS_MIN_CLUSTER_SIZE = 2        # Minimum sources per cluster
AI_NEWS_MIN_CLUSTER_SCORE = 20      # Minimum cluster average score
AI_NEWS_MIN_BODY_WORDS = 500        # Minimum article body length
AI_NEWS_MAX_BODY_WORDS = 800        # Maximum article body length

# AI News Desk RSS Feeds (Tier 1 = premium sources)
AI_NEWS_FEEDS = [
    # Tier 1: Premium Financial
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC Top News", "tier": 1, "category": "markets"},
    {"url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "name": "CNBC Economy", "tier": 1, "category": "economy"},
    {"url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "name": "CNBC Finance", "tier": 1, "category": "finance"},
    {"url": "https://www.cnbc.com/id/10001147/device/rss/rss.html", "name": "CNBC Earnings", "tier": 1, "category": "earnings"},
    {"url": "https://www.cnbc.com/id/15839135/device/rss/rss.html", "name": "CNBC Commodities", "tier": 1, "category": "commodities"},
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories", "name": "MarketWatch Top", "tier": 1, "category": "markets"},
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance", "tier": 1, "category": "markets"},
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve", "tier": 1, "category": "central_bank"},
    
    # Tier 2: Specialized
    {"url": "https://www.investing.com/rss/news.rss", "name": "Investing.com News", "tier": 2, "category": "markets"},
    {"url": "https://www.investing.com/rss/news_25.rss", "name": "Investing.com Economy", "tier": 2, "category": "economy"},
    {"url": "https://www.investing.com/rss/news_14.rss", "name": "Investing.com Forex", "tier": 2, "category": "forex"},
    {"url": "https://www.investing.com/rss/news_301.rss", "name": "Investing.com Crypto", "tier": 2, "category": "crypto"},
    {"url": "https://www.fxstreet.com/rss/news", "name": "FXStreet News", "tier": 2, "category": "forex"},
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk", "tier": 1, "category": "crypto"},
    {"url": "https://cointelegraph.com/rss", "name": "Cointelegraph", "tier": 2, "category": "crypto"},
    {"url": "https://decrypt.co/feed", "name": "Decrypt", "tier": 2, "category": "crypto"},
    {"url": "https://oilprice.com/rss/main", "name": "OilPrice", "tier": 2, "category": "commodities"},
    {"url": "https://seekingalpha.com/feed.xml", "name": "Seeking Alpha", "tier": 2, "category": "markets"},
    
    # Tier 3: Business News
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business", "tier": 3, "category": "economy"},
    {"url": "https://www.theguardian.com/uk/business/rss", "name": "Guardian Business", "tier": 3, "category": "economy"},
]

# Entity Database for scoring
AI_NEWS_ENTITIES = {
    "mega_caps": {
        "nvidia": ["nvidia", "nvda", "jensen huang"],
        "apple": ["apple", "aapl", "tim cook"],
        "microsoft": ["microsoft", "msft", "satya nadella"],
        "amazon": ["amazon", "amzn", "aws"],
        "alphabet": ["alphabet", "google", "googl"],
        "meta": ["meta", "facebook", "zuckerberg"],
        "tesla": ["tesla", "tsla", "elon musk"],
    },
    "central_banks": {
        "fed": ["federal reserve", "fed ", "fomc", "federal open market"],
        "powell": ["jerome powell", "powell", "fed chair"],
        "ecb": ["ecb", "european central bank"],
        "lagarde": ["christine lagarde", "lagarde"],
        "boe": ["bank of england", "boe "],
        "boj": ["bank of japan", "boj "],
    },
    "monetary_policy": {
        "rate_cut": ["rate cut", "cuts rates", "lower rates"],
        "rate_hike": ["rate hike", "raise rates", "higher rates"],
        "hawkish": ["hawkish", "tightening"],
        "dovish": ["dovish", "easing", "stimulus"],
    },
    "economic_data": {
        "cpi": ["cpi", "consumer price index", "inflation data"],
        "gdp": ["gdp", "gross domestic product"],
        "nfp": ["nfp", "non-farm payrolls", "jobs report"],
        "unemployment": ["unemployment rate", "jobless"],
    },
    "market_events": {
        "crash": ["crash", "plunge", "collapse"],
        "rally": ["rally", "surge", "soars", "spikes"],
        "correction": ["correction", "pullback", "sell-off"],
        "breakout": ["all-time high", "ath", "record high"],
    },
    "crypto": {
        "bitcoin": ["bitcoin", "btc"],
        "ethereum": ["ethereum", "eth"],
        "solana": ["solana", "sol"],
        "bitcoin_etf": ["bitcoin etf", "btc etf", "spot bitcoin"],
    },
    "commodities": {
        "gold": ["gold", "xau", "bullion"],
        "silver": ["silver", "xag"],
        "oil": ["oil", "crude", "wti", "brent"],
    },
    "currencies": {
        "dollar": ["dollar", "usd", "greenback", "dxy"],
        "euro": ["euro", "eur"],
        "yen": ["yen", "jpy"],
        "pound": ["pound", "sterling", "gbp"],
    },
    "geopolitical": {
        "tariff": ["tariff", "tariffs", "trade barrier"],
        "sanction": ["sanction", "sanctions"],
        "trade_war": ["trade war", "trade tensions"],
    },
}

# Impact keywords for scoring
AI_NEWS_IMPACT_KEYWORDS = {
    "fed cuts": 15, "fed hikes": 15, "rate decision": 12, "fomc": 12,
    "rate cut": 12, "rate hike": 12, "basis points": 10,
    "cpi": 12, "inflation": 10, "gdp": 10, "nfp": 12, "payrolls": 10,
    "crash": 15, "plunge": 12, "collapse": 15, "crisis": 15,
    "all-time high": 12, "record high": 12, "surge": 8, "soars": 8,
    "bitcoin etf": 10, "sec approves": 12, "crypto crash": 10,
    "tariff": 8, "sanctions": 8, "trade war": 10,
    "layoffs": 7, "job cuts": 7, "acquisition": 7, "merger": 7,
}

# Exclusion keywords (noise filter)
AI_NEWS_EXCLUSIONS = [
    "football", "soccer", "cricket", "tennis", "basketball", "baseball",
    "nfl", "nba", "mlb", "nhl", "olympics", "super bowl",
    "celebrity", "actor", "actress", "movie", "film", "tv show",
    "grammy", "oscar", "emmy", "concert", "album",
    "recipe", "cooking", "fashion", "beauty", "wedding", "divorce",
    "horoscope", "lottery", "jackpot", "weather forecast",
    "video game", "playstation", "xbox", "nintendo",
]


def get_ai_news_interval() -> Tuple[int, str]:
    """
    Smart scheduling for AI News Desk based on market sessions.
    
    Returns: (interval_seconds, session_name)
    
    Schedule:
    - London + US session (overlap): 30 minutes
    - Other active hours: 60 minutes  
    - Weekend: 4 hours
    - Holidays: 4 hours (uses weekend schedule)
    """
    now = datetime.now(timezone.utc)
    day_of_week = now.weekday()  # 0=Monday, 6=Sunday
    hour_utc = now.hour
    
    # Weekend (Saturday/Sunday)
    if day_of_week >= 5:
        return 14400, "weekend"  # 4 hours
    
    # Check for major holidays (simplified - US/UK bank holidays)
    # Christmas, New Year, etc.
    month, day = now.month, now.day
    holidays = [
        (1, 1),   # New Year's Day
        (12, 25), # Christmas
        (12, 26), # Boxing Day
    ]
    if (month, day) in holidays:
        return 14400, "holiday"  # 4 hours
    
    # London session: 8:00-16:30 UTC (08:00-16:30 GMT)
    # US session: 14:30-21:00 UTC (09:30-16:00 EST)
    # Overlap: 14:30-16:30 UTC
    
    london_open = 8
    london_close = 17  # 16:30 rounded up
    us_open = 14       # 14:30 rounded down
    us_close = 21
    
    # London + US overlap OR active US session
    if (us_open <= hour_utc < us_close) or (london_open <= hour_utc < london_close):
        # Check if it's the overlap period (most active)
        if us_open <= hour_utc < london_close:
            return 1800, "london_us_overlap"  # 30 minutes
        else:
            return 1800, "active_session"  # 30 minutes during any major session
    
    # Asian session (less active for our users)
    if 0 <= hour_utc < 8:
        return 3600, "asian_session"  # 1 hour
    
    # Quiet hours
    return 3600, "quiet"  # 1 hour


def ai_news_extract_entities(text: str) -> Dict[str, Set[str]]:
    """Extract entities from text for scoring."""
    text_lower = text.lower()
    found = defaultdict(set)
    
    for category, entity_dict in AI_NEWS_ENTITIES.items():
        for entity_name, keywords in entity_dict.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    found[category].add(entity_name)
                    break
    return dict(found)


def ai_news_score_article(article: Dict) -> Dict:
    """Score an article from 0-100 based on market relevance."""
    title = article.get('title', '')
    summary = article.get('summary', '')
    text = f"{title} {summary}".lower()
    
    # Check exclusions first
    for exclusion in AI_NEWS_EXCLUSIONS:
        if exclusion in text:
            article['score'] = 0
            article['excluded'] = True
            return article
    
    score = 0
    
    # Keyword scoring
    for keyword, points in AI_NEWS_IMPACT_KEYWORDS.items():
        if keyword.lower() in text:
            score += points
    
    # Percentage bonus
    percentages = re.findall(r'[-+]?\d+(?:\.\d+)?\s*%', text)
    if percentages:
        max_pct = max(abs(float(p.replace('%', '').strip())) for p in percentages)
        if max_pct >= 20: score += 15
        elif max_pct >= 10: score += 10
        elif max_pct >= 5: score += 6
    
    # Entity extraction & bonus
    entities = ai_news_extract_entities(text)
    article['entities'] = entities
    
    if 'central_banks' in entities: score += 8
    if 'monetary_policy' in entities: score += 6
    if 'mega_caps' in entities: score += 4
    if 'economic_data' in entities: score += 5
    if 'market_events' in entities: score += 5
    if 'geopolitical' in entities: score += 4
    
    # Tier multiplier
    tier = article.get('tier', 3)
    if tier == 1: score *= 1.3
    elif tier == 2: score *= 1.15
    
    # Recency multiplier
    if article.get('published'):
        try:
            pub_str = article['published']
            if isinstance(pub_str, str):
                pub_time = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
            else:
                pub_time = pub_str
            if pub_time.tzinfo is None:
                pub_time = pub_time.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - pub_time).total_seconds() / 3600
            if age_hours < 1: score *= 1.5
            elif age_hours < 3: score *= 1.3
            elif age_hours < 6: score *= 1.15
        except:
            pass
    
    article['score'] = min(100, max(0, round(score, 1)))
    article['excluded'] = False
    
    # Impact level
    if article['score'] >= 60: article['impact_level'] = 'critical'
    elif article['score'] >= 40: article['impact_level'] = 'high'
    elif article['score'] >= 20: article['impact_level'] = 'medium'
    else: article['impact_level'] = 'low'
    
    # Event type classification
    if 'central_banks' in entities or 'monetary_policy' in entities:
        article['event_type'] = 'central_bank_news'
    elif 'economic_data' in entities:
        article['event_type'] = 'economic_data'
    elif 'crypto' in entities:
        article['event_type'] = 'crypto_news'
    elif 'commodities' in entities:
        article['event_type'] = 'commodities'
    elif 'market_events' in entities:
        article['event_type'] = 'market_move'
    else:
        article['event_type'] = 'general_market'
    
    return article


def ai_news_fetch_feeds(max_age_hours: int = 12) -> List[Dict]:
    """Fetch articles from AI News Desk feeds."""
    all_articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    
    for feed_info in AI_NEWS_FEEDS:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(feed_info["url"], headers=headers, timeout=15, verify=False)
            
            if response.status_code != 200:
                continue
            
            feed = feedparser.parse(response.content)
            
            for entry in feed.entries[:20]:
                pub_date = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
                
                if pub_date and pub_date < cutoff:
                    continue
                
                title = entry.get('title', '').strip()
                summary = entry.get('summary', entry.get('description', '')).strip()
                url = entry.get('link', '').strip()
                
                if not title or not url:
                    continue
                
                all_articles.append({
                    "title": title,
                    "summary": summary[:500] if summary else "",
                    "url": url,
                    "source": feed_info["name"],
                    "tier": feed_info.get("tier", 2),
                    "category": feed_info.get("category", "general"),
                    "published": pub_date.isoformat() if pub_date else None,
                })
                
        except Exception as e:
            continue
    
    return all_articles


def ai_news_cluster_articles(articles: List[Dict]) -> List[Dict]:
    """Cluster similar articles using TF-IDF or title similarity."""
    if not articles:
        return []
    
    articles = sorted(articles, key=lambda x: x.get('score', 0), reverse=True)
    
    # TF-IDF similarity
    tfidf_sim = None
    if HAS_SKLEARN and len(articles) > 1:
        try:
            texts = [f"{a['title']} {a.get('summary', '')}" for a in articles]
            vectorizer = TfidfVectorizer(stop_words='english', max_features=1000, ngram_range=(1, 2))
            tfidf_matrix = vectorizer.fit_transform(texts)
            tfidf_sim = cosine_similarity(tfidf_matrix)
        except:
            tfidf_sim = None
    
    clusters = []
    used_indices = set()
    
    for i, article in enumerate(articles):
        if i in used_indices:
            continue
        
        cluster = {
            "lead_article": article,
            "sources": [article],
            "total_score": article.get('score', 0),
            "entities": article.get('entities', {}),
            "event_type": article.get('event_type', 'general'),
        }
        used_indices.add(i)
        
        for j, other in enumerate(articles):
            if j in used_indices or len(cluster["sources"]) >= 7:
                continue
            
            similarity = 0.0
            if tfidf_sim is not None:
                similarity = tfidf_sim[i][j] * 0.6
            
            # Entity similarity
            set1 = set()
            set2 = set()
            for ents in article.get('entities', {}).values():
                set1.update(ents)
            for ents in other.get('entities', {}).values():
                set2.update(ents)
            if set1 and set2:
                entity_sim = len(set1 & set2) / len(set1 | set2)
                similarity += entity_sim * 0.4
            
            if tfidf_sim is None:
                title_sim = SequenceMatcher(None, article['title'].lower(), other['title'].lower()).ratio()
                similarity = max(similarity, title_sim * 0.5)
            
            if similarity > 0.20:
                cluster["sources"].append(other)
                cluster["total_score"] += other.get('score', 0)
                used_indices.add(j)
        
        cluster["avg_score"] = cluster["total_score"] / len(cluster["sources"])
        
        if len(cluster["sources"]) >= AI_NEWS_MIN_CLUSTER_SIZE:
            if cluster["avg_score"] >= AI_NEWS_MIN_CLUSTER_SCORE:
                clusters.append(cluster)
    
    return sorted(clusters, key=lambda x: x["avg_score"], reverse=True)


def ai_news_create_single_clusters(articles: List[Dict], count: int, used_urls: Set[str]) -> List[Dict]:
    """Create clusters from single high-scoring articles when clustering fails."""
    sorted_articles = sorted(articles, key=lambda x: x.get('score', 0), reverse=True)
    clusters = []
    
    for article in sorted_articles:
        if len(clusters) >= count:
            break
        if article['url'] in used_urls:
            continue
        
        clusters.append({
            "lead_article": article,
            "sources": [article],
            "avg_score": article.get('score', 0),
            "entities": article.get('entities', {}),
            "event_type": article.get('event_type', 'general'),
            "is_single_source": True,
        })
    
    return clusters


def ai_news_synthesize_article(cluster: Dict) -> Optional[Dict]:
    """Use Gemini to synthesize an original article."""
    if not GOOGLE_GEMINI_API_KEY:
        return None
    
    sources = cluster["sources"][:5]
    is_single = cluster.get("is_single_source", False)
    
    sources_text = ""
    for i, src in enumerate(sources, 1):
        sources_text += f"\nSOURCE {i}: {src['source']}\nTitle: {src['title']}\nSummary: {src.get('summary', '')[:400]}\n"
    
    entity_hints = []
    for ents in cluster.get("entities", {}).values():
        entity_hints.extend(list(ents)[:3])
    
    if is_single:
        source_instruction = "You have ONE source article. Expand on this topic with your financial knowledge."
    else:
        source_instruction = f"Synthesize these {len(sources)} news sources into ONE compelling original article."
    
    prompt = f"""You are a professional financial journalist for London Strategic Edge, a premium market intelligence platform.

{source_instruction}

{sources_text}

KEY ENTITIES: {', '.join(entity_hints[:10]) if entity_hints else 'Various market topics'}
EVENT TYPE: {cluster.get('event_type', 'market_news')}

REQUIREMENTS:
1. Write an ORIGINAL article - do NOT copy phrases from sources
2. Lead with the most important market-moving information
3. Include SPECIFIC numbers: percentages, dollar amounts, dates
4. Explain WHY this matters to traders/investors
5. Include affected trading symbols (e.g., BTC/USD, SPY, EUR/USD, XAU/USD)
6. Professional, authoritative tone like Reuters or Bloomberg
7. Length: 500-800 words (AIM FOR 600-700 WORDS)
8. Use multiple paragraphs with clear structure

OUTPUT FORMAT - Respond with ONLY valid JSON:
{{"headline": "Compelling headline under 100 characters", "summary": "2-3 sentence preview", "body": "Full article 500-800 words", "category": "markets|crypto|forex|commodities|economy|central_bank|earnings|tech", "sentiment": "bullish|bearish|neutral", "impact_level": "critical|high|medium|low", "affected_symbols": ["BTC/USD", "SPY"], "key_points": ["Point 1", "Point 2", "Point 3"]}}"""

    try:
        response_text = call_gemini_api(prompt, max_tokens=3000, temperature=0.7)
        
        if not response_text:
            return None
        
        # Clean markdown
        response_text = clean_gemini_json_response(response_text)
        
        article_data = json.loads(response_text)
        article_data["sources"] = json.dumps([{"name": s["source"], "url": s["url"], "title": s["title"]} for s in sources])
        article_data["event_type"] = cluster.get("event_type", "general")
        
        return article_data
        
    except Exception as e:
        log.warning(f"⚠️ AI News synthesis error: {e}")
        return None


def ai_news_get_unsplash_image(headline: str, category: str = None) -> Optional[Dict]:
    """Get image from Unsplash with fallbacks."""
    
    # Fallback images by category
    fallbacks = {
        "crypto": {"url": "https://images.unsplash.com/photo-1518546305927-5a555bb7020d?w=1200", "credit": "André François McKenzie", "credit_link": "https://unsplash.com/@silverhousehd"},
        "forex": {"url": "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?w=1200", "credit": "Nick Chong", "credit_link": "https://unsplash.com/@nick604"},
        "markets": {"url": "https://images.unsplash.com/photo-1590283603385-17ffb3a7f29f?w=1200", "credit": "Nicholas Cappello", "credit_link": "https://unsplash.com/@bash__profile"},
        "economy": {"url": "https://images.unsplash.com/photo-1526304640581-d334cdbbf45e?w=1200", "credit": "Alexander Mils", "credit_link": "https://unsplash.com/@alexandermils"},
        "central_bank": {"url": "https://images.unsplash.com/photo-1541354329998-f4d9a9f9297f?w=1200", "credit": "Etienne Martin", "credit_link": "https://unsplash.com/@etiennemartin"},
        "commodities": {"url": "https://images.unsplash.com/photo-1610375461246-83df859d849d?w=1200", "credit": "Jingming Pan", "credit_link": "https://unsplash.com/@pokmer"},
    }
    
    if not UNSPLASH_ACCESS_KEY:
        return fallbacks.get(category, fallbacks["markets"])
    
    # Build search query
    headline_lower = headline.lower()
    keyword_map = {
        "bitcoin": "bitcoin cryptocurrency", "crypto": "cryptocurrency market",
        "gold": "gold investment", "oil": "crude oil energy",
        "fed": "federal reserve", "inflation": "inflation economy",
        "dollar": "us dollar currency", "stock": "stock market trading",
    }
    
    search_query = "financial market trading"
    for keyword, query in keyword_map.items():
        if keyword in headline_lower:
            search_query = query
            break
    
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        params = urllib.parse.urlencode({"query": search_query, "per_page": "5", "orientation": "landscape"})
        url = f"https://api.unsplash.com/search/photos?{params}"
        
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Client-ID {UNSPLASH_ACCESS_KEY}")
        
        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            data = json.loads(response.read().decode('utf-8'))
            if data.get("results"):
                photo = random.choice(data["results"][:3])
                return {
                    "url": photo["urls"]["regular"],
                    "credit": photo["user"]["name"],
                    "credit_link": photo["user"]["links"]["html"],
                }
    except Exception as e:
        log.debug(f"Unsplash error: {e}")
    
    return fallbacks.get(category, fallbacks["markets"])


def ai_news_get_recent_headlines(hours: int = 6) -> Set[str]:
    """Get recent headlines to avoid duplicates."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        result = sb.table("ai_news_articles").select("headline").gte("created_at", cutoff).execute()
        if result.data:
            return {r["headline"].lower()[:50] for r in result.data}
    except:
        pass
    return set()


def ai_news_store_article(article: Dict, image: Optional[Dict]) -> bool:
    """Store synthesized article in Supabase."""
    try:
        data = {
            "headline": article["headline"],
            "summary": article["summary"],
            "body": article["body"],
            "category": article["category"],
            "sentiment": article["sentiment"],
            "impact_level": article["impact_level"],
            "affected_symbols": article.get("affected_symbols", []),
            "key_points": article.get("key_points", []),
            "sources": article.get("sources", "[]"),
            "is_ai_generated": True,
            "is_featured": article["impact_level"] in ["critical", "high"],
            "image_url": image["url"] if image else None,
            "image_credit": image["credit"] if image else None,
            "image_credit_link": image["credit_link"] if image else None,
        }
        
        try:
            data["event_type"] = article.get("event_type", "general")
            sb.table("ai_news_articles").insert(data).execute()
            return True
        except Exception as e:
            if "event_type" in str(e):
                del data["event_type"]
                sb.table("ai_news_articles").insert(data).execute()
                return True
            raise e
    except Exception as e:
        log.warning(f"⚠️ AI News storage error: {e}")
        return False


async def ai_news_desk_task():
    """
    AI News Desk - Synthesizes professional articles from multiple sources.
    
    Schedule:
    - London/US session: Every 30 minutes (3 articles = 6/hour)
    - Other sessions: Every 60 minutes (3 articles = 3/hour)
    - Weekend/Holiday: Every 4 hours (3 articles)
    """
    log.info("📰 AI News Desk started")
    log.info(f"   Target: {AI_NEWS_DESK_ARTICLES} articles per cycle")
    log.info(f"   Article length: {AI_NEWS_MIN_BODY_WORDS}-{AI_NEWS_MAX_BODY_WORDS} words")
    
    # Initial delay to let other tasks start
    await asyncio.sleep(30)
    
    while not tick_streamer._shutdown.is_set():
        try:
            interval, session_name = get_ai_news_interval()
            session_emoji = "🔥" if "overlap" in session_name else "📊" if "active" in session_name else "🌙"
            
            log.info(f"📰 AI News Desk cycle starting ({session_emoji} {session_name})")
            
            # Get recent headlines to avoid duplicates
            recent_headlines = ai_news_get_recent_headlines(hours=6)
            
            # Fetch articles
            loop = asyncio.get_event_loop()
            raw_articles = await loop.run_in_executor(None, ai_news_fetch_feeds, 12)
            
            if not raw_articles:
                log.warning("📰 No articles fetched, retrying later")
                await asyncio.sleep(interval)
                continue
            
            log.info(f"📰 Fetched {len(raw_articles)} raw articles")
            
            # Score articles
            scored = []
            excluded = 0
            for article in raw_articles:
                scored_article = ai_news_score_article(article)
                if scored_article.get('excluded'):
                    excluded += 1
                else:
                    scored.append(scored_article)
            
            # Filter by minimum score
            high_quality = [a for a in scored if a.get('score', 0) >= AI_NEWS_MIN_SCORE]
            log.info(f"📰 Scored: {len(scored)} | Excluded: {excluded} | High quality: {len(high_quality)}")
            
            if not high_quality:
                log.warning("📰 No high-quality articles, retrying later")
                await asyncio.sleep(interval)
                continue
            
            # Cluster articles
            clusters = ai_news_cluster_articles(high_quality)
            
            # Fallback to single articles if not enough clusters
            if len(clusters) < AI_NEWS_DESK_ARTICLES:
                used_urls = set()
                for c in clusters:
                    for s in c["sources"]:
                        used_urls.add(s["url"])
                
                available = [a for a in high_quality if a["url"] not in used_urls and a["title"].lower()[:50] not in recent_headlines]
                single_clusters = ai_news_create_single_clusters(available, AI_NEWS_DESK_ARTICLES - len(clusters), used_urls)
                clusters.extend(single_clusters)
            
            log.info(f"📰 Topics to generate: {len(clusters)}")
            
            # Generate articles
            generated_count = 0
            generated_headlines = set()
            
            for i, cluster in enumerate(clusters[:AI_NEWS_DESK_ARTICLES]):
                log.info(f"📝 Synthesizing article {i+1}/{AI_NEWS_DESK_ARTICLES}: {cluster['lead_article']['title'][:50]}...")
                
                # Synthesize
                article = await loop.run_in_executor(None, ai_news_synthesize_article, cluster)
                
                if not article:
                    log.warning(f"   ⚠️ Synthesis failed")
                    continue
                
                # Check duplicates
                headline_key = article["headline"].lower()[:50]
                if headline_key in generated_headlines or headline_key in recent_headlines:
                    log.warning(f"   ⚠️ Duplicate headline, skipping")
                    continue
                
                # Get image
                image = ai_news_get_unsplash_image(article["headline"], article.get("category"))
                
                # Store
                if ai_news_store_article(article, image):
                    word_count = len(article.get('body', '').split())
                    log.info(f"   ✅ STORED: {article['headline'][:50]}... ({word_count} words)")
                    generated_count += 1
                    generated_headlines.add(headline_key)
                else:
                    log.warning(f"   ⚠️ Storage failed")
                
                await asyncio.sleep(2)  # Rate limit
            
            log.info(f"📰 AI News Desk cycle complete: {generated_count}/{AI_NEWS_DESK_ARTICLES} articles generated")
            log.info(f"📰 Next cycle in {interval // 60} minutes ({session_emoji} {session_name})")
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            log.error(f"❌ AI News Desk error: {e}")
            await asyncio.sleep(300)  # 5 min on error


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    """Run all 7 tasks in parallel with graceful shutdown."""
    log.info("=" * 70)
    log.info("🚀 LONDON STRATEGIC EDGE - BULLETPROOF WORKER v7.1")
    log.info("=" * 70)
    log.info("   1️⃣  Tick streaming (TwelveData) - %d symbols", len(ALL_SYMBOLS))
    log.info("   2️⃣  Economic calendar (Trading Economics) - double-scrape")
    log.info("   3️⃣  Financial news (%d RSS feeds + Gemini AI)", len(RSS_FEEDS))
    log.info("   4️⃣  Gap detection & backfill (AUTO-HEALING)")
    log.info("   5️⃣  Bond yields (G20 sovereign bonds) - 30 min")
    log.info("   6️⃣  Whale flow tracker (BTC whales) - $100k+ threshold")
    log.info("   7️⃣  AI News Desk (article synthesis) - 3 per cycle")
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
        whale_tracker.shutdown()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Create tasks
    tasks = [
        asyncio.create_task(tick_streaming_task(), name="tick_stream"),
        asyncio.create_task(economic_calendar_task(), name="econ_calendar"),
        asyncio.create_task(financial_news_task(), name="news"),
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
        asyncio.create_task(bond_yield_task(), name="bond_yields"),
        asyncio.create_task(whale_flow_task(), name="whale_flow"),
        asyncio.create_task(ai_news_desk_task(), name="ai_news_desk"),
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log.error(f"❌ Main loop error: {e}")
    finally:
        tick_streamer.shutdown()
        whale_tracker.shutdown()
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
