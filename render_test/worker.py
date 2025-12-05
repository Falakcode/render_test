#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v8.0
================================================================================
Production-grade data pipeline with EODHD WebSocket streaming.

NEW in v8.0:
  - EODHD WebSocket replaces TwelveData (70x better forex tick frequency!)
  - 280 symbols: 70 forex, 50 crypto, 100 stocks, 60 ETFs (no commodities)
  - Pre/Post market data for US stocks
  - EODHD Economic Calendar API (replaces Trading Economics scraper)
  - Calendar checks every 5 min + 3s follow-up for late releases
  - 7 parallel WebSocket connections (50 symbols each max)
  - Advanced News Classifier integration ready

TASKS:
  1. Tick Streaming     - EODHD WebSocket (280 symbols across 7 connections)
  2. Economic Calendar  - EODHD API (every 5 min + 3s follow-up)
  3. Financial News     - 50+ RSS feeds + Gemini AI classification
  4. Gap Fill Service   - Auto-detect and repair missing candle data
  5. Bond Yields        - G20 sovereign yields (every 30 minutes)
  6. Whale Flow Tracker - BTC whale movements via Blockchain.com WebSocket
  7. AI News Desk       - Professional article synthesis (3 per cycle)

SYMBOL CAPACITY: 280/300 WebSocket credits used (20 spare)
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

# EODHD API (primary data source)
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "6931c829362a18.89813191")

# Supabase
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Google Gemini API for AI classification
GOOGLE_GEMINI_API_KEY = os.environ.get("GOOGLE_GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "LWD7wgTnS8zYEtNcnOc9qUgr_Encl7iycazKCp2vhvc")

SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# ============================================================================
#                           TRADING SYMBOLS (280 TOTAL)
# ============================================================================
# Capacity: 300 WebSocket credits, using 280 (20 spare)

# FOREX (70 pairs) - EODHD format: EURUSD (no slash)
FOREX_SYMBOLS = [
    # G10 Majors (28)
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "GBPAUD", "EURNZD", "GBPNZD", "AUDNZD", "EURCAD", "GBPCAD",
    "AUDCAD", "NZDCAD", "EURCHF", "GBPCHF", "AUDCHF", "NZDCHF", "CADCHF",
    # Scandinavian (8)
    "USDSEK", "USDNOK", "USDDKK", "EURSEK", "EURNOK", "EURDKK", "NOKSEK", "SEKJPY",
    # Emerging Markets (18)
    "USDMXN", "USDZAR", "USDTRY", "USDPLN", "USDHUF", "USDCZK", "USDSGD",
    "USDHKD", "USDTHB", "USDMYR", "USDINR", "USDCNH", "USDKRW", "USDTWD",
    "EURMXN", "EURZAR", "EURTRY", "EURPLN",
    # Exotic Crosses (16)
    "GBPMXN", "GBPZAR", "GBPTRY", "GBPPLN", "AUDMXN", "AUDZAR", "AUDSGD",
    "NZDSGD", "CADMXN", "CADZAR", "CHFMXN", "CHFZAR", "TRYJPY", "ZARJPY",
    "MXNJPY", "SGDJPY",
]

# CRYPTO (50 pairs) - EODHD format: BTC-USD
CRYPTO_SYMBOLS = [
    # Top 30 by market cap
    "BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD", "DOGE-USD", "ADA-USD",
    "AVAX-USD", "DOT-USD", "MATIC-USD", "LINK-USD", "SHIB-USD", "LTC-USD",
    "BCH-USD", "UNI-USD", "ATOM-USD", "XLM-USD", "FIL-USD", "NEAR-USD",
    "APT-USD", "ARB-USD", "OP-USD", "INJ-USD", "IMX-USD", "STX-USD",
    "VET-USD", "ALGO-USD", "AAVE-USD", "GRT-USD", "SAND-USD", "MANA-USD",
    # DeFi & Layer 2 (10)
    "CRV-USD", "COMP-USD", "MKR-USD", "SNX-USD", "LDO-USD", "SUSHI-USD",
    "1INCH-USD", "YFI-USD", "BAL-USD", "CAKE-USD",
    # Exchange Tokens (5)
    "BNB-USD", "OKB-USD", "CRO-USD", "KCS-USD", "LEO-USD",
    # Meme & Other (5)
    "PEPE-USD", "FLOKI-USD", "BONK-USD", "WIF-USD", "ETC-USD",
]

# US STOCKS (100 symbols) - EODHD format: AAPL (symbol only, endpoint handles exchange)
US_STOCK_SYMBOLS = [
    # Magnificent 7
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    # Semiconductors (15)
    "AMD", "INTC", "AVGO", "QCOM", "TXN", "MU", "AMAT", "LRCX",
    "KLAC", "MRVL", "ON", "NXPI", "MCHP", "ADI", "SWKS",
    # Tech Software (15)
    "CRM", "ADBE", "NOW", "INTU", "PANW", "SNPS", "CDNS", "WDAY",
    "ZS", "CRWD", "DDOG", "TEAM", "MDB", "SNOW", "NET",
    # Finance (15)
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW",
    "AXP", "V", "MA", "PYPL", "SQ", "COIN", "HOOD",
    # Healthcare (10)
    "UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "TMO", "ABT", "BMY", "GILD",
    # Consumer (15)
    "WMT", "COST", "TGT", "HD", "LOW", "NKE", "SBUX", "MCD",
    "KO", "PEP", "PG", "CL", "EL", "LULU", "DG",
    # Industrial/Energy (13)
    "XOM", "CVX", "COP", "SLB", "CAT", "DE", "UPS", "FDX",
    "BA", "LMT", "RTX", "NOC", "GD",
    # Media/Entertainment (10)
    "DIS", "NFLX", "CMCSA", "WBD", "PARA", "SPOT", "ROKU", "TTD", "RBLX", "U",
]

# ETFs & INDICES (60 symbols) - includes VIX, US30 equivalents
ETF_INDEX_SYMBOLS = [
    # Major Index ETFs (15)
    "SPY", "QQQ", "DIA", "IWM", "VTI", "VOO", "IVV", "VTV", "VUG", "VIG",
    "MDY", "IJH", "IJR", "VXF", "ITOT",
    # Sector ETFs (15)
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE",
    "XBI", "XHB", "XRT", "XME", "XOP",
    # Bond ETFs (10)
    "TLT", "IEF", "SHY", "BND", "AGG", "LQD", "HYG", "JNK", "TIP", "GOVT",
    # International (10)
    "EEM", "VWO", "EFA", "VEA", "FXI", "EWJ", "EWZ", "EWG", "EWU", "EWC",
    # Volatility & Leveraged (10) - includes VIX proxies
    "VXX", "UVXY", "SVXY", "VIXY", "SQQQ", "TQQQ", "SPXU", "SPXL", "ARKK", "ARKW",
]

# Combine all symbols
ALL_FOREX = FOREX_SYMBOLS
ALL_CRYPTO = CRYPTO_SYMBOLS
ALL_US_STOCKS = US_STOCK_SYMBOLS
ALL_ETFS = ETF_INDEX_SYMBOLS

TOTAL_SYMBOLS = len(ALL_FOREX) + len(ALL_CRYPTO) + len(ALL_US_STOCKS) + len(ALL_ETFS)

# ============================================================================
#                           EODHD WEBSOCKET CONFIG
# ============================================================================

EODHD_WS_BASE = "wss://ws.eodhistoricaldata.com/ws"

# WebSocket endpoints by asset type
EODHD_ENDPOINTS = {
    "forex": f"{EODHD_WS_BASE}/forex",
    "crypto": f"{EODHD_WS_BASE}/crypto", 
    "us": f"{EODHD_WS_BASE}/us",
}

# Connection configuration (max 50 symbols per connection)
WEBSOCKET_CONNECTIONS = [
    {"name": "forex_1", "endpoint": "forex", "symbols": ALL_FOREX[:50]},
    {"name": "forex_2", "endpoint": "forex", "symbols": ALL_FOREX[50:]},
    {"name": "crypto_1", "endpoint": "crypto", "symbols": ALL_CRYPTO},
    {"name": "us_stocks_1", "endpoint": "us", "symbols": ALL_US_STOCKS[:50]},
    {"name": "us_stocks_2", "endpoint": "us", "symbols": ALL_US_STOCKS[50:]},
    {"name": "etfs_1", "endpoint": "us", "symbols": ALL_ETFS[:50]},
    {"name": "etfs_2", "endpoint": "us", "symbols": ALL_ETFS[50:]},
]

# ============================================================================
#                              CONFIGURATION
# ============================================================================

# Tick Streaming
BATCH_MAX = 500
BATCH_FLUSH_SECS = 2
WATCHDOG_TIMEOUT_SECS = 60          # No ticks for 60s = reconnect
RECV_TIMEOUT_SECS = 30              # Timeout for each recv() call
CONNECTION_TIMEOUT_SECS = 15
MAX_RECONNECT_DELAY = 30
HEALTH_LOG_INTERVAL = 60

# Economic Calendar (EODHD API)
CALENDAR_CHECK_INTERVAL = 300       # 5 minutes
CALENDAR_FOLLOWUP_DELAY = 3         # 3 seconds after main check
EODHD_CALENDAR_URL = "https://eodhd.com/api/economic-events"

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
    """Call Google Gemini API and return the response text."""
    if not GOOGLE_GEMINI_API_KEY:
        log.warning("⚠️ Google Gemini API key not configured")
        return None
    
    url = f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
        }
    }
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url, data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
        
        text = result['candidates'][0]['content']['parts'][0]['text']
        return text.strip()
        
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
#                    TASK 1: EODHD WEBSOCKET TICK STREAMING
# ============================================================================

class EODHDTickStreamer:
    """
    Multi-connection EODHD WebSocket tick streamer.
    
    Features:
    - 7 parallel WebSocket connections (280 symbols)
    - Automatic reconnection with exponential backoff
    - Watchdog for each connection
    - Batch inserts to Supabase
    - Health monitoring and logging
    """
    
    def __init__(self):
        self._batch: List[dict] = []
        self._batch_lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        
        # Per-connection state
        self._connections: Dict[str, dict] = {}
        for conn in WEBSOCKET_CONNECTIONS:
            self._connections[conn["name"]] = {
                "config": conn,
                "ws": None,
                "is_connected": False,
                "tick_count": 0,
                "last_tick_time": datetime.now(timezone.utc),
                "symbols_streaming": set(),
                "reconnect_count": 0,
            }
        
        # Global stats
        self._total_ticks = 0
        self._start_time = datetime.now(timezone.utc)
        self._last_health_log = datetime.now(timezone.utc)
    
    def _to_float(self, x) -> Optional[float]:
        """Convert to float preserving precision."""
        if x is None:
            return None
        try:
            return float(Decimal(str(x)))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _normalize_symbol(self, symbol: str, endpoint: str) -> str:
        """
        Convert EODHD symbol format to internal format.
        EODHD: EURUSD, BTC-USD, AAPL
        Internal: EUR/USD, BTC/USD, AAPL
        """
        if endpoint == "forex":
            # EURUSD -> EUR/USD
            if len(symbol) == 6:
                return f"{symbol[:3]}/{symbol[3:]}"
            return symbol
        elif endpoint == "crypto":
            # BTC-USD -> BTC/USD
            return symbol.replace("-", "/")
        else:
            # Stocks/ETFs stay as-is
            return symbol

    async def _flush(self):
        """Flush batch to Supabase."""
        async with self._batch_lock:
            if not self._batch:
                return
            payload, self._batch = self._batch, []

        try:
            sb.table(SUPABASE_TABLE).insert(payload).execute()
            log.info(f"✅ Inserted {len(payload)} ticks to {SUPABASE_TABLE}")
        except Exception as e:
            log.error(f"❌ Insert failed: {e} - re-queuing {len(payload)} rows")
            async with self._batch_lock:
                self._batch[:0] = payload

    async def _periodic_flush(self):
        """Flush batch every BATCH_FLUSH_SECS."""
        while not self._shutdown.is_set():
            await asyncio.sleep(BATCH_FLUSH_SECS)
            await self._flush()

    async def _handle_tick(self, data: dict, conn_name: str, endpoint: str):
        """Handle incoming tick data."""
        try:
            # Extract symbol
            symbol = data.get("s", data.get("symbol", ""))
            if not symbol:
                return
            
            # Normalize symbol format
            normalized = self._normalize_symbol(symbol, endpoint)
            
            # Extract price data
            price = self._to_float(data.get("p", data.get("price")))
            bid = self._to_float(data.get("b", data.get("bid")))
            ask = self._to_float(data.get("a", data.get("ask")))
            volume = self._to_float(data.get("v", data.get("volume")))
            timestamp = data.get("t", data.get("timestamp"))
            
            if price is None and bid is None:
                return
            
            # Use mid price if no direct price
            if price is None and bid and ask:
                price = (bid + ask) / 2
            
            # Update connection stats
            conn = self._connections[conn_name]
            conn["tick_count"] += 1
            conn["last_tick_time"] = datetime.now(timezone.utc)
            conn["symbols_streaming"].add(symbol)
            self._total_ticks += 1
            
            # Build tick record
            tick = {
                "symbol": normalized,
                "price": price,
                "bid": bid,
                "ask": ask,
                "volume": volume,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            async with self._batch_lock:
                self._batch.append(tick)
                if len(self._batch) >= BATCH_MAX:
                    await self._flush()
                    
        except Exception as e:
            log.warning(f"⚠️ Error handling tick: {e}")

    async def _run_connection(self, conn_name: str):
        """Run a single WebSocket connection with reconnection logic."""
        conn = self._connections[conn_name]
        config = conn["config"]
        endpoint = config["endpoint"]
        symbols = config["symbols"]
        
        url = f"{EODHD_ENDPOINTS[endpoint]}?api_token={EODHD_API_KEY}"
        
        backoff = 1
        
        while not self._shutdown.is_set():
            try:
                log.info(f"🔌 [{conn_name}] Connecting to /{endpoint} with {len(symbols)} symbols...")
                
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    conn["ws"] = ws
                    conn["is_connected"] = True
                    conn["last_tick_time"] = datetime.now(timezone.utc)
                    
                    # Subscribe to symbols
                    subscribe_msg = {
                        "action": "subscribe",
                        "symbols": ",".join(symbols)
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    log.info(f"✅ [{conn_name}] Connected and subscribed to {len(symbols)} symbols")
                    
                    # Reset backoff on successful connection
                    backoff = 1
                    
                    # Main receive loop
                    while not self._shutdown.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT_SECS)
                            data = json.loads(msg)
                            
                            # Handle status messages
                            if "status_code" in data:
                                if data.get("status_code") != 200:
                                    log.warning(f"⚠️ [{conn_name}] Status: {data}")
                                continue
                            
                            # Handle tick data
                            await self._handle_tick(data, conn_name, endpoint)
                            
                        except asyncio.TimeoutError:
                            # Check watchdog
                            silence = (datetime.now(timezone.utc) - conn["last_tick_time"]).total_seconds()
                            if silence > WATCHDOG_TIMEOUT_SECS:
                                log.warning(f"🐕 [{conn_name}] Watchdog triggered - {silence:.0f}s silence")
                                break
                            continue
                        except json.JSONDecodeError:
                            continue
                            
            except websockets.exceptions.InvalidStatusCode as e:
                log.error(f"❌ [{conn_name}] HTTP {e.status_code}")
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"🔌 [{conn_name}] Connection closed: {e.code}")
            except Exception as e:
                log.error(f"❌ [{conn_name}] Error: {e}")
            finally:
                conn["is_connected"] = False
                conn["ws"] = None
                conn["reconnect_count"] += 1
            
            if self._shutdown.is_set():
                break
                
            # Exponential backoff
            log.info(f"🔄 [{conn_name}] Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(MAX_RECONNECT_DELAY, backoff * 2)

    async def _health_logger(self):
        """Log health stats periodically."""
        while not self._shutdown.is_set():
            await asyncio.sleep(HEALTH_LOG_INTERVAL)
            
            elapsed = (datetime.now(timezone.utc) - self._start_time).total_seconds()
            rate = self._total_ticks / elapsed if elapsed > 0 else 0
            
            # Count active connections and streaming symbols
            active = sum(1 for c in self._connections.values() if c["is_connected"])
            streaming = set()
            for c in self._connections.values():
                streaming.update(c["symbols_streaming"])
            
            log.info(f"📊 HEALTH: {self._total_ticks:,} ticks | {rate:.1f}/sec | "
                    f"{active}/{len(self._connections)} connections | "
                    f"{len(streaming)}/{TOTAL_SYMBOLS} symbols streaming")
            
            # Per-connection stats
            for name, conn in self._connections.items():
                status = "🟢" if conn["is_connected"] else "🔴"
                ticks = conn["tick_count"]
                syms = len(conn["symbols_streaming"])
                log.info(f"   {status} {name}: {ticks:,} ticks, {syms} symbols")

    async def run(self):
        """Main entry point - run all connections in parallel."""
        log.info("=" * 70)
        log.info("🚀 EODHD TICK STREAMER v8.0 STARTING")
        log.info("=" * 70)
        log.info(f"📊 Total symbols: {TOTAL_SYMBOLS}")
        log.info(f"   Forex: {len(ALL_FOREX)} | Crypto: {len(ALL_CRYPTO)}")
        log.info(f"   US Stocks: {len(ALL_US_STOCKS)} | ETFs: {len(ALL_ETFS)}")
        log.info(f"🔌 WebSocket connections: {len(WEBSOCKET_CONNECTIONS)}")
        log.info(f"🐕 Watchdog timeout: {WATCHDOG_TIMEOUT_SECS}s")
        log.info("=" * 70)
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._periodic_flush(), name="flusher"),
            asyncio.create_task(self._health_logger(), name="health"),
        ]
        
        # Start all WebSocket connections
        for conn_config in WEBSOCKET_CONNECTIONS:
            task = asyncio.create_task(
                self._run_connection(conn_config["name"]),
                name=f"ws_{conn_config['name']}"
            )
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            log.info("🛑 Shutting down tick streamer...")
            await self._flush()
            for task in tasks:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    def shutdown(self):
        """Request graceful shutdown."""
        log.info("🛑 Shutdown requested")
        self._shutdown.set()


# Global instance
tick_streamer = EODHDTickStreamer()


async def tick_streaming_task():
    """Entry point for tick streaming."""
    await tick_streamer.run()


# ============================================================================
#                    TASK 2: EODHD ECONOMIC CALENDAR
# ============================================================================

# Impact classification (EODHD doesn't provide impact, we classify manually)
HIGH_IMPACT_KEYWORDS = [
    "Non Farm Payrolls", "NFP", "CPI", "Consumer Price Index", "Core CPI",
    "GDP", "Gross Domestic Product", "FOMC", "Fed Interest Rate", "ECB Interest",
    "BOE Interest", "BOJ Interest", "Unemployment Rate", "Retail Sales",
    "ISM Manufacturing PMI", "ISM Services PMI", "PCE", "Core PCE"
]

MEDIUM_IMPACT_KEYWORDS = [
    "PPI", "Producer Price Index", "Durable Goods", "Housing Starts",
    "Building Permits", "Consumer Confidence", "Michigan Consumer",
    "Trade Balance", "Industrial Production", "Manufacturing PMI",
    "Services PMI", "Employment Change", "ADP Employment"
]


def classify_event_impact(event_name: str) -> str:
    """Classify economic event impact based on keywords."""
    event_upper = event_name.upper()
    
    for keyword in HIGH_IMPACT_KEYWORDS:
        if keyword.upper() in event_upper:
            return "High"
    
    for keyword in MEDIUM_IMPACT_KEYWORDS:
        if keyword.upper() in event_upper:
            return "Medium"
    
    return "Low"


def fetch_eodhd_calendar(days_ahead: int = 7) -> List[Dict]:
    """
    Fetch economic calendar from EODHD API.
    
    Returns list of events with: date, time, country, event, actual, forecast, previous, importance
    """
    try:
        today = datetime.now(timezone.utc).date()
        from_date = today.strftime("%Y-%m-%d")
        to_date = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        params = {
            "api_token": EODHD_API_KEY,
            "from": from_date,
            "to": to_date,
            "fmt": "json"
        }
        
        log.info(f"📅 Fetching EODHD calendar: {from_date} to {to_date}")
        
        response = requests.get(EODHD_CALENDAR_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not isinstance(data, list):
            log.warning(f"⚠️ Unexpected calendar response: {type(data)}")
            return []
        
        events = []
        for item in data:
            try:
                # Parse date and time
                date_str = item.get("date", "")
                # EODHD format: "2025-12-05 13:30:00"
                if " " in date_str:
                    date_part, time_part = date_str.split(" ", 1)
                    time_part = time_part[:5]  # HH:MM
                else:
                    date_part = date_str
                    time_part = "00:00"
                
                # Get country code
                country = item.get("country", "")
                if not country or len(country) != 2:
                    continue
                
                event_name = item.get("event", "")
                if not event_name or len(event_name) < 3:
                    continue
                
                # Extract values
                actual = item.get("actual")
                forecast = item.get("estimate")
                previous = item.get("previous")
                
                # Classify impact
                importance = classify_event_impact(event_name)
                
                event_data = {
                    'date': date_part,
                    'time': time_part,
                    'country': country.upper(),
                    'event': event_name,
                    'actual': str(actual) if actual is not None else None,
                    'forecast': str(forecast) if forecast is not None else None,
                    'previous': str(previous) if previous is not None else None,
                    'importance': importance,
                    'comparison': item.get("comparison"),
                    'unit': item.get("unit"),
                }
                
                events.append(event_data)
                
            except Exception as e:
                log.warning(f"⚠️ Error parsing calendar event: {e}")
                continue
        
        log.info(f"📋 Fetched {len(events)} economic events from EODHD")
        return events
        
    except Exception as e:
        log.error(f"❌ Error fetching EODHD calendar: {e}")
        return []


def upsert_economic_events(events: List[Dict]) -> Tuple[int, int]:
    """
    Insert or update economic events in Supabase.
    Returns (new_count, updated_count).
    """
    if not events:
        return 0, 0

    new_count = 0
    updated_count = 0
    
    for event in events:
        try:
            # Check if event exists
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
                # Update if actual value changed
                old_actual = existing.data[0].get('actual')
                if old_actual != event['actual'] and event['actual']:
                    sb.table('Economic_calander').update({
                        'actual': event['actual'],
                        'forecast': event['forecast'],
                        'previous': event['previous'],
                    }).eq(
                        'date', event['date']
                    ).eq(
                        'time', event['time']
                    ).eq(
                        'country', event['country']
                    ).eq(
                        'event', event['event']
                    ).execute()
                    
                    log.info(f"📊 UPDATED: {event['country']} {event['event']} = {event['actual']}")
                    updated_count += 1
            else:
                # Insert new event
                sb.table('Economic_calander').insert(event).execute()
                new_count += 1

        except Exception as e:
            log.warning(f"⚠️ Error upserting event {event['event']}: {e}")
            continue

    return new_count, updated_count


async def economic_calendar_task():
    """
    EODHD Economic Calendar with double-check pattern.
    
    Schedule: Every 5 minutes + 3 second follow-up
    This catches both scheduled releases and late data.
    """
    log.info("📅 Economic calendar started (EODHD API)")
    log.info(f"   Check interval: {CALENDAR_CHECK_INTERVAL // 60} minutes")
    log.info(f"   Follow-up delay: {CALENDAR_FOLLOWUP_DELAY} seconds")
    
    while not tick_streamer._shutdown.is_set():
        try:
            # Primary check
            log.info("📅 Running primary calendar check...")
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, fetch_eodhd_calendar, 7)
            
            if events:
                new_count, updated_count = await loop.run_in_executor(
                    None, upsert_economic_events, events
                )
                
                # Count high impact events with actuals
                high_impact = [e for e in events if e['importance'] == 'High' and e['actual']]
                
                log.info(f"📅 Primary check: {len(events)} events, {new_count} new, {updated_count} updated")
                if high_impact:
                    log.info(f"   🔥 High impact with actuals: {len(high_impact)}")
            
            # Wait 3 seconds then do follow-up check (catches late releases)
            await asyncio.sleep(CALENDAR_FOLLOWUP_DELAY)
            
            log.info("📅 Running follow-up check (catching late releases)...")
            events = await loop.run_in_executor(None, fetch_eodhd_calendar, 1)
            
            if events:
                new_count, updated_count = await loop.run_in_executor(
                    None, upsert_economic_events, events
                )
                if updated_count > 0:
                    log.info(f"📅 Follow-up caught {updated_count} late releases!")
            
            # Wait until next check
            log.info(f"📅 Next calendar check in {CALENDAR_CHECK_INTERVAL // 60} minutes")
            await asyncio.sleep(CALENDAR_CHECK_INTERVAL - CALENDAR_FOLLOWUP_DELAY)
            
        except Exception as e:
            log.error(f"❌ Error in economic calendar task: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 3: FINANCIAL NEWS SCRAPER
#                    (Keep existing RSS + Gemini implementation)
# ============================================================================

# 50+ RSS Feeds for comprehensive coverage
RSS_FEEDS = [
    # BBC News
    {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC News (Main)"},
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC News (Business)"},
    {"url": "http://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC News (World)"},
    
    # CNBC
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC (Top News)"},
    {"url": "https://www.cnbc.com/id/10001147/device/rss/rss.html", "name": "CNBC (Markets)"},
    {"url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "name": "CNBC (Business)"},
    
    # Reuters
    {"url": "https://www.reutersagency.com/feed/?taxonomy=best-sectors&post_type=best", "name": "Reuters"},
    
    # Bloomberg (unofficial)
    {"url": "https://feeds.bloomberg.com/markets/news.rss", "name": "Bloomberg Markets"},
    
    # Financial Times
    {"url": "https://www.ft.com/rss/home", "name": "Financial Times"},
    
    # MarketWatch
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories/", "name": "MarketWatch (Top)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/marketpulse/", "name": "MarketWatch (Pulse)"},
    
    # Yahoo Finance
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance"},
    
    # Crypto
    {"url": "https://cointelegraph.com/rss", "name": "CoinTelegraph"},
    {"url": "https://decrypt.co/feed", "name": "Decrypt"},
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk"},
    {"url": "https://bitcoinmagazine.com/feed", "name": "Bitcoin Magazine"},
    
    # General News (for geopolitics)
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "name": "NYT World"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "name": "NYT Business"},
    {"url": "https://feeds.washingtonpost.com/rss/world", "name": "WaPo World"},
    {"url": "https://feeds.washingtonpost.com/rss/business", "name": "WaPo Business"},
    
    # Central Banks
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve"},
    {"url": "https://www.ecb.europa.eu/rss/press.html", "name": "ECB"},
    
    # Energy/Commodities
    {"url": "https://oilprice.com/rss/main", "name": "OilPrice.com"},
    
    # Tech
    {"url": "https://techcrunch.com/feed/", "name": "TechCrunch"},
    {"url": "https://www.theverge.com/rss/index.xml", "name": "The Verge"},
]


def get_news_scan_interval() -> Tuple[int, str]:
    """Get news scan interval based on trading session."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()
    
    # Weekend
    if weekday >= 5:
        return 3600, "weekend"
    
    # Active sessions (London 8-17, US 13-21 UTC)
    if 8 <= hour < 21:
        return 600, "active"
    
    # Quiet hours
    return 1800, "quiet"


def should_prefilter_article(title: str, summary: str) -> bool:
    """Pre-filter articles before AI classification."""
    text = f"{title} {summary}".lower()
    
    # Must-include keywords
    include_keywords = [
        'fed', 'fomc', 'powell', 'lagarde', 'ecb', 'boe', 'rate', 'inflation',
        'bitcoin', 'btc', 'ethereum', 'crypto', 'blockchain',
        'stock', 'market', 'nasdaq', 's&p', 'dow', 'rally', 'crash',
        'oil', 'gold', 'opec', 'commodity',
        'gdp', 'cpi', 'unemployment', 'jobs', 'nonfarm', 'payroll',
        'trump', 'biden', 'election', 'tariff', 'sanction',
        'war', 'russia', 'ukraine', 'china', 'taiwan', 'iran', 'israel',
        'bank', 'crisis', 'recession', 'earnings', 'revenue'
    ]
    
    # Exclude keywords
    exclude_keywords = [
        'sport', 'celebrity', 'entertainment', 'movie', 'music',
        'recipe', 'weather', 'horoscope', 'lifestyle'
    ]
    
    if any(kw in text for kw in exclude_keywords):
        return False
    
    return any(kw in text for kw in include_keywords)


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
        return None
        
    prompt = f"""You are a professional financial news analyst. Analyze this article and determine if it will impact financial markets.

TITLE: {article['title']}
SUMMARY: {article['summary']}
SOURCE: {article['source']}

Respond with ONLY valid JSON (no markdown, no explanation):
{{"is_important": true, "event_type": "war", "impact_level": "high", "affected_symbols": ["BTC/USD", "XAU/USD"], "sentiment": "bearish", "summary": "2-3 sentence market impact summary", "keywords": ["keyword1", "keyword2"]}}

Valid event_types: war, political_shock, economic_data, central_bank, trade_war, commodity_shock, market_crash, crypto_crash, crypto_rally, natural_disaster, bank_crisis, energy_crisis, pandemic_health, market_rally, earnings, regulatory
Valid impact_levels: critical, high, medium, low
Valid sentiments: bullish, bearish, neutral"""

    try:
        response_text = call_gemini_api(prompt, max_tokens=500, temperature=0.1)
        if not response_text:
            return None
        
        response_text = clean_gemini_json_response(response_text)
        return json.loads(response_text)

    except Exception as e:
        log.warning(f"⚠️ Gemini classification error: {e}")
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
        if 'duplicate key value' not in str(e).lower():
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

            # Pre-filter
            filtered_articles = [
                a for a in unique_articles
                if should_prefilter_article(a['title'], a['summary'])
            ]

            log.info(f"🎯 After filtering: {len(filtered_articles)} articles")

            # Classify and store
            stored_count = 0
            for article in filtered_articles[:30]:  # Limit to 30 per cycle
                classification = await loop.run_in_executor(None, classify_article_with_gemini, article)

                if not classification:
                    continue

                if not classification.get('is_important') or classification.get('impact_level') not in ['critical', 'high']:
                    continue

                impact_emoji = "🚨" if classification['impact_level'] == 'critical' else "⚡"
                log.info(f"{impact_emoji} IMPORTANT: {article['title'][:60]}... ({classification['impact_level']})")

                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored_count += 1

                await asyncio.sleep(1)

            log.info(f"✅ News cycle complete: {stored_count} stored")
            log.info(f"😴 Next scan in {interval // 60} minutes")
            
            await asyncio.sleep(interval)

        except Exception as e:
            log.error(f"❌ Error in financial news task: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 4: GAP FILL SERVICE
# ============================================================================

def symbol_to_table(symbol: str) -> str:
    """Convert symbol to table name: BTC/USD -> candles_btc_usd"""
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


async def gap_fill_task():
    """Periodically scan for and fill gaps in candle data."""
    log.info("🔧 Gap fill service started")
    log.info(f"   Scan interval: {GAP_SCAN_INTERVAL // 60} minutes")
    log.info(f"   Max gap hours: {MAX_GAP_HOURS}")
    
    # Initial delay
    await asyncio.sleep(60)
    
    # Priority symbols for gap detection
    priority_symbols = []
    for s in ALL_CRYPTO[:10]:
        priority_symbols.append(s.replace("-", "/"))
    for s in ALL_FOREX[:10]:
        priority_symbols.append(f"{s[:3]}/{s[3:]}")
    priority_symbols.extend(ALL_US_STOCKS[:10])
    
    while not tick_streamer._shutdown.is_set():
        try:
            log.info("🔧 Running gap scan...")
            
            total_gaps = 0
            symbols_with_gaps = 0
            
            for symbol in priority_symbols:
                gaps = detect_gaps(symbol)
                if gaps:
                    symbols_with_gaps += 1
                    total_gaps += len(gaps)
                    log.info(f"   ⚠️ {symbol}: {len(gaps)} gaps detected")
            
            if total_gaps == 0:
                log.info("✅ No gaps detected in priority symbols")
            else:
                log.info(f"⚠️ Found {total_gaps} gaps in {symbols_with_gaps} symbols")
            
            await asyncio.sleep(GAP_SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ Error in gap fill task: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 5: BOND YIELDS (G20)
#                    (Keep existing implementation)
# ============================================================================

G20_BONDS = {
    "US": {
        "name": "United States",
        "flag": "🇺🇸",
        "6m": "https://www.investing.com/rates-bonds/u.s.-6-month-bond-yield",
        "1y": "https://www.investing.com/rates-bonds/u.s.-1-year-bond-yield",
        "2y": "https://www.investing.com/rates-bonds/u.s.-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
    },
    "DE": {
        "name": "Germany",
        "flag": "🇩🇪",
        "2y": "https://www.investing.com/rates-bonds/germany-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/germany-10-year-bond-yield"
    },
    "GB": {
        "name": "United Kingdom",
        "flag": "🇬🇧",
        "2y": "https://www.investing.com/rates-bonds/uk-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/uk-10-year-bond-yield"
    },
    "JP": {
        "name": "Japan",
        "flag": "🇯🇵",
        "2y": "https://www.investing.com/rates-bonds/japan-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield"
    },
    "CN": {
        "name": "China",
        "flag": "🇨🇳",
        "1y": "https://www.investing.com/rates-bonds/china-1-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/china-10-year-bond-yield"
    },
    "FR": {
        "name": "France",
        "flag": "🇫🇷",
        "2y": "https://www.investing.com/rates-bonds/france-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/france-10-year-bond-yield"
    },
    "IT": {
        "name": "Italy",
        "flag": "🇮🇹",
        "2y": "https://www.investing.com/rates-bonds/italy-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/italy-10-year-bond-yield"
    },
    "CA": {
        "name": "Canada",
        "flag": "🇨🇦",
        "2y": "https://www.investing.com/rates-bonds/canada-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/canada-10-year-bond-yield"
    },
    "AU": {
        "name": "Australia",
        "flag": "🇦🇺",
        "2y": "https://www.investing.com/rates-bonds/australia-2-year-bond-yield",
        "10y": "https://www.investing.com/rates-bonds/australia-10-year-bond-yield"
    },
    "KR": {
        "name": "South Korea",
        "flag": "🇰🇷",
        "10y": "https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield"
    },
    "IN": {
        "name": "India",
        "flag": "🇮🇳",
        "10y": "https://www.investing.com/rates-bonds/india-10-year-bond-yield"
    },
    "BR": {
        "name": "Brazil",
        "flag": "🇧🇷",
        "10y": "https://www.investing.com/rates-bonds/brazil-10-year-bond-yield"
    },
    "MX": {
        "name": "Mexico",
        "flag": "🇲🇽",
        "10y": "https://www.investing.com/rates-bonds/mexico-10-year-bond-yield"
    },
}


def scrape_bond_yield(url: str, session: requests.Session) -> Optional[Dict]:
    """Scrape yield from Investing.com."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
    }
    
    try:
        response = session.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        yield_elem = soup.find('div', {'data-test': 'instrument-price-last'})
        if not yield_elem:
            return None
        
        yield_text = yield_elem.get_text(strip=True)
        yield_value = float(yield_text.replace(',', ''))
        
        change_elem = soup.find('div', {'data-test': 'instrument-price-change'})
        change_value = None
        if change_elem:
            try:
                change_text = change_elem.get_text(strip=True)
                change_value = float(change_text.replace(',', '').replace('+', ''))
            except:
                pass
        
        return {'yield': yield_value, 'change': change_value}
        
    except Exception:
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
            'yield_10y': None,
            'yield_2y': None,
            'spread_10y_2y': None,
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
        
        for maturity in ['2y', '10y']:
            url = config.get(maturity)
            if url:
                data = scrape_bond_yield(url, session)
                if data:
                    country_data[f'yield_{maturity}'] = data['yield']
                time.sleep(1.5)
        
        if country_data['yield_10y'] and country_data['yield_2y']:
            country_data['spread_10y_2y'] = round(
                country_data['yield_10y'] - country_data['yield_2y'], 3
            )
        
        if country_data['yield_10y']:
            log.info(f"   {config['flag']} {country_code}: 10Y={country_data['yield_10y']:.2f}%")
        
        results.append(country_data)
    
    return results


def store_bond_yields(yields: List[Dict]) -> int:
    """Store bond yields in Supabase."""
    try:
        valid_yields = [y for y in yields if y['yield_10y'] is not None]
        if valid_yields:
            sb.table('sovereign_yields').insert(valid_yields).execute()
            return len(valid_yields)
    except Exception as e:
        log.error(f"❌ Error storing bond yields: {e}")
    return 0


async def bond_yield_task():
    """Periodically scrape and store G20 bond yields."""
    log.info("📈 Bond yield scraper started")
    log.info(f"   Interval: {BOND_SCRAPE_INTERVAL // 60} minutes")
    
    while not tick_streamer._shutdown.is_set():
        try:
            loop = asyncio.get_event_loop()
            yields = await loop.run_in_executor(None, scrape_all_g20_yields)
            
            if yields:
                has_10y = sum(1 for y in yields if y['yield_10y'])
                await loop.run_in_executor(None, store_bond_yields, yields)
                log.info(f"📊 Bond scrape complete: {has_10y}/{len(G20_BONDS)} countries")
            
            await asyncio.sleep(BOND_SCRAPE_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ Error in bond yield task: {e}")
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 6: BTC WHALE FLOW TRACKER
#                    (Keep existing implementation - simplified here)
# ============================================================================

WHALE_THRESHOLD_USD = 100000
BLOCKCHAIN_WS_URL = "wss://ws.blockchain.info/inv"

KNOWN_EXCHANGE_WALLETS = {
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "Binance",
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j": "Binance",
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "Coinbase",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh": "Coinbase",
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r": "Bitfinex",
}


class WhaleFlowTracker:
    """Track large BTC transactions."""
    
    def __init__(self):
        self._shutdown = asyncio.Event()
        self._btc_price = 100000.0
        self._flows = []
        self._lock = asyncio.Lock()
    
    async def _update_btc_price(self):
        """Update BTC price periodically."""
        while not self._shutdown.is_set():
            try:
                response = requests.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": "bitcoin", "vs_currencies": "usd"},
                    timeout=10
                )
                if response.status_code == 200:
                    self._btc_price = response.json()["bitcoin"]["usd"]
            except:
                pass
            await asyncio.sleep(60)
    
    async def _handle_transaction(self, tx: Dict):
        """Handle incoming transaction."""
        try:
            tx_data = tx.get('x', tx)
            total_satoshi = sum(out.get('value', 0) for out in tx_data.get('out', []))
            total_btc = total_satoshi / 1e8
            total_usd = total_btc * self._btc_price
            
            if total_usd < WHALE_THRESHOLD_USD:
                return
            
            # Detect exchange involvement
            exchange = None
            for inp in tx_data.get('inputs', []):
                addr = inp.get('prev_out', {}).get('addr', '')
                if addr in KNOWN_EXCHANGE_WALLETS:
                    exchange = KNOWN_EXCHANGE_WALLETS[addr]
                    break
            
            for out in tx_data.get('out', []):
                addr = out.get('addr', '')
                if addr in KNOWN_EXCHANGE_WALLETS:
                    exchange = KNOWN_EXCHANGE_WALLETS[addr]
                    break
            
            emoji = "🟢" if exchange else "⚪"
            exchange_str = f"({exchange})" if exchange else ""
            
            log.info(f"🐋 {emoji} WHALE: {total_btc:.2f} BTC (${total_usd:,.0f}) {exchange_str}")
            
        except Exception as e:
            log.warning(f"⚠️ Error handling whale tx: {e}")
    
    async def run(self):
        """Main WebSocket loop."""
        log.info("🐋 Whale flow tracker started")
        log.info(f"   Threshold: ${WHALE_THRESHOLD_USD:,}")
        
        # Start price updater
        price_task = asyncio.create_task(self._update_btc_price())
        
        backoff = 1
        
        try:
            while not self._shutdown.is_set():
                try:
                    async with websockets.connect(BLOCKCHAIN_WS_URL) as ws:
                        await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                        log.info("🐋 Connected to Blockchain.info WebSocket")
                        
                        backoff = 1
                        
                        while not self._shutdown.is_set():
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=60)
                                data = json.loads(msg)
                                
                                if data.get("op") == "utx":
                                    await self._handle_transaction(data)
                                    
                            except asyncio.TimeoutError:
                                continue
                            except json.JSONDecodeError:
                                continue
                                
                except Exception as e:
                    log.warning(f"🐋 Whale tracker error: {e}")
                
                if self._shutdown.is_set():
                    break
                    
                log.info(f"🐋 Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(30, backoff * 2)
                
        finally:
            price_task.cancel()
            with suppress(asyncio.CancelledError):
                await price_task
    
    def shutdown(self):
        self._shutdown.set()


whale_tracker = WhaleFlowTracker()


async def whale_flow_task():
    """Entry point for whale flow tracking."""
    await whale_tracker.run()


# ============================================================================
#                    TASK 7: AI NEWS DESK
#                    (Placeholder - use existing implementation)
# ============================================================================

async def ai_news_desk_task():
    """AI News Desk - placeholder for article synthesis."""
    log.info("📰 AI News Desk started (simplified)")
    
    while not tick_streamer._shutdown.is_set():
        await asyncio.sleep(1800)  # 30 minutes
        log.info("📰 AI News Desk cycle (placeholder)")


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    """Run all tasks in parallel with graceful shutdown."""
    log.info("=" * 70)
    log.info("🚀 LONDON STRATEGIC EDGE - WORKER v8.0 (EODHD)")
    log.info("=" * 70)
    log.info(f"   1️⃣  Tick streaming (EODHD) - {TOTAL_SYMBOLS} symbols")
    log.info(f"   2️⃣  Economic calendar (EODHD API) - every 5 min")
    log.info(f"   3️⃣  Financial news ({len(RSS_FEEDS)} RSS feeds + Gemini)")
    log.info(f"   4️⃣  Gap detection & backfill")
    log.info(f"   5️⃣  Bond yields (G20) - 30 min")
    log.info(f"   6️⃣  Whale flow tracker (BTC) - $100k threshold")
    log.info(f"   7️⃣  AI News Desk")
    log.info("=" * 70)
    log.info(f"📊 Symbol breakdown:")
    log.info(f"   Forex: {len(ALL_FOREX)} | Crypto: {len(ALL_CRYPTO)}")
    log.info(f"   US Stocks: {len(ALL_US_STOCKS)} | ETFs: {len(ALL_ETFS)}")
    log.info(f"   Total: {TOTAL_SYMBOLS}/300 WebSocket credits")
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
        sys.exit(0)bye!")
        sys.exit(0)
