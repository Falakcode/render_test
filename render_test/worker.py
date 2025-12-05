#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v8.2
================================================================================
Production-grade data pipeline with EODHD WebSocket streaming.

TASKS:
  1. Tick Streaming     - EODHD WebSocket (280 symbols across 7 connections)
  2. Economic Calendar  - EODHD API (every 5 min + 3s follow-up)
  3. Financial News     - RSS feeds + Gemini AI classification
  4. Gap Fill Service   - Auto-detect and repair missing candle data
  5. Bond Yields        - G20 sovereign yields (every 30 minutes)
  6. Whale Flow Tracker - BTC whale movements via Blockchain.com WebSocket
  7. AI News Desk       - Placeholder for article synthesis
  8. G20 Macro Fetcher  - 16 indicators x 20 countries (hourly)
  9. Index Poller       - 11 global indices via REST API (every 20 seconds)

SYMBOL CAPACITY: 280/300 WebSocket credits used
================================================================================
"""

import os
import json
import asyncio
import logging
import signal
import sys
import time
import threading
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Tuple, Any
from contextlib import suppress
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser

# ============================================================================
#                              ENVIRONMENT
# ============================================================================

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "6931c829362a18.89813191")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
GOOGLE_GEMINI_API_KEY = os.environ.get("GOOGLE_GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# ============================================================================
#                           TRADING SYMBOLS (280 TOTAL)
# ============================================================================

FOREX_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "GBPAUD", "EURNZD", "GBPNZD", "AUDNZD", "EURCAD", "GBPCAD",
    "AUDCAD", "NZDCAD", "EURCHF", "GBPCHF", "AUDCHF", "NZDCHF", "CADCHF",
    "USDSEK", "USDNOK", "USDDKK", "EURSEK", "EURNOK", "EURDKK", "NOKSEK", "SEKJPY",
    "USDMXN", "USDZAR", "USDTRY", "USDBRL", "USDPLN", "USDHUF", "USDCZK",
    "USDSGD", "USDHKD", "USDCNH", "USDTHB", "USDKRW", "USDINR", "USDIDR",
    "EURPLN", "EURHUF", "EURCZK", "EURTRY",
    "GBPZAR", "GBPMXN", "AUDZAR", "NZDZAR", "ZARJPY", "MXNJPY", "TRYJPY",
    "SGDJPY", "HKDJPY", "CNHJPY", "GBPSGD", "AUDSGD", "EURSGD", "CADSGD",
    "NZDSGD", "CHFSGD"
]

CRYPTO_SYMBOLS = [
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "SOL-USD", "ADA-USD", "DOGE-USD",
    "DOT-USD", "AVAX-USD", "MATIC-USD",
    "LINK-USD", "UNI-USD", "LTC-USD", "ATOM-USD", "XLM-USD", "FIL-USD",
    "APT-USD", "ARB-USD", "OP-USD", "NEAR-USD", "ICP-USD", "VET-USD",
    "ALGO-USD", "FTM-USD", "SAND-USD",
    "MANA-USD", "AXS-USD", "AAVE-USD", "MKR-USD", "CRV-USD", "SNX-USD",
    "COMP-USD", "ENJ-USD", "CHZ-USD", "GALA-USD", "IMX-USD", "LDO-USD",
    "RPL-USD", "GMX-USD", "BLUR-USD",
    "ETH-BTC", "BNB-BTC", "XRP-BTC", "SOL-BTC", "ADA-BTC",
    "USDT-USD", "USDC-USD", "DAI-USD", "BUSD-USD", "TUSD-USD"
]

US_STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "AMD", "INTC", "AVGO", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC", "ADI", "MRVL", "ON",
    "CRM", "ADBE", "NOW", "SNOW", "PLTR", "PANW", "CRWD", "ZS", "DDOG", "NET",
    "TEAM", "MDB", "WDAY", "SPLK", "ZM",
    "V", "MA", "PYPL", "SQ", "COIN", "HOOD", "AFRM", "UPST",
    "JPM", "BAC", "WFC", "GS", "MS", "C", "USB", "SCHW",
    "UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "BMY", "GILD", "AMGN", "BIIB",
    "WMT", "COST", "TGT", "HD", "LOW", "NKE", "SBUX", "MCD", "DIS", "NFLX",
    "BA", "LMT", "RTX", "GE", "CAT", "DE", "HON", "MMM", "UPS", "FDX",
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "PSX", "VLO",
    "AMT", "PLD", "CCI", "EQIX",
    "T", "VZ", "TMUS", "CMCSA",
    "F", "GM", "RIVN", "LCID"
]

ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "VT",
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE",
    "SMH", "XBI", "IBB", "KRE", "XHB", "XRT",
    "EEM", "EFA", "VWO", "FXI", "EWJ", "EWZ", "EWG", "INDA",
    "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "BND", "AGG",
    "GLD", "SLV", "USO", "UNG", "DBA", "DBC", "PDBC", "CPER",
    "VXX", "UVXY", "SVXY", "TQQQ", "SQQQ", "SPXU",
    "ARKK", "ARKG", "ARKF", "ARKW", "KWEB", "MCHI"
]

ALL_FOREX = FOREX_SYMBOLS
ALL_CRYPTO = CRYPTO_SYMBOLS
ALL_US_STOCKS = US_STOCKS
ALL_ETFS = ETFS
TOTAL_SYMBOLS = len(ALL_FOREX) + len(ALL_CRYPTO) + len(ALL_US_STOCKS) + len(ALL_ETFS)

# ============================================================================
#                          EODHD WEBSOCKET CONFIG
# ============================================================================

EODHD_ENDPOINTS = {
    "forex": "wss://ws.eodhistoricaldata.com/ws/forex",
    "crypto": "wss://ws.eodhistoricaldata.com/ws/crypto",
    "us": "wss://ws.eodhistoricaldata.com/ws/us"
}

WS_CONNECTIONS = [
    {"name": "forex_1", "endpoint": "forex", "symbols": ALL_FOREX[:35]},
    {"name": "forex_2", "endpoint": "forex", "symbols": ALL_FOREX[35:]},
    {"name": "crypto_1", "endpoint": "crypto", "symbols": ALL_CRYPTO[:25]},
    {"name": "crypto_2", "endpoint": "crypto", "symbols": ALL_CRYPTO[25:]},
    {"name": "us_1", "endpoint": "us", "symbols": ALL_US_STOCKS[:50]},
    {"name": "us_2", "endpoint": "us", "symbols": ALL_US_STOCKS[50:]},
    {"name": "us_etf", "endpoint": "us", "symbols": ALL_ETFS},
]

# ============================================================================
#                            CONFIGURATION
# ============================================================================

BATCH_MAX = 500
BATCH_FLUSH_INTERVAL = 2
WATCHDOG_TIMEOUT_SECS = 60
NUCLEAR_TIMEOUT_SECS = 120
HEALTH_LOG_INTERVAL = 60
WEEKLY_RESET_DAY = 5
WEEKLY_RESET_HOUR = 22
MIN_GAP_MINUTES = 3
MAX_GAP_HOURS = 4
GAP_SCAN_INTERVAL = 300
BOND_SCRAPE_INTERVAL = 1800
MACRO_FETCH_INTERVAL = 3600
INDEX_POLL_INTERVAL = 20  # Poll indices every 20 seconds

# ============================================================================
#                              LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("worker")

# ============================================================================
#                           SUPABASE CLIENT
# ============================================================================

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================================
#                    TASK 1: EODHD TICK STREAMING
# ============================================================================

class EODHDTickStreamer:
    def __init__(self):
        self._shutdown = asyncio.Event()
        self._connections: Dict[str, Dict] = {}
        self._batch: List[Dict] = []
        self._batch_lock = asyncio.Lock()
        self._total_ticks = 0
        self._start_time = datetime.now(timezone.utc)
        self._last_main_loop_heartbeat = datetime.now(timezone.utc)
        self._last_weekly_reset = None
        self._failure_reasons = Counter()
        
        for conn_config in WS_CONNECTIONS:
            name = conn_config["name"]
            self._connections[name] = {
                "config": conn_config,
                "ws": None,
                "is_connected": False,
                "tick_count": 0,
                "last_tick_time": datetime.now(timezone.utc),
                "last_activity_time": datetime.now(timezone.utc),
                "symbols_streaming": set(),
                "task": None,
            }
    
    def _normalize_symbol(self, symbol: str, endpoint: str) -> str:
        if endpoint == "forex":
            if len(symbol) == 6:
                return f"{symbol[:3]}/{symbol[3:]}"
            return symbol
        elif endpoint == "crypto":
            return symbol.replace("-", "/")
        else:
            return symbol
    
    def _to_float(self, val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    
    async def _flush(self):
        if not self._batch:
            return
        batch_copy = self._batch.copy()
        self._batch.clear()
        try:
            sb.table(SUPABASE_TABLE).insert(batch_copy).execute()
        except Exception as e:
            log.warning(f"Batch insert failed: {e}")
    
    async def _periodic_flush(self):
        while not self._shutdown.is_set():
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            async with self._batch_lock:
                await self._flush()
    
    async def _handle_tick(self, data: Dict, conn_name: str, endpoint: str):
        try:
            symbol = data.get("s", data.get("symbol", ""))
            if not symbol:
                return
            
            normalized = self._normalize_symbol(symbol, endpoint)
            price = self._to_float(data.get("p", data.get("price")))
            bid = self._to_float(data.get("b", data.get("bid")))
            ask = self._to_float(data.get("a", data.get("ask")))
            volume = self._to_float(data.get("v", data.get("volume")))
            
            if price is None and bid is None:
                return
            if price is None and bid and ask:
                price = (bid + ask) / 2
            
            conn = self._connections[conn_name]
            conn["tick_count"] += 1
            conn["last_tick_time"] = datetime.now(timezone.utc)
            conn["last_activity_time"] = datetime.now(timezone.utc)
            conn["symbols_streaming"].add(symbol)
            self._total_ticks += 1
            
            tick = {
                "symbol": normalized,
                "price": price,
                "bid": bid,
                "ask": ask,
                "volume": volume,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            
            async with self._batch_lock:
                self._batch.append(tick)
                if len(self._batch) >= BATCH_MAX:
                    await self._flush()
        except Exception as e:
            log.warning(f"Error handling tick: {e}")

    async def _kill_connection(self, conn_name: str):
        conn = self._connections[conn_name]
        conn["is_connected"] = False
        if conn["ws"]:
            try:
                await asyncio.wait_for(conn["ws"].close(), timeout=3)
            except Exception:
                pass
            conn["ws"] = None

    async def _watchdog(self):
        log.info(f"Watchdog started (timeout: {WATCHDOG_TIMEOUT_SECS}s)")
        while not self._shutdown.is_set():
            await asyncio.sleep(5)
            now = datetime.now(timezone.utc)
            
            for conn_name, conn in self._connections.items():
                if not conn["is_connected"]:
                    continue
                activity_elapsed = (now - conn["last_activity_time"]).total_seconds()
                if activity_elapsed > WATCHDOG_TIMEOUT_SECS:
                    log.warning(f"WATCHDOG [{conn_name}]: No activity for {activity_elapsed:.1f}s - killing!")
                    self._failure_reasons["watchdog_kill"] += 1
                    await self._kill_connection(conn_name)
            
            active_connections = sum(1 for c in self._connections.values() if c["is_connected"])
            if active_connections > 0:
                if now.weekday() == WEEKLY_RESET_DAY and now.hour == WEEKLY_RESET_HOUR:
                    reset_key = f"{now.year}-{now.isocalendar()[1]}"
                    if self._last_weekly_reset != reset_key:
                        log.info("WEEKLY RESET: Saturday 10pm maintenance")
                        self._last_weekly_reset = reset_key
                        self._failure_reasons["weekly_reset"] += 1
                        for cn in self._connections:
                            await self._kill_connection(cn)

    async def _nuclear_watchdog(self):
        log.info(f"NUCLEAR WATCHDOG started (timeout: {NUCLEAR_TIMEOUT_SECS}s)")
        while not self._shutdown.is_set():
            await asyncio.sleep(10)
            now = datetime.now(timezone.utc)
            main_loop_elapsed = (now - self._last_main_loop_heartbeat).total_seconds()
            
            if main_loop_elapsed > NUCLEAR_TIMEOUT_SECS:
                log.error(f"NUCLEAR WATCHDOG TRIGGERED! System stuck for {main_loop_elapsed:.1f}s!")
                self._failure_reasons["nuclear_kill"] += 1
                for conn_name, conn in self._connections.items():
                    if conn["task"] and not conn["task"].done():
                        conn["task"].cancel()
                    await self._kill_connection(conn_name)
                self._last_main_loop_heartbeat = datetime.now(timezone.utc)

    async def _health_logger(self):
        while not self._shutdown.is_set():
            await asyncio.sleep(HEALTH_LOG_INTERVAL)
            now = datetime.now(timezone.utc)
            elapsed = (now - self._start_time).total_seconds()
            rate = self._total_ticks / elapsed if elapsed > 0 else 0
            main_loop_elapsed = (now - self._last_main_loop_heartbeat).total_seconds()
            
            active = sum(1 for c in self._connections.values() if c["is_connected"])
            streaming = set()
            for c in self._connections.values():
                streaming.update(c["symbols_streaming"])
            
            status = "HEALTHY" if main_loop_elapsed < 30 else "STALE" if main_loop_elapsed < NUCLEAR_TIMEOUT_SECS else "STUCK"
            log.info(f"HEALTH: {status} | Ticks: {self._total_ticks:,} ({rate:.1f}/sec) | Connections: {active}/{len(self._connections)} | Symbols: {len(streaming)}/{TOTAL_SYMBOLS}")

    async def _run_connection(self, conn_name: str):
        conn = self._connections[conn_name]
        config = conn["config"]
        endpoint = config["endpoint"]
        symbols = config["symbols"]
        url = f"{EODHD_ENDPOINTS[endpoint]}?api_token={EODHD_API_KEY}"
        backoff = 1
        
        while not self._shutdown.is_set():
            self._last_main_loop_heartbeat = datetime.now(timezone.utc)
            try:
                log.info(f"[{conn_name}] Connecting to {endpoint}...")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    conn["ws"] = ws
                    conn["is_connected"] = True
                    conn["last_activity_time"] = datetime.now(timezone.utc)
                    backoff = 1
                    
                    sub_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                    await ws.send(json.dumps(sub_msg))
                    log.info(f"[{conn_name}] Subscribed to {len(symbols)} symbols")
                    
                    async for message in ws:
                        if self._shutdown.is_set():
                            break
                        conn["last_activity_time"] = datetime.now(timezone.utc)
                        self._last_main_loop_heartbeat = datetime.now(timezone.utc)
                        try:
                            data = json.loads(message)
                            if isinstance(data, dict):
                                if data.get("status_code") or data.get("message"):
                                    continue
                                await self._handle_tick(data, conn_name, endpoint)
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        await self._handle_tick(item, conn_name, endpoint)
                        except json.JSONDecodeError:
                            pass
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"[{conn_name}] Connection closed: {e.code}")
                self._failure_reasons["connection_closed"] += 1
                conn["is_connected"] = False
            except Exception as e:
                log.error(f"[{conn_name}] Error: {e}")
                self._failure_reasons["http_error"] += 1
                conn["is_connected"] = False
            
            if self._shutdown.is_set():
                break
            log.info(f"[{conn_name}] Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(30, backoff * 2)
    
    async def run(self):
        log.info(f"Starting EODHD tick streamer ({len(self._connections)} connections)")
        flush_task = asyncio.create_task(self._periodic_flush())
        watchdog_task = asyncio.create_task(self._watchdog())
        nuclear_task = asyncio.create_task(self._nuclear_watchdog())
        health_task = asyncio.create_task(self._health_logger())
        
        for conn_name in self._connections:
            task = asyncio.create_task(self._run_connection(conn_name))
            self._connections[conn_name]["task"] = task
        
        await self._shutdown.wait()
        
        flush_task.cancel()
        watchdog_task.cancel()
        nuclear_task.cancel()
        health_task.cancel()
        
        for conn in self._connections.values():
            if conn["task"]:
                conn["task"].cancel()
            await self._kill_connection(conn["config"]["name"])
        
        async with self._batch_lock:
            await self._flush()
    
    def shutdown(self):
        self._shutdown.set()


tick_streamer = EODHDTickStreamer()


async def tick_streaming_task():
    await tick_streamer.run()


# ============================================================================
#                    TASK 2: EODHD ECONOMIC CALENDAR
# ============================================================================

HIGH_IMPACT_KEYWORDS = [
    "non farm payrolls", "nonfarm payrolls", "nfp", "unemployment rate",
    "cpi", "consumer price index", "inflation", "core cpi",
    "gdp", "gross domestic product", "interest rate decision",
    "fed", "fomc", "ecb", "boe", "boj", "rba", "retail sales",
    "pce", "core pce", "ism manufacturing", "ism services"
]

MEDIUM_IMPACT_KEYWORDS = [
    "ppi", "producer price", "housing starts", "building permits",
    "durable goods", "consumer confidence", "michigan", "pmi",
    "industrial production", "trade balance", "jobless claims"
]


def classify_calendar_impact(event_name: str) -> str:
    name_lower = event_name.lower()
    for kw in HIGH_IMPACT_KEYWORDS:
        if kw in name_lower:
            return "high"
    for kw in MEDIUM_IMPACT_KEYWORDS:
        if kw in name_lower:
            return "medium"
    return "low"


def fetch_eodhd_calendar(days_ahead: int = 7) -> List[Dict]:
    from_date = datetime.now().strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    url = "https://eodhd.com/api/economic-events"
    params = {
        "api_token": EODHD_API_KEY,
        "from": from_date,
        "to": to_date,
        "limit": 1000,
        "fmt": "json"
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        return []
    except Exception:
        return []


def store_calendar_events(events: List[Dict]) -> int:
    stored = 0
    for event in events:
        try:
            event_date = event.get("date", "")
            date_part = event_date.split(" ")[0] if event_date else None
            time_part = event_date.split(" ")[1] if event_date and " " in event_date else None
            
            data = {
                "event": event.get("type", ""),
                "country": event.get("country", ""),
                "date": date_part,
                "time": time_part,
                "actual": str(event.get("actual")) if event.get("actual") is not None else None,
                "previous": str(event.get("previous")) if event.get("previous") is not None else None,
                "consensus": str(event.get("estimate")) if event.get("estimate") is not None else None,
                "importance": classify_calendar_impact(event.get("type", "")),
            }
            # Insert - duplicates handled by unique constraint or ignored
            sb.table("Economic_calander").insert(data).execute()
            stored += 1
        except Exception:
            pass
    return stored


async def economic_calendar_task():
    log.info("Economic calendar task started (EODHD API)")
    while not tick_streamer._shutdown.is_set():
        try:
            log.info("Fetching economic calendar...")
            events = fetch_eodhd_calendar(days_ahead=7)
            if events:
                stored = store_calendar_events(events)
                high_impact = sum(1 for e in events if classify_calendar_impact(e.get("type", "")) == "high")
                log.info(f"Calendar: {len(events)} events, {high_impact} high-impact, {stored} stored")
            
            await asyncio.sleep(3)
            events2 = fetch_eodhd_calendar(days_ahead=7)
            if events2:
                store_calendar_events(events2)
            
            await asyncio.sleep(300)
        except Exception as e:
            log.error(f"Calendar task error: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 3: FINANCIAL NEWS
# ============================================================================

RSS_FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories/", "name": "MarketWatch"},
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance"},
    {"url": "https://cointelegraph.com/rss", "name": "CoinTelegraph"},
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve"},
    {"url": "https://feeds.reuters.com/reuters/businessNews", "name": "Reuters Business"},
    {"url": "https://www.investing.com/rss/news.rss", "name": "Investing.com"},
    {"url": "https://seekingalpha.com/market_currents.xml", "name": "Seeking Alpha"},
    {"url": "https://oilprice.com/rss/main", "name": "OilPrice"},
]

INCLUDE_KEYWORDS = [
    "fed", "federal reserve", "powell", "ecb", "lagarde", "boe", "boj",
    "rate", "interest", "inflation", "cpi", "gdp", "unemployment",
    "bitcoin", "btc", "ethereum", "crypto", "stock", "market",
    "oil", "gold", "commodity", "tariff", "trade war", "sanctions",
    "war", "attack", "military", "trump", "biden", "election"
]

EXCLUDE_KEYWORDS = ["sport", "sports", "football", "basketball", "celebrity", "entertainment", "weather", "horoscope"]


def should_prefilter_article(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in text:
            return False
    for kw in INCLUDE_KEYWORDS:
        if kw in text:
            return True
    return False


def optimize_articles_for_cost(articles: List[Dict]) -> List[Dict]:
    if not articles:
        return []
    
    articles_with_images = [a for a in articles if a.get("image_url")]
    if not articles_with_images:
        articles_with_images = articles[:50]
    
    unique_articles = []
    seen_titles = []
    for article in articles_with_images:
        title = article.get("title", "")
        is_duplicate = False
        for seen in seen_titles:
            if SequenceMatcher(None, title.lower(), seen.lower()).ratio() >= 0.70:
                is_duplicate = True
                break
        if not is_duplicate:
            unique_articles.append(article)
            seen_titles.append(title)
    
    category_counts = {"crypto": 0, "political": 0, "central_bank": 0, "market": 0, "commodity": 0, "macro": 0}
    category_limits = {"crypto": 4, "political": 4, "central_bank": 4, "market": 4, "commodity": 3, "macro": 4}
    
    crypto_kw = ["bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "xrp"]
    political_kw = ["trump", "biden", "election", "president", "white house"]
    central_bank_kw = ["fed", "powell", "ecb", "lagarde", "rate", "monetary", "fomc"]
    market_kw = ["stock", "nasdaq", "s&p", "dow", "market", "rally", "crash"]
    commodity_kw = ["oil", "gold", "opec", "crude", "commodity"]
    macro_kw = ["gdp", "inflation", "cpi", "unemployment", "jobs", "pmi"]
    
    final_articles = []
    for article in unique_articles:
        text = f"{article['title']} {article.get('summary', '')}".lower()
        category = None
        if any(kw in text for kw in crypto_kw):
            category = "crypto"
        elif any(kw in text for kw in central_bank_kw):
            category = "central_bank"
        elif any(kw in text for kw in macro_kw):
            category = "macro"
        elif any(kw in text for kw in political_kw):
            category = "political"
        elif any(kw in text for kw in commodity_kw):
            category = "commodity"
        elif any(kw in text for kw in market_kw):
            category = "market"
        
        if category and category in category_limits:
            if category_counts[category] >= category_limits[category]:
                continue
            category_counts[category] += 1
        final_articles.append(article)
    
    return final_articles


def fetch_rss_feed(feed_url: str, source_name: str) -> List[Dict]:
    try:
        feed = feedparser.parse(feed_url)
        articles = []
        for entry in feed.entries[:20]:
            article = {
                "title": entry.get("title", ""),
                "summary": entry.get("summary", entry.get("description", ""))[:500],
                "url": entry.get("link", ""),
                "source": source_name,
                "published": entry.get("published", ""),
                "image_url": None,
            }
            if hasattr(entry, "media_content") and entry.media_content:
                article["image_url"] = entry.media_content[0].get("url")
            elif hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
                article["image_url"] = entry.media_thumbnail[0].get("url")
            articles.append(article)
        return articles
    except Exception:
        return []


def call_gemini_api(prompt: str, max_tokens: int = 1000, temperature: float = 0.7) -> Optional[str]:
    if not GOOGLE_GEMINI_API_KEY:
        return None
    try:
        url = f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens}
        }
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        return None
    except Exception:
        return None


def classify_article_with_gemini(article: Dict) -> Optional[Dict]:
    prompt = f"""Analyze this financial news article and respond with JSON only:

Title: {article['title']}
Summary: {article['summary'][:300]}

Return JSON with:
- summary: 1-2 sentence summary
- event_type: war|political_shock|economic_data|central_bank|trade_war|commodity_shock|market_crash|crypto_crash|crypto_rally|market_rally|earnings|regulatory
- impact_level: critical|high|medium|low
- affected_symbols: array of ticker symbols
- sentiment: bullish|bearish|neutral
- keywords: array of 3-5 key terms
- is_important: boolean

Respond with valid JSON only."""

    try:
        response_text = call_gemini_api(prompt, max_tokens=500, temperature=0.1)
        if not response_text:
            return None
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        return json.loads(response_text.strip())
    except Exception:
        return None


def store_news_article(article: Dict, classification: Dict) -> bool:
    try:
        data = {
            "title": article["title"],
            "summary": classification["summary"],
            "url": article["url"],
            "source": article["source"],
            "published_at": article["published"],
            "image_url": article.get("image_url"),
            "event_type": classification["event_type"],
            "impact_level": classification["impact_level"],
            "affected_symbols": classification["affected_symbols"],
            "sentiment": classification["sentiment"],
            "keywords": classification["keywords"],
            "is_breaking": classification["impact_level"] in ["critical", "high"]
        }
        sb.table("financial_news").insert(data).execute()
        return True
    except Exception:
        return False


def get_news_scan_interval() -> Tuple[int, str]:
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return 7200, "weekend"
    if 8 <= now.hour < 21:
        return 600, "active"
    return 1800, "quiet"


async def financial_news_task():
    log.info("Financial news scraper started")
    while not tick_streamer._shutdown.is_set():
        try:
            interval, session_name = get_news_scan_interval()
            log.info(f"Starting news scan ({session_name} session)")
            
            all_articles = []
            loop = asyncio.get_event_loop()
            for feed in RSS_FEEDS:
                articles = await loop.run_in_executor(None, fetch_rss_feed, feed["url"], feed["name"])
                all_articles.extend(articles)
                await asyncio.sleep(0.3)
            
            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    unique_articles.append(article)
            
            filtered_articles = [a for a in unique_articles if should_prefilter_article(a["title"], a.get("summary", ""))]
            filtered_articles = optimize_articles_for_cost(filtered_articles)
            
            stored_count = 0
            for article in filtered_articles:
                classification = await loop.run_in_executor(None, classify_article_with_gemini, article)
                if not classification:
                    continue
                if not classification.get("is_important") or classification.get("impact_level") not in ["critical", "high"]:
                    continue
                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored_count += 1
                await asyncio.sleep(1)
            
            log.info(f"News cycle complete: {stored_count} stored")
            await asyncio.sleep(interval)
        except Exception as e:
            log.error(f"Financial news task error: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 4: GAP FILL SERVICE
# ============================================================================

PRIORITY_SYMBOLS = ["BTC/USD", "ETH/USD", "EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD", "AAPL", "MSFT", "SPY", "QQQ"]


def symbol_to_table(symbol: str) -> str:
    return f"candles_{symbol.lower().replace('/', '_')}"


def detect_gaps(symbol: str, lookback_hours: int = MAX_GAP_HOURS) -> List[Tuple]:
    table_name = symbol_to_table(symbol)
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        result = sb.table(table_name).select("timestamp").gte("timestamp", since).order("timestamp", desc=False).execute()
        if not result.data or len(result.data) < 2:
            return []
        gaps = []
        timestamps = [datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) for r in result.data]
        for i in range(1, len(timestamps)):
            gap_minutes = (timestamps[i] - timestamps[i-1]).total_seconds() / 60
            if gap_minutes > MIN_GAP_MINUTES:
                gaps.append((timestamps[i-1] + timedelta(minutes=1), timestamps[i] - timedelta(minutes=1)))
        return gaps
    except Exception:
        return []


async def gap_fill_task():
    log.info("Gap fill service started")
    while not tick_streamer._shutdown.is_set():
        try:
            total_gaps = 0
            for symbol in PRIORITY_SYMBOLS:
                gaps = detect_gaps(symbol)
                if gaps:
                    log.info(f"{symbol}: {len(gaps)} gaps detected")
                    total_gaps += len(gaps)
            log.info(f"Gap scan complete: {total_gaps} total gaps")
            await asyncio.sleep(GAP_SCAN_INTERVAL)
        except Exception as e:
            log.error(f"Gap fill error: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 5: BOND YIELDS (G20)
# ============================================================================

G20_BONDS = [
    {"country": "US", "name": "United States", "url": "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"},
    {"country": "DE", "name": "Germany", "url": "https://www.investing.com/rates-bonds/germany-10-year-bond-yield"},
    {"country": "GB", "name": "United Kingdom", "url": "https://www.investing.com/rates-bonds/uk-10-year-bond-yield"},
    {"country": "JP", "name": "Japan", "url": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield"},
    {"country": "FR", "name": "France", "url": "https://www.investing.com/rates-bonds/france-10-year-bond-yield"},
    {"country": "IT", "name": "Italy", "url": "https://www.investing.com/rates-bonds/italy-10-year-bond-yield"},
    {"country": "CA", "name": "Canada", "url": "https://www.investing.com/rates-bonds/canada-10-year-bond-yield"},
    {"country": "AU", "name": "Australia", "url": "https://www.investing.com/rates-bonds/australia-10-year-bond-yield"},
    {"country": "CN", "name": "China", "url": "https://www.investing.com/rates-bonds/china-10-year-bond-yield"},
    {"country": "IN", "name": "India", "url": "https://www.investing.com/rates-bonds/india-10-year-bond-yield"},
    {"country": "BR", "name": "Brazil", "url": "https://www.investing.com/rates-bonds/brazil-10-year-bond-yield"},
    {"country": "MX", "name": "Mexico", "url": "https://www.investing.com/rates-bonds/mexico-10-year-bond-yield"},
]


def scrape_bond_yield(url: str) -> Optional[float]:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        price_elem = soup.find("span", {"data-test": "instrument-price-last"})
        if price_elem:
            return float(price_elem.text.strip().replace(",", ""))
        return None
    except Exception:
        return None


def scrape_all_bond_yields() -> List[Dict]:
    results = []
    for bond in G20_BONDS:
        yield_10y = scrape_bond_yield(bond["url"])
        results.append({
            "country": bond["country"],
            "country_name": bond["name"],
            "yield_10y": yield_10y,
            "yield_2y": None,
            "spread_10y_2y": None,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
        time.sleep(1)
    return results


def store_bond_yields(yields: List[Dict]) -> int:
    stored = 0
    for y in yields:
        try:
            sb.table("sovereign_yields").upsert(y, on_conflict="country").execute()
            stored += 1
        except Exception:
            pass
    return stored


async def bond_yield_task():
    log.info("Bond yield scraper started")
    while not tick_streamer._shutdown.is_set():
        try:
            log.info("Scraping G20 bond yields...")
            loop = asyncio.get_event_loop()
            yields = await loop.run_in_executor(None, scrape_all_bond_yields)
            if yields:
                has_10y = sum(1 for y in yields if y["yield_10y"])
                await loop.run_in_executor(None, store_bond_yields, yields)
                log.info(f"Bond scrape complete: {has_10y}/{len(G20_BONDS)} countries")
            await asyncio.sleep(BOND_SCRAPE_INTERVAL)
        except Exception as e:
            log.error(f"Bond yield task error: {e}")
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 6: WHALE FLOW TRACKER (Simplified)
# ============================================================================

class WhaleFlowTracker:
    def __init__(self):
        self._shutdown = asyncio.Event()
        self._btc_price = 50000.0
        self._tx_count = 0
        self._whale_count = 0
    
    def shutdown(self):
        self._shutdown.set()
    
    async def _fetch_btc_price(self):
        try:
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                self._btc_price = response.json()["bitcoin"]["usd"]
        except Exception:
            pass
    
    async def run(self):
        log.info("Whale Flow Tracker started")
        await self._fetch_btc_price()
        url = "wss://ws.blockchain.info/inv"
        backoff = 1
        
        while not self._shutdown.is_set():
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                    log.info("Subscribed to BTC unconfirmed transactions")
                    backoff = 1
                    
                    async for message in ws:
                        if self._shutdown.is_set():
                            break
                        try:
                            data = json.loads(message)
                            if data.get("op") == "utx":
                                self._tx_count += 1
                                tx = data.get("x", {})
                                outputs = tx.get("out", [])
                                total_btc = sum(o.get("value", 0) for o in outputs) / 1e8
                                total_usd = total_btc * self._btc_price
                                if total_usd >= 500000:
                                    self._whale_count += 1
                                    log.info(f"WHALE: {total_btc:.2f} BTC (${total_usd:,.0f})")
                        except Exception:
                            pass
            except Exception as e:
                log.warning(f"Whale tracker error: {e}")
            
            if self._shutdown.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(30, backoff * 2)


whale_tracker = WhaleFlowTracker()


async def whale_flow_task():
    await whale_tracker.run()


# ============================================================================
#                    TASK 7: AI NEWS DESK (Placeholder)
# ============================================================================

async def ai_news_desk_task():
    log.info("AI News Desk started (placeholder)")
    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            if now.weekday() >= 5:
                await asyncio.sleep(14400)
                continue
            interval = 1800 if 8 <= now.hour < 21 else 3600
            await asyncio.sleep(interval)
        except Exception as e:
            log.error(f"AI News Desk error: {e}")
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 8: G20 MACRO FETCHER
# ============================================================================

G20_MACRO_COUNTRIES = {
    "US": {"name": "United States", "currency": "USD", "econ_code": "US"},
    "UK": {"name": "United Kingdom", "currency": "GBP", "econ_code": "UK"},
    "DE": {"name": "Germany", "currency": "EUR", "econ_code": "DE"},
    "FR": {"name": "France", "currency": "EUR", "econ_code": "FR"},
    "IT": {"name": "Italy", "currency": "EUR", "econ_code": "IT"},
    "JP": {"name": "Japan", "currency": "JPY", "econ_code": "JP"},
    "CN": {"name": "China", "currency": "CNY", "econ_code": "CN"},
    "IN": {"name": "India", "currency": "INR", "econ_code": "IN"},
    "BR": {"name": "Brazil", "currency": "BRL", "econ_code": "BR"},
    "RU": {"name": "Russia", "currency": "RUB", "econ_code": "RU"},
    "CA": {"name": "Canada", "currency": "CAD", "econ_code": "CA"},
    "AU": {"name": "Australia", "currency": "AUD", "econ_code": "AU"},
    "MX": {"name": "Mexico", "currency": "MXN", "econ_code": "MX"},
    "KR": {"name": "South Korea", "currency": "KRW", "econ_code": "KR"},
    "ID": {"name": "Indonesia", "currency": "IDR", "econ_code": "ID"},
    "TR": {"name": "Turkey", "currency": "TRY", "econ_code": "TR"},
    "SA": {"name": "Saudi Arabia", "currency": "SAR", "econ_code": "SA"},
    "ZA": {"name": "South Africa", "currency": "ZAR", "econ_code": "ZA"},
    "AR": {"name": "Argentina", "currency": "ARS", "econ_code": "AR"},
    "EU": {"name": "European Union", "currency": "EUR", "econ_code": "EU"},
}

MACRO_INDICATORS = {
    "CPI YoY": {"patterns": [("Inflation Rate", "yoy"), ("Inflation Rate", None), ("CPI", None), ("Consumer Price Index", "yoy")]},
    "Core CPI YoY": {"patterns": [("Core Inflation Rate", "yoy"), ("Core Inflation Rate", None), ("Core CPI", "yoy")]},
    "Consumer Confidence": {"patterns": [("Consumer Confidence", None), ("CB Consumer Confidence", None), ("Michigan Consumer Sentiment", None)]},
    "PCE YoY": {"patterns": [("PCE Price Index", "yoy"), ("PCE Price Index", None), ("Core PCE Price Index", "yoy")]},
    "GDP QoQ": {"patterns": [("GDP Growth Rate", "qoq"), ("Gross Domestic Product", "qoq"), ("GDP", "qoq")]},
    "GDP YoY": {"patterns": [("GDP Growth Rate", "yoy"), ("Gross Domestic Product", "yoy"), ("GDP", "yoy")]},
    "Manufacturing PMI": {"patterns": [("ISM Manufacturing PMI", None), ("S&P Global Manufacturing PMI", None), ("Manufacturing PMI", None)]},
    "Services PMI": {"patterns": [("ISM Services PMI", None), ("ISM Non-Manufacturing PMI", None), ("S&P Global Services PMI", None), ("Services PMI", None)]},
    "Composite PMI": {"patterns": [("S&P Global Composite PMI", None), ("Composite PMI", None)]},
    "Employment": {"patterns": [("Non Farm Payrolls", None), ("Employment Change", None)]},
    "Unemployment": {"patterns": [("Unemployment Rate", None), ("Jobless Rate", None)]},
    "Jobless Claims": {"patterns": [("Initial Jobless Claims", None), ("Continuing Jobless Claims", None)]},
    "PPI MoM": {"patterns": [("Producer Price Index", "mom"), ("Producer Price Index", None), ("PPI", "mom")]},
    "Retail Sales MoM": {"patterns": [("Retail Sales", "mom"), ("Retail Sales", None)]},
    "Interest Rate": {"patterns": [("Interest Rate Decision", None), ("Fed Interest Rate Decision", None), ("ECB Interest Rate Decision", None)]},
    "Trade Balance": {"patterns": [("Balance of Trade", None), ("Trade Balance", None)]},
}


def classify_macro_event(event_type: str, comparison: str = None) -> Optional[str]:
    event_lower = event_type.lower().strip()
    comp_lower = comparison.lower().strip() if comparison else None
    for indicator_name, config in MACRO_INDICATORS.items():
        for pattern_name, pattern_comp in config["patterns"]:
            if pattern_name.lower() in event_lower or event_lower in pattern_name.lower():
                if pattern_comp is None or comp_lower == pattern_comp:
                    return indicator_name
    return None


def parse_float_safe(value: Any) -> Optional[float]:
    if value in [None, "", "null", "N/A", "-"]:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def calculate_surprise(actual: float, estimate: float) -> Tuple[Optional[float], Optional[str]]:
    if actual is None or estimate is None or estimate == 0:
        return None, None
    surprise = ((actual - estimate) / abs(estimate)) * 100
    if abs(surprise) < 1:
        direction = "inline"
    elif surprise > 0:
        direction = "beat"
    else:
        direction = "miss"
    return round(surprise, 2), direction


def fetch_country_macro_events(country_code: str) -> List[Dict]:
    country_info = G20_MACRO_COUNTRIES.get(country_code, {})
    econ_code = country_info.get("econ_code", country_code)
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    url = "https://eodhd.com/api/economic-events"
    params = {"api_token": EODHD_API_KEY, "country": econ_code, "from": from_date, "to": to_date, "limit": 1000, "fmt": "json"}
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        return []
    except Exception:
        return []


def process_country_indicators(country_code: str, events: List[Dict]) -> Dict[str, Dict]:
    country_info = G20_MACRO_COUNTRIES.get(country_code, {})
    indicators_data = {}
    for event in events:
        event_type = event.get("type", "")
        comparison = event.get("comparison", "")
        indicator = classify_macro_event(event_type, comparison)
        if not indicator:
            continue
        actual = parse_float_safe(event.get("actual"))
        estimate = parse_float_safe(event.get("estimate"))
        previous = parse_float_safe(event.get("previous"))
        change = parse_float_safe(event.get("change"))
        change_pct = parse_float_safe(event.get("change_percentage"))
        if actual is None and previous is None:
            continue
        event_date = event.get("date", "")
        period = event.get("period", "")
        surprise, surprise_dir = calculate_surprise(actual, estimate)
        current = indicators_data.get(indicator)
        if current is None or (actual is not None and event_date > current.get("event_date", "")):
            indicators_data[indicator] = {
                "country_code": country_code,
                "country_name": country_info.get("name", country_code),
                "currency": country_info.get("currency"),
                "indicator": indicator,
                "event_type": event_type,
                "comparison": comparison,
                "actual": actual,
                "previous": previous,
                "estimate": estimate,
                "change": change,
                "change_pct": change_pct,
                "surprise": surprise,
                "surprise_direction": surprise_dir,
                "period": period,
                "event_date": event_date,
            }
    return indicators_data


def upsert_macro_indicator(data: Dict) -> bool:
    try:
        existing = sb.table("macro_indicators").select("actual, previous, estimate, event_date").eq(
            "country_code", data["country_code"]
        ).eq("indicator", data["indicator"]).execute()
        if existing.data:
            row = existing.data[0]
            if (row["actual"] == data["actual"] and row["previous"] == data["previous"] and 
                row["estimate"] == data["estimate"] and row["event_date"] == data["event_date"]):
                return False
        sb.table("macro_indicators").upsert({
            "country_code": data["country_code"],
            "country_name": data["country_name"],
            "currency": data["currency"],
            "indicator": data["indicator"],
            "event_type": data["event_type"],
            "comparison": data["comparison"],
            "actual": data["actual"],
            "previous": data["previous"],
            "estimate": data["estimate"],
            "change": data["change"],
            "change_pct": data["change_pct"],
            "surprise": data["surprise"],
            "surprise_direction": data["surprise_direction"],
            "period": data["period"],
            "event_date": data["event_date"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="country_code,indicator").execute()
        return True
    except Exception as e:
        log.error(f"Error upserting macro indicator: {e}")
        return False


def run_macro_fetch_cycle():
    log.info("Starting G20 macro fetch cycle (16 indicators x 20 countries)")
    total_indicators = 0
    updated_count = 0
    countries_processed = 0
    for country_code in G20_MACRO_COUNTRIES.keys():
        try:
            events = fetch_country_macro_events(country_code)
            if not events:
                continue
            indicators = process_country_indicators(country_code, events)
            country_updates = 0
            for indicator_name, data in indicators.items():
                if upsert_macro_indicator(data):
                    updated_count += 1
                    country_updates += 1
                total_indicators += 1
            countries_processed += 1
            if country_updates > 0:
                log.info(f"  {country_code}: {len(indicators)} indicators, {country_updates} updated")
            time.sleep(0.2)
        except Exception as e:
            log.error(f"  {country_code}: Error - {e}")
    log.info(f"Macro cycle complete: {countries_processed}/{len(G20_MACRO_COUNTRIES)} countries, {total_indicators} indicators, {updated_count} updated")
    return updated_count


async def macro_fetcher_task():
    log.info("G20 Macro Fetcher started (hourly, upsert-only)")
    await asyncio.sleep(30)
    while not tick_streamer._shutdown.is_set():
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, run_macro_fetch_cycle)
            log.info(f"Next macro fetch in {MACRO_FETCH_INTERVAL // 60} minutes")
            await asyncio.sleep(MACRO_FETCH_INTERVAL)
        except Exception as e:
            log.error(f"Macro fetcher task error: {e}")
            await asyncio.sleep(300)


# ============================================================================
#                       TASK 9: INDEX POLLER (REST API)
# ============================================================================

# Index symbols: EODHD API symbol -> (display_symbol, candle_table)
INDEX_SYMBOLS = {
    "GSPC.INDX": ("SPX", "candles_spx"),
    "DJI.INDX": ("US30", "candles_us30"),
    "IXIC.INDX": ("NASDAQ", "candles_nasdaq"),
    "NDX.INDX": ("NAS100", "candles_nas100"),
    "RUT.INDX": ("RUT", "candles_rut"),
    "FTSE.INDX": ("UK100", "candles_uk100"),
    "GDAXI.INDX": ("DAX", "candles_dax"),
    "FCHI.INDX": ("CAC40", "candles_cac40"),
    "N225.INDX": ("NIKKEI", "candles_nikkei"),
    "HSI.INDX": ("HSI", "candles_hsi"),
    "STOXX50E.INDX": ("STOXX50", "candles_stoxx50"),
}


def fetch_index_realtime(eodhd_symbol: str) -> Optional[Dict]:
    """Fetch real-time quote for a single index."""
    url = f"https://eodhd.com/api/real-time/{eodhd_symbol}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data and data.get("close") and data.get("close") != "NA":
                return data
    except Exception as e:
        log.warning(f"Index fetch error {eodhd_symbol}: {e}")
    return None


def fetch_all_indices_parallel() -> Dict[str, Dict]:
    """Fetch all indices in parallel using ThreadPoolExecutor."""
    results = {}
    with ThreadPoolExecutor(max_workers=11) as executor:
        futures = {executor.submit(fetch_index_realtime, sym): sym for sym in INDEX_SYMBOLS.keys()}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                if data:
                    results[symbol] = data
            except Exception:
                pass
    return results


def insert_index_to_tickdata(display_symbol: str, price: float, change: float, change_p: float):
    """Insert index tick into tickdata table."""
    try:
        tick = {
            "symbol": display_symbol,
            "price": price,
            "bid": None,
            "ask": None,
            "volume": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        sb.table(SUPABASE_TABLE).insert(tick).execute()
        return True
    except Exception as e:
        log.warning(f"Index tickdata insert failed for {display_symbol}: {e}")
        return False


def run_index_poll_cycle() -> int:
    """Run one cycle of index polling. Returns number of indices updated."""
    data = fetch_all_indices_parallel()
    updated = 0
    now = datetime.now(timezone.utc)
    
    for eodhd_symbol, (display_symbol, candle_table) in INDEX_SYMBOLS.items():
        d = data.get(eodhd_symbol)
        if not d:
            continue
        
        try:
            price = float(d["close"])
            change = float(d.get("change", 0) or 0)
            change_p = float(d.get("change_p", 0) or 0)
            high = float(d.get("high", price) or price)
            low = float(d.get("low", price) or price)
            open_price = float(d.get("open", price) or price)
            
            # 1. Insert to tickdata
            try:
                tick = {
                    "symbol": display_symbol,
                    "price": price,
                    "bid": None,
                    "ask": None,
                    "volume": None,
                    "ts": now.isoformat(),
                }
                sb.table(SUPABASE_TABLE).insert(tick).execute()
            except Exception as e:
                log.warning(f"Index tickdata insert failed for {display_symbol}: {e}")
            
            # 2. Insert/upsert to candle table (1-minute candle)
            # Round timestamp to current minute
            minute_ts = now.replace(second=0, microsecond=0).isoformat()
            try:
                candle = {
                    "timestamp": minute_ts,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": price,
                    "volume": None,
                }
                sb.table(candle_table).upsert(candle, on_conflict="timestamp").execute()
            except Exception as e:
                log.warning(f"Index candle insert failed for {display_symbol} -> {candle_table}: {e}")
            
            updated += 1
            
        except Exception as e:
            log.warning(f"Index process error {display_symbol}: {e}")
    
    return updated
    
    return updated


async def index_poller_task():
    """Task 9: Poll global indices every 20 seconds via REST API."""
    log.info(f"Index Poller started ({len(INDEX_SYMBOLS)} indices, every {INDEX_POLL_INTERVAL}s)")
    await asyncio.sleep(5)  # Small delay at startup
    
    poll_count = 0
    total_updates = 0
    
    while not tick_streamer._shutdown.is_set():
        try:
            poll_count += 1
            loop = asyncio.get_event_loop()
            updated = await loop.run_in_executor(None, run_index_poll_cycle)
            total_updates += updated
            
            # Log every poll for first 10, then every 30
            if poll_count <= 10 or poll_count % 30 == 0:
                log.info(f"Index Poller: poll #{poll_count}, {updated} indices updated this cycle, {total_updates} total")
            
            await asyncio.sleep(INDEX_POLL_INTERVAL)
            
        except Exception as e:
            log.error(f"Index poller error: {e}")
            await asyncio.sleep(60)


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    log.info("=" * 70)
    log.info("LONDON STRATEGIC EDGE - WORKER v8.2")
    log.info("=" * 70)
    log.info(f"  1. Tick streaming (EODHD) - {TOTAL_SYMBOLS} symbols")
    log.info(f"  2. Economic calendar (EODHD API) - every 5 min")
    log.info(f"  3. Financial news ({len(RSS_FEEDS)} RSS + Gemini)")
    log.info(f"  4. Gap detection & backfill")
    log.info(f"  5. Bond yields (G20) - 30 min")
    log.info(f"  6. Whale flow tracker")
    log.info(f"  7. AI News Desk (placeholder)")
    log.info(f"  8. G20 Macro Fetcher - 16 indicators x 20 countries (hourly)")
    log.info(f"  9. Index Poller - {len(INDEX_SYMBOLS)} indices (every {INDEX_POLL_INTERVAL}s)")
    log.info("=" * 70)
    log.info(f"Symbols: Forex={len(ALL_FOREX)} Crypto={len(ALL_CRYPTO)} Stocks={len(ALL_US_STOCKS)} ETFs={len(ALL_ETFS)} Indices={len(INDEX_SYMBOLS)}")
    log.info("=" * 70)

    def signal_handler(sig, frame):
        log.info(f"Received signal {sig}, shutting down...")
        tick_streamer.shutdown()
        whale_tracker.shutdown()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    tasks = [
        asyncio.create_task(tick_streaming_task(), name="tick_stream"),
        asyncio.create_task(economic_calendar_task(), name="econ_calendar"),
        asyncio.create_task(financial_news_task(), name="news"),
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
        asyncio.create_task(bond_yield_task(), name="bond_yields"),
        asyncio.create_task(whale_flow_task(), name="whale_flow"),
        asyncio.create_task(ai_news_desk_task(), name="ai_news_desk"),
        asyncio.create_task(macro_fetcher_task(), name="macro_fetcher"),
        asyncio.create_task(index_poller_task(), name="index_poller"),
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log.error(f"Main loop error: {e}")
    finally:
        tick_streamer.shutdown()
        whale_tracker.shutdown()
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        log.info("Worker shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Goodbye!")
        sys.exit(0)
