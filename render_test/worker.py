#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v9.0 (DEBUG EDITION)
================================================================================
Production-grade data pipeline with comprehensive debugging and diagnostics.

DEBUG FEATURES:
  - Startup schema validation
  - Request/response logging with payloads
  - Detailed error context and stack traces
  - Per-task health metrics
  - Configurable debug verbosity

TASKS:
  1. Tick Streaming     - EODHD WebSocket (280 symbols across 7 connections)
  2. Economic Calendar  - EODHD API (every 5 min)
  3. Financial News     - RSS feeds + Gemini AI classification
  4. Gap Fill Service   - Auto-detect and repair missing candle data
  5. Bond Yields        - G20 sovereign yields (every 30 minutes)
  6. Whale Flow Tracker - BTC whale movements via Blockchain.com WebSocket
  7. AI News Desk       - Placeholder for article synthesis
  8. G20 Macro Fetcher  - 20 countries x 6 indicators (hourly)
  9. Index Poller       - 11 global indices via REST API (every 20 seconds)
  10. Sector Sentiment  - AI market sentiment analysis (scheduled updates)
  11. News Articles     - Multi-source news scraper with filtering

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
import traceback
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Tuple, Any
from contextlib import suppress
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser

# ============================================================================
#                              CONFIGURATION
# ============================================================================

# Debug settings - set via environment or change here
DEBUG_MODE = os.environ.get("DEBUG_MODE", "true").lower() == "true"
DEBUG_LOG_PAYLOADS = os.environ.get("DEBUG_LOG_PAYLOADS", "false").lower() == "true"
DEBUG_LOG_RESPONSES = os.environ.get("DEBUG_LOG_RESPONSES", "false").lower() == "true"

# API Keys and URLs
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
GOOGLE_GEMINI_API_KEY = os.environ.get("GOOGLE_GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# Timing configuration
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
INDEX_POLL_INTERVAL = 20
CALENDAR_FETCH_INTERVAL = 300
NEWS_SCAN_INTERVAL_ACTIVE = 600
NEWS_SCAN_INTERVAL_QUIET = 1800
NEWS_SCAN_INTERVAL_WEEKEND = 7200

# ============================================================================
#                              LOGGING SETUP
# ============================================================================

class DebugFormatter(logging.Formatter):
    """Custom formatter with colors for terminal output."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        if hasattr(record, 'task'):
            task_prefix = f"[{record.task}] "
        else:
            task_prefix = ""
        
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        formatted = f"{self.formatTime(record)} | {color}{record.levelname:8s}{reset} | {task_prefix}{record.getMessage()}"
        
        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        
        return formatted


def setup_logging():
    """Configure logging with debug-friendly format."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
    console_handler.setFormatter(DebugFormatter())
    root_logger.addHandler(console_handler)
    
    # Reduce noise from external libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    return logging.getLogger("worker")


log = setup_logging()


def log_with_task(level: str, task: str, message: str, **kwargs):
    """Log a message with task context."""
    extra = {'task': task}
    getattr(log, level)(message, extra=extra, **kwargs)


def debug(task: str, message: str):
    """Debug log with task context."""
    if DEBUG_MODE:
        log_with_task('debug', task, message)


def info(task: str, message: str):
    """Info log with task context."""
    log_with_task('info', task, message)


def warning(task: str, message: str):
    """Warning log with task context."""
    log_with_task('warning', task, message)


def error(task: str, message: str, exc_info: bool = False):
    """Error log with task context."""
    log_with_task('error', task, message, exc_info=exc_info)


# ============================================================================
#                           METRICS TRACKING
# ============================================================================

@dataclass
class TaskMetrics:
    """Track metrics for a single task."""
    name: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    success_count: int = 0
    error_count: int = 0
    last_success: Optional[datetime] = None
    last_error: Optional[datetime] = None
    last_error_message: Optional[str] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def record_success(self):
        self.success_count += 1
        self.last_success = datetime.now(timezone.utc)
    
    def record_error(self, message: str):
        self.error_count += 1
        self.last_error = datetime.now(timezone.utc)
        self.last_error_message = message
    
    def get_success_rate(self) -> float:
        total = self.success_count + self.error_count
        return (self.success_count / total * 100) if total > 0 else 0.0
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "uptime_seconds": (datetime.now(timezone.utc) - self.started_at).total_seconds(),
            "success_count": self.success_count,
            "error_count": self.error_count,
            "success_rate": f"{self.get_success_rate():.1f}%",
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_error": self.last_error.isoformat() if self.last_error else None,
            "last_error_message": self.last_error_message,
            **self.custom_metrics
        }


class MetricsCollector:
    """Collect and report metrics across all tasks."""
    
    def __init__(self):
        self._tasks: Dict[str, TaskMetrics] = {}
        self._lock = threading.Lock()
    
    def get_or_create(self, task_name: str) -> TaskMetrics:
        with self._lock:
            if task_name not in self._tasks:
                self._tasks[task_name] = TaskMetrics(name=task_name)
            return self._tasks[task_name]
    
    def report(self) -> Dict:
        with self._lock:
            return {name: metrics.to_dict() for name, metrics in self._tasks.items()}
    
    def log_summary(self):
        """Log a summary of all task metrics."""
        log.info("=" * 70)
        log.info("METRICS SUMMARY")
        log.info("=" * 70)
        for name, metrics in self._tasks.items():
            status = "✓" if metrics.error_count == 0 else "⚠" if metrics.get_success_rate() > 90 else "✗"
            log.info(f"  {status} {name}: {metrics.success_count} ok, {metrics.error_count} err ({metrics.get_success_rate():.1f}%)")
            if metrics.last_error_message:
                log.info(f"      Last error: {metrics.last_error_message[:80]}")
        log.info("=" * 70)


metrics = MetricsCollector()

# ============================================================================
#                           SUPABASE CLIENT WITH DEBUGGING
# ============================================================================

class DebugSupabaseClient:
    """Wrapper around Supabase client with request/response logging."""
    
    def __init__(self, url: str, key: str):
        self._client = create_client(url, key) if url and key else None
        self._request_count = 0
        self._error_count = 0
        self._lock = threading.Lock()
    
    @property
    def client(self):
        return self._client
    
    def table(self, name: str):
        """Get a table reference with debug logging."""
        return DebugTableRef(self, name)
    
    def log_request(self, table: str, operation: str, data: Any = None):
        """Log an outgoing request."""
        with self._lock:
            self._request_count += 1
        
        if DEBUG_LOG_PAYLOADS and data:
            debug("SUPABASE", f"Request #{self._request_count}: {operation} {table}")
            debug("SUPABASE", f"  Payload: {json.dumps(data, default=str)[:500]}")
        else:
            debug("SUPABASE", f"Request #{self._request_count}: {operation} {table}")
    
    def log_response(self, table: str, operation: str, response: Any, error: Exception = None):
        """Log a response."""
        if error:
            with self._lock:
                self._error_count += 1
            warning("SUPABASE", f"Response ERROR: {operation} {table} - {str(error)[:200]}")
            if DEBUG_MODE:
                warning("SUPABASE", f"  Full error: {error}")
        elif DEBUG_LOG_RESPONSES:
            debug("SUPABASE", f"Response OK: {operation} {table}")
    
    def get_stats(self) -> Dict:
        return {
            "total_requests": self._request_count,
            "total_errors": self._error_count,
            "error_rate": f"{(self._error_count / self._request_count * 100) if self._request_count > 0 else 0:.1f}%"
        }


class DebugTableRef:
    """Wrapper around Supabase table operations with debug logging."""
    
    def __init__(self, client: DebugSupabaseClient, table_name: str):
        self._client = client
        self._table_name = table_name
        self._table = client.client.table(table_name) if client.client else None
    
    def insert(self, data: Any):
        """Insert with logging."""
        self._client.log_request(self._table_name, "INSERT", data)
        
        class InsertBuilder:
            def __init__(inner_self, table_ref):
                inner_self._table_ref = table_ref
                inner_self._data = data
            
            def execute(inner_self):
                try:
                    if inner_self._table_ref._table is None:
                        raise Exception("Supabase client not initialized")
                    result = inner_self._table_ref._table.insert(inner_self._data).execute()
                    inner_self._table_ref._client.log_response(inner_self._table_ref._table_name, "INSERT", result)
                    return result
                except Exception as e:
                    inner_self._table_ref._client.log_response(inner_self._table_ref._table_name, "INSERT", None, e)
                    raise
        
        return InsertBuilder(self)
    
    def upsert(self, data: Any, on_conflict: str = None):
        """Upsert with logging."""
        self._client.log_request(self._table_name, f"UPSERT(on_conflict={on_conflict})", data)
        
        class UpsertBuilder:
            def __init__(inner_self, table_ref):
                inner_self._table_ref = table_ref
                inner_self._data = data
                inner_self._on_conflict = on_conflict
            
            def execute(inner_self):
                try:
                    if inner_self._table_ref._table is None:
                        raise Exception("Supabase client not initialized")
                    result = inner_self._table_ref._table.upsert(inner_self._data, on_conflict=inner_self._on_conflict).execute()
                    inner_self._table_ref._client.log_response(inner_self._table_ref._table_name, "UPSERT", result)
                    return result
                except Exception as e:
                    inner_self._table_ref._client.log_response(inner_self._table_ref._table_name, "UPSERT", None, e)
                    raise
        
        return UpsertBuilder(self)
    
    def select(self, columns: str = "*"):
        """Select with chaining support."""
        return SelectBuilder(self, columns)


class SelectBuilder:
    """Chainable select query builder with logging."""
    
    def __init__(self, table_ref: DebugTableRef, columns: str):
        self._table_ref = table_ref
        self._columns = columns
        self._filters = []
        self._limit_val = None
        self._order_col = None
        self._order_desc = False
    
    def eq(self, column: str, value: Any):
        self._filters.append(("eq", column, value))
        return self
    
    def gte(self, column: str, value: Any):
        self._filters.append(("gte", column, value))
        return self
    
    def lte(self, column: str, value: Any):
        self._filters.append(("lte", column, value))
        return self
    
    def limit(self, count: int):
        self._limit_val = count
        return self
    
    def order(self, column: str, desc: bool = False):
        self._order_col = column
        self._order_desc = desc
        return self
    
    def execute(self):
        filter_desc = ", ".join([f"{f[1]}={f[0]}({f[2]})" for f in self._filters])
        self._table_ref._client.log_request(
            self._table_ref._table_name, 
            f"SELECT({self._columns}) WHERE {filter_desc}"
        )
        
        try:
            if self._table_ref._table is None:
                raise Exception("Supabase client not initialized")
            
            query = self._table_ref._table.select(self._columns)
            
            for filter_type, column, value in self._filters:
                if filter_type == "eq":
                    query = query.eq(column, value)
                elif filter_type == "gte":
                    query = query.gte(column, value)
                elif filter_type == "lte":
                    query = query.lte(column, value)
            
            if self._limit_val:
                query = query.limit(self._limit_val)
            
            if self._order_col:
                query = query.order(self._order_col, desc=self._order_desc)
            
            result = query.execute()
            self._table_ref._client.log_response(self._table_ref._table_name, "SELECT", result)
            return result
        except Exception as e:
            self._table_ref._client.log_response(self._table_ref._table_name, "SELECT", None, e)
            raise


# Initialize Supabase client
sb = DebugSupabaseClient(SUPABASE_URL, SUPABASE_KEY)

# ============================================================================
#                           TRADING SYMBOLS
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
#                    STARTUP DIAGNOSTICS
# ============================================================================

def run_startup_diagnostics():
    """Run comprehensive startup checks and log configuration."""
    log.info("=" * 70)
    log.info("STARTUP DIAGNOSTICS")
    log.info("=" * 70)
    
    # Check environment variables
    env_checks = [
        ("EODHD_API_KEY", EODHD_API_KEY, True),
        ("SUPABASE_URL", SUPABASE_URL, True),
        ("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_KEY, True),
        ("GOOGLE_GEMINI_API_KEY", GOOGLE_GEMINI_API_KEY, False),
    ]
    
    log.info("Environment Variables:")
    all_required_present = True
    for name, value, required in env_checks:
        if value:
            masked = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
            status = "✓"
        else:
            masked = "NOT SET"
            status = "✗" if required else "○"
            if required:
                all_required_present = False
        log.info(f"  {status} {name}: {masked}")
    
    if not all_required_present:
        log.error("Missing required environment variables! Worker may not function correctly.")
    
    # Log configuration
    log.info("")
    log.info("Configuration:")
    log.info(f"  DEBUG_MODE: {DEBUG_MODE}")
    log.info(f"  DEBUG_LOG_PAYLOADS: {DEBUG_LOG_PAYLOADS}")
    log.info(f"  DEBUG_LOG_RESPONSES: {DEBUG_LOG_RESPONSES}")
    log.info(f"  BATCH_MAX: {BATCH_MAX}")
    log.info(f"  BATCH_FLUSH_INTERVAL: {BATCH_FLUSH_INTERVAL}s")
    log.info(f"  INDEX_POLL_INTERVAL: {INDEX_POLL_INTERVAL}s")
    log.info(f"  CALENDAR_FETCH_INTERVAL: {CALENDAR_FETCH_INTERVAL}s")
    
    # Log symbol counts
    log.info("")
    log.info("Symbol Coverage:")
    log.info(f"  Forex: {len(ALL_FOREX)}")
    log.info(f"  Crypto: {len(ALL_CRYPTO)}")
    log.info(f"  US Stocks: {len(ALL_US_STOCKS)}")
    log.info(f"  ETFs: {len(ALL_ETFS)}")
    log.info(f"  Indices: {len(INDEX_SYMBOLS)}")
    log.info(f"  TOTAL: {TOTAL_SYMBOLS + len(INDEX_SYMBOLS)}")
    
    # Test Supabase connection
    log.info("")
    log.info("Testing Supabase Connection...")
    try:
        if sb.client:
            # Try a simple query
            result = sb.client.table(SUPABASE_TABLE).select("symbol").limit(1).execute()
            log.info(f"  ✓ Supabase connection OK (table: {SUPABASE_TABLE})")
        else:
            log.error("  ✗ Supabase client not initialized")
    except Exception as e:
        log.error(f"  ✗ Supabase connection failed: {e}")
    
    # Test EODHD API
    log.info("")
    log.info("Testing EODHD API...")
    try:
        test_url = f"https://eodhd.com/api/real-time/AAPL.US?api_token={EODHD_API_KEY}&fmt=json"
        response = requests.get(test_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            log.info(f"  ✓ EODHD API OK (AAPL price: ${data.get('close', 'N/A')})")
        else:
            log.warning(f"  ⚠ EODHD API returned status {response.status_code}")
    except Exception as e:
        log.error(f"  ✗ EODHD API test failed: {e}")
    
    log.info("")
    log.info("=" * 70)
    log.info("DIAGNOSTICS COMPLETE")
    log.info("=" * 70)
    log.info("")


# ============================================================================
#                    TASK 1: EODHD TICK STREAMING
# ============================================================================

class EODHDTickStreamer:
    """WebSocket tick streamer with comprehensive debugging."""
    
    TASK_NAME = "TICK_STREAM"
    
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
        self._metrics = metrics.get_or_create(self.TASK_NAME)
        
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
                "errors": [],
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
        """Flush batch to Supabase with error handling."""
        if not self._batch:
            return
        
        batch_copy = self._batch.copy()
        self._batch.clear()
        batch_size = len(batch_copy)
        
        debug(self.TASK_NAME, f"Flushing batch of {batch_size} ticks")
        
        try:
            sb.table(SUPABASE_TABLE).insert(batch_copy).execute()
            self._metrics.record_success()
            self._metrics.custom_metrics["last_batch_size"] = batch_size
            debug(self.TASK_NAME, f"Batch insert OK: {batch_size} ticks")
        except Exception as e:
            self._metrics.record_error(str(e))
            error_msg = str(e)
            warning(self.TASK_NAME, f"Batch insert FAILED: {error_msg[:200]}")
            
            # Log sample of failed data for debugging
            if DEBUG_MODE and batch_copy:
                sample = batch_copy[0]
                debug(self.TASK_NAME, f"Sample failed record: {json.dumps(sample, default=str)}")
    
    async def _periodic_flush(self):
        """Periodically flush tick batch."""
        while not self._shutdown.is_set():
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            async with self._batch_lock:
                await self._flush()
    
    async def _handle_tick(self, data: Dict, conn_name: str, endpoint: str):
        """Process a single tick with validation."""
        try:
            symbol = data.get("s", data.get("symbol", ""))
            if not symbol:
                debug(self.TASK_NAME, f"[{conn_name}] Tick missing symbol: {data}")
                return
            
            normalized = self._normalize_symbol(symbol, endpoint)
            price = self._to_float(data.get("p", data.get("price")))
            bid = self._to_float(data.get("b", data.get("bid")))
            ask = self._to_float(data.get("a", data.get("ask")))
            volume = self._to_float(data.get("v", data.get("volume")))
            
            if price is None and bid is None:
                debug(self.TASK_NAME, f"[{conn_name}] Tick missing price/bid: {symbol}")
                return
            
            if price is None and bid and ask:
                price = (bid + ask) / 2
            
            conn = self._connections[conn_name]
            conn["tick_count"] += 1
            conn["last_tick_time"] = datetime.now(timezone.utc)
            conn["last_activity_time"] = datetime.now(timezone.utc)
            conn["symbols_streaming"].add(symbol)
            self._total_ticks += 1
            
            # Use 'volume' column (not 'day_volume')
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
            warning(self.TASK_NAME, f"Error handling tick: {e}")
            if DEBUG_MODE:
                debug(self.TASK_NAME, f"Failed tick data: {data}")

    async def _kill_connection(self, conn_name: str):
        """Kill a WebSocket connection."""
        conn = self._connections[conn_name]
        conn["is_connected"] = False
        if conn["ws"]:
            try:
                await asyncio.wait_for(conn["ws"].close(), timeout=3)
            except Exception:
                pass
            conn["ws"] = None
        debug(self.TASK_NAME, f"[{conn_name}] Connection killed")

    async def _watchdog(self):
        """Monitor connections and kill stale ones."""
        info(self.TASK_NAME, f"Watchdog started (timeout: {WATCHDOG_TIMEOUT_SECS}s)")
        
        while not self._shutdown.is_set():
            await asyncio.sleep(5)
            now = datetime.now(timezone.utc)
            
            for conn_name, conn in self._connections.items():
                if not conn["is_connected"]:
                    continue
                    
                activity_elapsed = (now - conn["last_activity_time"]).total_seconds()
                
                if activity_elapsed > WATCHDOG_TIMEOUT_SECS:
                    warning(self.TASK_NAME, f"WATCHDOG [{conn_name}]: No activity for {activity_elapsed:.1f}s - killing!")
                    self._failure_reasons["watchdog_kill"] += 1
                    conn["errors"].append(f"Watchdog kill at {now.isoformat()}")
                    await self._kill_connection(conn_name)
            
            # Weekly reset check
            active_connections = sum(1 for c in self._connections.values() if c["is_connected"])
            if active_connections > 0:
                if now.weekday() == WEEKLY_RESET_DAY and now.hour == WEEKLY_RESET_HOUR:
                    reset_key = f"{now.year}-{now.isocalendar()[1]}"
                    if self._last_weekly_reset != reset_key:
                        info(self.TASK_NAME, "WEEKLY RESET: Saturday 10pm maintenance")
                        self._last_weekly_reset = reset_key
                        self._failure_reasons["weekly_reset"] += 1
                        for cn in self._connections:
                            await self._kill_connection(cn)

    async def _nuclear_watchdog(self):
        """Emergency watchdog for system-wide stuck state."""
        info(self.TASK_NAME, f"NUCLEAR WATCHDOG started (timeout: {NUCLEAR_TIMEOUT_SECS}s)")
        
        while not self._shutdown.is_set():
            await asyncio.sleep(10)
            now = datetime.now(timezone.utc)
            main_loop_elapsed = (now - self._last_main_loop_heartbeat).total_seconds()
            
            if main_loop_elapsed > NUCLEAR_TIMEOUT_SECS:
                error(self.TASK_NAME, f"NUCLEAR WATCHDOG TRIGGERED! System stuck for {main_loop_elapsed:.1f}s!")
                self._failure_reasons["nuclear_kill"] += 1
                
                for conn_name, conn in self._connections.items():
                    if conn["task"] and not conn["task"].done():
                        conn["task"].cancel()
                    await self._kill_connection(conn_name)
                
                self._last_main_loop_heartbeat = datetime.now(timezone.utc)

    async def _health_logger(self):
        """Periodically log health status."""
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
            
            self._metrics.custom_metrics.update({
                "total_ticks": self._total_ticks,
                "tick_rate": f"{rate:.1f}/sec",
                "active_connections": f"{active}/{len(self._connections)}",
                "symbols_streaming": len(streaming),
                "status": status,
                "failure_reasons": dict(self._failure_reasons),
            })
            
            info(self.TASK_NAME, f"HEALTH: {status} | Ticks: {self._total_ticks:,} ({rate:.1f}/sec) | Connections: {active}/{len(self._connections)} | Symbols: {len(streaming)}/{TOTAL_SYMBOLS}")
            
            # Log per-connection stats in debug mode
            if DEBUG_MODE:
                for conn_name, conn in self._connections.items():
                    status_icon = "✓" if conn["is_connected"] else "✗"
                    debug(self.TASK_NAME, f"  {status_icon} {conn_name}: {conn['tick_count']} ticks, {len(conn['symbols_streaming'])} symbols")

    async def _run_connection(self, conn_name: str):
        """Run a single WebSocket connection with reconnection logic."""
        conn = self._connections[conn_name]
        config = conn["config"]
        endpoint = config["endpoint"]
        symbols = config["symbols"]
        url = f"{EODHD_ENDPOINTS[endpoint]}?api_token={EODHD_API_KEY}"
        backoff = 1
        
        info(self.TASK_NAME, f"[{conn_name}] Starting connection for {len(symbols)} {endpoint} symbols")
        
        while not self._shutdown.is_set():
            self._last_main_loop_heartbeat = datetime.now(timezone.utc)
            
            try:
                debug(self.TASK_NAME, f"[{conn_name}] Connecting to {endpoint}...")
                
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    conn["ws"] = ws
                    conn["is_connected"] = True
                    conn["last_activity_time"] = datetime.now(timezone.utc)
                    backoff = 1
                    
                    # Subscribe to symbols
                    sub_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                    await ws.send(json.dumps(sub_msg))
                    info(self.TASK_NAME, f"[{conn_name}] Connected and subscribed to {len(symbols)} symbols")
                    
                    # Process messages
                    async for message in ws:
                        if self._shutdown.is_set():
                            break
                            
                        conn["last_activity_time"] = datetime.now(timezone.utc)
                        self._last_main_loop_heartbeat = datetime.now(timezone.utc)
                        
                        try:
                            data = json.loads(message)
                            
                            if isinstance(data, dict):
                                # Skip status messages
                                if data.get("status_code") or data.get("message"):
                                    debug(self.TASK_NAME, f"[{conn_name}] Status: {data.get('message', data)}")
                                    continue
                                await self._handle_tick(data, conn_name, endpoint)
                                
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        await self._handle_tick(item, conn_name, endpoint)
                                        
                        except json.JSONDecodeError as e:
                            debug(self.TASK_NAME, f"[{conn_name}] JSON decode error: {e}")
                            
            except websockets.exceptions.ConnectionClosed as e:
                warning(self.TASK_NAME, f"[{conn_name}] Connection closed: code={e.code}, reason={e.reason}")
                self._failure_reasons["connection_closed"] += 1
                conn["is_connected"] = False
                conn["errors"].append(f"Connection closed: {e.code}")
                
            except Exception as e:
                error(self.TASK_NAME, f"[{conn_name}] Error: {e}", exc_info=DEBUG_MODE)
                self._failure_reasons["exception"] += 1
                conn["is_connected"] = False
                conn["errors"].append(str(e))
            
            if self._shutdown.is_set():
                break
                
            info(self.TASK_NAME, f"[{conn_name}] Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(30, backoff * 2)
    
    async def run(self):
        """Main run loop for tick streamer."""
        info(self.TASK_NAME, f"Starting EODHD tick streamer ({len(self._connections)} connections)")
        
        # Start helper tasks
        flush_task = asyncio.create_task(self._periodic_flush())
        watchdog_task = asyncio.create_task(self._watchdog())
        nuclear_task = asyncio.create_task(self._nuclear_watchdog())
        health_task = asyncio.create_task(self._health_logger())
        
        # Start connection tasks
        for conn_name in self._connections:
            task = asyncio.create_task(self._run_connection(conn_name))
            self._connections[conn_name]["task"] = task
        
        # Wait for shutdown
        await self._shutdown.wait()
        
        # Cleanup
        flush_task.cancel()
        watchdog_task.cancel()
        nuclear_task.cancel()
        health_task.cancel()
        
        for conn in self._connections.values():
            if conn["task"]:
                conn["task"].cancel()
            await self._kill_connection(conn["config"]["name"])
        
        # Final flush
        async with self._batch_lock:
            await self._flush()
        
        info(self.TASK_NAME, "Tick streamer shutdown complete")
    
    def shutdown(self):
        """Signal shutdown."""
        self._shutdown.set()


# Global tick streamer instance
tick_streamer = EODHDTickStreamer()


async def tick_streaming_task():
    """Task 1: WebSocket tick streaming."""
    await tick_streamer.run()


# ============================================================================
#                    TASK 2: ECONOMIC CALENDAR
# ============================================================================

CALENDAR_TASK = "CALENDAR"

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
    """Classify event importance based on keywords."""
    name_lower = event_name.lower()
    for kw in HIGH_IMPACT_KEYWORDS:
        if kw in name_lower:
            return "High"  # Capital H to match constraint
    for kw in MEDIUM_IMPACT_KEYWORDS:
        if kw in name_lower:
            return "Medium"
    return "Low"


def fetch_eodhd_calendar(days_ahead: int = 7) -> Tuple[List[Dict], Optional[str]]:
    """Fetch economic calendar from EODHD API.
    Returns (events, error_message)."""
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
    
    debug(CALENDAR_TASK, f"Fetching calendar: {from_date} to {to_date}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            events = response.json()
            debug(CALENDAR_TASK, f"API returned {len(events)} events")
            return events, None
        else:
            return [], f"API returned status {response.status_code}: {response.text[:200]}"
            
    except requests.Timeout:
        return [], "Request timed out"
    except Exception as e:
        return [], f"Request failed: {e}"


def store_calendar_events(events: List[Dict]) -> Tuple[int, int, List[str]]:
    """Store calendar events with detailed error tracking.
    Returns (stored_count, skipped_count, errors)."""
    if not events:
        return 0, 0, []
    
    stored = 0
    skipped = 0
    errors = []
    
    # Parse all events first
    parsed_events = []
    for event in events:
        try:
            event_date = event.get("date", "")
            date_part = event_date.split(" ")[0] if event_date else None
            time_part = event_date.split(" ")[1] if event_date and " " in event_date else None
            
            if not date_part:
                continue
            
            event_name = event.get("type", "") or ""
            country = event.get("country", "") or ""
            
            parsed_events.append({
                "event": event_name,
                "country": country,
                "date": date_part,
                "time": time_part,
                "actual": str(event.get("actual")) if event.get("actual") is not None else None,
                "previous": str(event.get("previous")) if event.get("previous") is not None else None,
                "consensus": str(event.get("estimate")) if event.get("estimate") is not None else None,
                "importance": classify_calendar_impact(event_name),
            })
        except Exception as e:
            errors.append(f"Parse error: {e}")
            continue
    
    if not parsed_events:
        return 0, 0, errors
    
    debug(CALENDAR_TASK, f"Parsed {len(parsed_events)} events")
    
    # Get date range for batch lookup
    dates = set(e["date"] for e in parsed_events if e["date"])
    if not dates:
        return 0, 0, errors
    
    # Batch fetch existing events
    existing_keys = set()
    try:
        min_date = min(dates)
        max_date = max(dates)
        
        debug(CALENDAR_TASK, f"Checking existing events from {min_date} to {max_date}")
        
        result = sb.table("Economic_calander").select(
            "date,time,country,event"
        ).gte("date", min_date).lte("date", max_date).execute()
        
        for row in result.data:
            key = (row.get("date"), row.get("time"), row.get("country"), row.get("event"))
            existing_keys.add(key)
        
        debug(CALENDAR_TASK, f"Found {len(existing_keys)} existing events")
        
    except Exception as e:
        errors.append(f"Lookup failed: {e}")
        debug(CALENDAR_TASK, f"Existing events lookup failed, will try insert anyway: {e}")
    
    # Filter to only new events
    new_events = []
    for evt in parsed_events:
        key = (evt["date"], evt["time"], evt["country"], evt["event"])
        if key in existing_keys:
            skipped += 1
        else:
            new_events.append(evt)
    
    debug(CALENDAR_TASK, f"New events to insert: {len(new_events)}, skipping {skipped} duplicates")
    
    # Batch insert new events
    if new_events:
        chunk_size = 50
        for i in range(0, len(new_events), chunk_size):
            chunk = new_events[i:i+chunk_size]
            try:
                sb.table("Economic_calander").insert(chunk).execute()
                stored += len(chunk)
                debug(CALENDAR_TASK, f"Inserted chunk of {len(chunk)} events")
            except Exception as e:
                error_msg = str(e)
                errors.append(f"Batch insert failed: {error_msg[:100]}")
                
                # Try individual inserts
                for evt in chunk:
                    try:
                        sb.table("Economic_calander").insert(evt).execute()
                        stored += 1
                    except Exception as e2:
                        skipped += 1
                        if len(errors) < 10:  # Limit error messages
                            errors.append(f"Individual insert failed: {str(e2)[:50]}")
    
    return stored, skipped, errors


async def economic_calendar_task():
    """Task 2: Fetch and store economic calendar events."""
    task_metrics = metrics.get_or_create(CALENDAR_TASK)
    
    info(CALENDAR_TASK, "Economic calendar task started")
    
    while not tick_streamer._shutdown.is_set():
        try:
            info(CALENDAR_TASK, "Fetching economic calendar...")
            
            events, fetch_error = fetch_eodhd_calendar(days_ahead=7)
            
            if fetch_error:
                warning(CALENDAR_TASK, f"Fetch failed: {fetch_error}")
                task_metrics.record_error(fetch_error)
            elif events:
                stored, skipped, errors = store_calendar_events(events)
                high_impact = sum(1 for e in events if classify_calendar_impact(e.get("type", "")) == "High")
                
                info(CALENDAR_TASK, f"Results: {len(events)} fetched, {stored} stored, {skipped} skipped, {high_impact} high-impact")
                
                if errors:
                    for err in errors[:5]:  # Log first 5 errors
                        warning(CALENDAR_TASK, f"  Error: {err}")
                    if len(errors) > 5:
                        warning(CALENDAR_TASK, f"  ... and {len(errors) - 5} more errors")
                
                task_metrics.record_success()
                task_metrics.custom_metrics.update({
                    "last_fetch_count": len(events),
                    "last_stored_count": stored,
                    "last_skipped_count": skipped,
                    "last_error_count": len(errors),
                })
            else:
                info(CALENDAR_TASK, "No events returned from API")
            
            # Wait before next fetch
            await asyncio.sleep(CALENDAR_FETCH_INTERVAL)
            
        except Exception as e:
            error(CALENDAR_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 3: FINANCIAL NEWS
# ============================================================================

NEWS_TASK = "NEWS"

RSS_FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories/", "name": "MarketWatch"},
    {"url": "https://finance.yahoo.com/news/rssindex", "name": "Yahoo Finance"},
    {"url": "https://cointelegraph.com/rss", "name": "CoinTelegraph"},
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml", "name": "Federal Reserve"},
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
    """Check if article passes keyword filters."""
    text = f"{title} {summary}".lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in text:
            return False
    for kw in INCLUDE_KEYWORDS:
        if kw in text:
            return True
    return False


def fetch_rss_feed(feed_url: str, source_name: str) -> Tuple[List[Dict], Optional[str]]:
    """Fetch articles from RSS feed. Returns (articles, error)."""
    try:
        feed = feedparser.parse(feed_url)
        
        if feed.bozo and feed.bozo_exception:
            return [], f"Parse error: {feed.bozo_exception}"
        
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
        
        return articles, None
        
    except Exception as e:
        return [], str(e)


def call_gemini_api(prompt: str, max_tokens: int = 1000, temperature: float = 0.7) -> Tuple[Optional[str], Optional[str]]:
    """Call Gemini API. Returns (response_text, error)."""
    if not GOOGLE_GEMINI_API_KEY:
        return None, "GOOGLE_GEMINI_API_KEY not set"
    
    try:
        url = f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens}
        }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            return text, None
        else:
            return None, f"API returned {response.status_code}: {response.text[:100]}"
            
    except Exception as e:
        return None, str(e)


def classify_article_with_gemini(article: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Classify article using Gemini. Returns (classification, error)."""
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

    response_text, api_error = call_gemini_api(prompt, max_tokens=500, temperature=0.1)
    
    if api_error:
        return None, api_error
    
    if not response_text:
        return None, "Empty response"
    
    try:
        # Clean up response
        text = response_text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        
        return json.loads(text.strip()), None
        
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"


def store_news_article(article: Dict, classification: Dict) -> Tuple[bool, Optional[str]]:
    """Store classified news article. Returns (success, error)."""
    try:
        data = {
            "title": article["title"],
            "summary": classification.get("summary", article["summary"][:200]),
            "url": article["url"],
            "source": article["source"],
            "published_at": article.get("published"),
            "image_url": article.get("image_url"),
            "event_type": classification.get("event_type", "other"),
            "impact_level": classification.get("impact_level", "low"),
            "affected_symbols": classification.get("affected_symbols", []),
            "sentiment": classification.get("sentiment", "neutral"),
            "keywords": classification.get("keywords", []),
            "is_breaking": classification.get("impact_level") in ["critical", "high"]
        }
        
        sb.table("financial_news").insert(data).execute()
        return True, None
        
    except Exception as e:
        return False, str(e)


def get_news_scan_interval() -> Tuple[int, str]:
    """Get appropriate scan interval based on time."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return NEWS_SCAN_INTERVAL_WEEKEND, "weekend"
    if 8 <= now.hour < 21:
        return NEWS_SCAN_INTERVAL_ACTIVE, "active"
    return NEWS_SCAN_INTERVAL_QUIET, "quiet"


async def financial_news_task():
    """Task 3: Fetch and classify financial news."""
    task_metrics = metrics.get_or_create(NEWS_TASK)
    
    info(NEWS_TASK, f"Financial news task started ({len(RSS_FEEDS)} feeds)")
    
    while not tick_streamer._shutdown.is_set():
        try:
            interval, session_name = get_news_scan_interval()
            info(NEWS_TASK, f"Starting news scan ({session_name} session)")
            
            # Fetch from all feeds
            all_articles = []
            feed_stats = {}
            
            loop = asyncio.get_event_loop()
            for feed in RSS_FEEDS:
                articles, feed_error = await loop.run_in_executor(
                    None, fetch_rss_feed, feed["url"], feed["name"]
                )
                
                feed_stats[feed["name"]] = {
                    "count": len(articles),
                    "error": feed_error
                }
                
                if articles:
                    all_articles.extend(articles)
                elif feed_error:
                    debug(NEWS_TASK, f"  {feed['name']}: ERROR - {feed_error}")
                
                await asyncio.sleep(0.3)
            
            # Deduplicate by URL
            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    unique_articles.append(article)
            
            # Filter by keywords
            filtered_articles = [
                a for a in unique_articles 
                if should_prefilter_article(a["title"], a.get("summary", ""))
            ]
            
            debug(NEWS_TASK, f"Articles: {len(all_articles)} total -> {len(unique_articles)} unique -> {len(filtered_articles)} filtered")
            
            # Classify and store
            stored_count = 0
            classify_errors = 0
            store_errors = 0
            
            for article in filtered_articles[:20]:  # Limit to avoid API costs
                classification, classify_error = await loop.run_in_executor(
                    None, classify_article_with_gemini, article
                )
                
                if classify_error:
                    classify_errors += 1
                    debug(NEWS_TASK, f"  Classify error: {classify_error[:50]}")
                    continue
                
                if not classification:
                    continue
                
                # Only store high-impact news
                if not classification.get("is_important"):
                    continue
                if classification.get("impact_level") not in ["critical", "high"]:
                    continue
                
                success, store_error = await loop.run_in_executor(
                    None, store_news_article, article, classification
                )
                
                if success:
                    stored_count += 1
                else:
                    store_errors += 1
                    debug(NEWS_TASK, f"  Store error: {store_error[:50]}")
                
                await asyncio.sleep(1)  # Rate limit
            
            info(NEWS_TASK, f"News cycle complete: {stored_count} stored, {classify_errors} classify errors, {store_errors} store errors")
            
            task_metrics.record_success()
            task_metrics.custom_metrics.update({
                "last_articles_fetched": len(all_articles),
                "last_articles_filtered": len(filtered_articles),
                "last_stored": stored_count,
                "session": session_name,
            })
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            error(NEWS_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 4: GAP FILL SERVICE
# ============================================================================

GAP_TASK = "GAP_FILL"

PRIORITY_SYMBOLS = ["BTC/USD", "ETH/USD", "EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD", "AAPL", "MSFT", "SPY", "QQQ"]


def symbol_to_table(symbol: str) -> str:
    """Convert symbol to table name."""
    return f"candles_{symbol.lower().replace('/', '_')}"


def detect_gaps(symbol: str, lookback_hours: int = MAX_GAP_HOURS) -> Tuple[List[Tuple], Optional[str]]:
    """Detect gaps in candle data. Returns (gaps, error)."""
    table_name = symbol_to_table(symbol)
    
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        
        result = sb.table(table_name).select("timestamp").gte(
            "timestamp", since
        ).order("timestamp", desc=False).execute()
        
        if not result.data or len(result.data) < 2:
            return [], None
        
        gaps = []
        timestamps = [
            datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) 
            for r in result.data
        ]
        
        for i in range(1, len(timestamps)):
            gap_minutes = (timestamps[i] - timestamps[i-1]).total_seconds() / 60
            if gap_minutes > MIN_GAP_MINUTES:
                gaps.append((
                    timestamps[i-1] + timedelta(minutes=1),
                    timestamps[i] - timedelta(minutes=1)
                ))
        
        return gaps, None
        
    except Exception as e:
        return [], str(e)


async def gap_fill_task():
    """Task 4: Detect and report gaps in candle data."""
    task_metrics = metrics.get_or_create(GAP_TASK)
    
    info(GAP_TASK, f"Gap fill service started ({len(PRIORITY_SYMBOLS)} priority symbols)")
    
    while not tick_streamer._shutdown.is_set():
        try:
            total_gaps = 0
            symbols_with_gaps = []
            
            for symbol in PRIORITY_SYMBOLS:
                gaps, gap_error = detect_gaps(symbol)
                
                if gap_error:
                    debug(GAP_TASK, f"  {symbol}: Error - {gap_error}")
                elif gaps:
                    total_gaps += len(gaps)
                    symbols_with_gaps.append(f"{symbol}({len(gaps)})")
                    debug(GAP_TASK, f"  {symbol}: {len(gaps)} gaps detected")
            
            if symbols_with_gaps:
                info(GAP_TASK, f"Gap scan: {total_gaps} gaps in {', '.join(symbols_with_gaps)}")
            else:
                info(GAP_TASK, "Gap scan: No gaps detected")
            
            task_metrics.record_success()
            task_metrics.custom_metrics["total_gaps"] = total_gaps
            
            await asyncio.sleep(GAP_SCAN_INTERVAL)
            
        except Exception as e:
            error(GAP_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 5: BOND YIELDS
# ============================================================================

BOND_TASK = "BOND_YIELDS"

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


def scrape_bond_yield(url: str) -> Tuple[Optional[float], Optional[str]]:
    """Scrape bond yield from Investing.com. Returns (yield, error)."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        
        soup = BeautifulSoup(response.text, "html.parser")
        price_elem = soup.find("span", {"data-test": "instrument-price-last"})
        
        if price_elem:
            yield_val = float(price_elem.text.strip().replace(",", ""))
            return yield_val, None
        
        return None, "Price element not found"
        
    except Exception as e:
        return None, str(e)


def store_bond_yield(data: Dict) -> Tuple[bool, Optional[str]]:
    """Store bond yield data. Returns (success, error)."""
    try:
        # Try insert first, then update if exists
        sb.table("sovereign_yields").insert(data).execute()
        return True, None
    except Exception as e:
        error_str = str(e).lower()
        if "duplicate" in error_str or "unique" in error_str or "conflict" in error_str:
            # Try update instead
            try:
                sb.table("sovereign_yields").update({
                    "yield_10y": data["yield_10y"],
                    "scraped_at": data["scraped_at"],
                }).eq("country", data["country"]).execute()
                return True, None
            except Exception as e2:
                return False, str(e2)
        return False, str(e)


async def bond_yield_task():
    """Task 5: Scrape and store G20 bond yields."""
    task_metrics = metrics.get_or_create(BOND_TASK)
    
    info(BOND_TASK, f"Bond yield scraper started ({len(G20_BONDS)} countries)")
    
    while not tick_streamer._shutdown.is_set():
        try:
            info(BOND_TASK, "Scraping G20 bond yields...")
            
            success_count = 0
            error_count = 0
            results = []
            
            loop = asyncio.get_event_loop()
            
            for bond in G20_BONDS:
                yield_val, scrape_error = await loop.run_in_executor(
                    None, scrape_bond_yield, bond["url"]
                )
                
                if scrape_error:
                    debug(BOND_TASK, f"  {bond['country']}: Scrape error - {scrape_error}")
                    error_count += 1
                    continue
                
                data = {
                    "country": bond["country"],
                    "country_name": bond["name"],
                    "yield_10y": yield_val,
                    "yield_2y": None,
                    "spread_10y_2y": None,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
                
                stored, store_error = store_bond_yield(data)
                
                if stored:
                    success_count += 1
                    results.append(f"{bond['country']}:{yield_val:.2f}%")
                else:
                    debug(BOND_TASK, f"  {bond['country']}: Store error - {store_error}")
                    error_count += 1
                
                await asyncio.sleep(1)  # Rate limit scraping
            
            info(BOND_TASK, f"Bond scrape complete: {success_count}/{len(G20_BONDS)} countries")
            if DEBUG_MODE and results:
                debug(BOND_TASK, f"  Yields: {', '.join(results[:6])}")
            
            if success_count > 0:
                task_metrics.record_success()
            else:
                task_metrics.record_error("No yields scraped")
            
            task_metrics.custom_metrics.update({
                "last_success_count": success_count,
                "last_error_count": error_count,
            })
            
            await asyncio.sleep(BOND_SCRAPE_INTERVAL)
            
        except Exception as e:
            error(BOND_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 6: WHALE FLOW TRACKER
# ============================================================================

WHALE_TASK = "WHALE_FLOW"
WHALE_THRESHOLD_USD = 500000  # Minimum USD value to be considered a whale
WHALE_PRICE_UPDATE_INTERVAL = 300  # Update BTC price every 5 minutes


class WhaleFlowTracker:
    """Track large BTC transactions via Blockchain.com WebSocket and store to Supabase."""
    
    def __init__(self):
        self._shutdown = asyncio.Event()
        self._btc_price = 100000.0
        self._tx_count = 0
        self._whale_count = 0
        self._last_price_update = 0
        self._metrics = metrics.get_or_create(WHALE_TASK)
        self._daily_stats = {
            "date": None,
            "total_transactions": 0,
            "total_btc": 0.0,
            "total_usd": 0.0,
            "largest_tx_btc": 0.0,
            "largest_tx_usd": 0.0,
        }
    
    def shutdown(self):
        self._shutdown.set()
    
    async def _fetch_btc_price(self):
        """Fetch current BTC price."""
        try:
            # Try CoinGecko first
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                self._btc_price = response.json()["bitcoin"]["usd"]
                self._last_price_update = time.time()
                debug(WHALE_TASK, f"BTC price updated: ${self._btc_price:,.0f}")
                return
        except Exception as e:
            debug(WHALE_TASK, f"CoinGecko error: {e}")
        
        # Fallback to Blockchain.info
        try:
            url = "https://blockchain.info/ticker"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                self._btc_price = response.json()["USD"]["last"]
                self._last_price_update = time.time()
                debug(WHALE_TASK, f"BTC price (fallback): ${self._btc_price:,.0f}")
        except Exception as e:
            debug(WHALE_TASK, f"BTC price fetch error: {e}")
    
    def _store_whale_transaction(self, tx_hash: str, btc_amount: float, usd_value: float,
                                  from_addrs: List[str], to_addrs: List[str]):
        """Store whale transaction to Supabase."""
        try:
            record = {
                "tx_hash": tx_hash,
                "btc_amount": btc_amount,
                "usd_value": usd_value,
                "btc_price": self._btc_price,
                "direction": "unknown",  # Could enhance with exchange detection
                "from_addresses": from_addrs[:5],  # Limit array size
                "to_addresses": to_addrs[:5],
                "is_confirmed": False,
                "detected_at": datetime.now(timezone.utc).isoformat()
            }
            
            sb.table("whale_flows").upsert(record, on_conflict="tx_hash").execute()
            debug(WHALE_TASK, f"Stored whale tx: {tx_hash[:16]}...")
            return True
        except Exception as e:
            debug(WHALE_TASK, f"Failed to store whale tx: {e}")
            return False
    
    def _update_daily_stats(self, btc_amount: float, usd_value: float):
        """Update daily aggregated stats."""
        today = datetime.now(timezone.utc).date().isoformat()
        
        # Reset if new day
        if self._daily_stats["date"] != today:
            self._daily_stats = {
                "date": today,
                "total_transactions": 0,
                "total_btc": 0.0,
                "total_usd": 0.0,
                "largest_tx_btc": 0.0,
                "largest_tx_usd": 0.0,
            }
        
        self._daily_stats["total_transactions"] += 1
        self._daily_stats["total_btc"] += btc_amount
        self._daily_stats["total_usd"] += usd_value
        
        if btc_amount > self._daily_stats["largest_tx_btc"]:
            self._daily_stats["largest_tx_btc"] = btc_amount
            self._daily_stats["largest_tx_usd"] = usd_value
        
        # Store to database every 10 transactions
        if self._daily_stats["total_transactions"] % 10 == 0:
            self._flush_daily_stats()
    
    def _flush_daily_stats(self):
        """Flush daily stats to Supabase."""
        try:
            if not self._daily_stats["date"]:
                return
            
            stats = self._daily_stats
            avg_btc = stats["total_btc"] / stats["total_transactions"] if stats["total_transactions"] > 0 else 0
            avg_usd = stats["total_usd"] / stats["total_transactions"] if stats["total_transactions"] > 0 else 0
            
            record = {
                "date": stats["date"],
                "total_transactions": stats["total_transactions"],
                "total_btc": stats["total_btc"],
                "total_usd": stats["total_usd"],
                "avg_tx_size_btc": avg_btc,
                "avg_tx_size_usd": avg_usd,
                "largest_tx_btc": stats["largest_tx_btc"],
                "largest_tx_usd": stats["largest_tx_usd"],
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            sb.table("whale_stats").upsert(record, on_conflict="date").execute()
            debug(WHALE_TASK, f"Updated daily stats: {stats['total_transactions']} whales today")
        except Exception as e:
            debug(WHALE_TASK, f"Failed to update daily stats: {e}")
    
    async def run(self):
        """Main whale tracking loop."""
        info(WHALE_TASK, "Whale Flow Tracker started")
        info(WHALE_TASK, f"Threshold: ${WHALE_THRESHOLD_USD:,}")
        
        await self._fetch_btc_price()
        
        url = "wss://ws.blockchain.info/inv"
        backoff = 1
        
        while not self._shutdown.is_set():
            try:
                # Update BTC price periodically
                if time.time() - self._last_price_update > WHALE_PRICE_UPDATE_INTERVAL:
                    await self._fetch_btc_price()
                
                debug(WHALE_TASK, "Connecting to Blockchain.com WebSocket...")
                
                async with websockets.connect(url, ping_interval=30) as ws:
                    await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                    info(WHALE_TASK, "Subscribed to BTC unconfirmed transactions")
                    backoff = 1
                    
                    async for message in ws:
                        if self._shutdown.is_set():
                            break
                        
                        try:
                            data = json.loads(message)
                            
                            if data.get("op") == "utx":
                                self._tx_count += 1
                                tx = data.get("x", {})
                                tx_hash = tx.get("hash", "")
                                
                                # Calculate total output value
                                outputs = tx.get("out", [])
                                inputs = tx.get("inputs", [])
                                
                                total_btc = sum(o.get("value", 0) for o in outputs) / 1e8
                                total_usd = total_btc * self._btc_price
                                
                                # Extract addresses
                                from_addrs = [i.get("prev_out", {}).get("addr", "") for i in inputs if i.get("prev_out", {}).get("addr")]
                                to_addrs = [o.get("addr", "") for o in outputs if o.get("addr")]
                                
                                if total_usd >= WHALE_THRESHOLD_USD:
                                    self._whale_count += 1
                                    info(WHALE_TASK, f"🐋 WHALE: {total_btc:.2f} BTC (${total_usd:,.0f})")
                                    
                                    # Store to database
                                    self._store_whale_transaction(
                                        tx_hash, total_btc, total_usd, from_addrs, to_addrs
                                    )
                                    
                                    # Update daily stats
                                    self._update_daily_stats(total_btc, total_usd)
                                    
                                    self._metrics.record_success()
                                
                                # Update price periodically during message processing
                                if time.time() - self._last_price_update > WHALE_PRICE_UPDATE_INTERVAL:
                                    await self._fetch_btc_price()
                                    
                        except Exception as e:
                            debug(WHALE_TASK, f"Message parse error: {e}")
                            
            except Exception as e:
                warning(WHALE_TASK, f"WebSocket error: {e}")
                self._metrics.record_error(str(e))
            
            if self._shutdown.is_set():
                break
            
            await asyncio.sleep(backoff)
            backoff = min(30, backoff * 2)
        
        # Flush final stats on shutdown
        self._flush_daily_stats()
        info(WHALE_TASK, f"Whale tracker stopped. Total: {self._tx_count} txs, {self._whale_count} whales")


whale_tracker = WhaleFlowTracker()


async def whale_flow_task():
    """Task 6: Track whale BTC movements."""
    await whale_tracker.run()


# ============================================================================
#                    TASK 7: AI NEWS DESK (Placeholder)
# ============================================================================

AI_NEWS_TASK = "AI_NEWS_DESK"


async def ai_news_desk_task():
    """Task 7: AI news synthesis (placeholder)."""
    task_metrics = metrics.get_or_create(AI_NEWS_TASK)
    
    info(AI_NEWS_TASK, "AI News Desk started (placeholder)")
    
    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            
            if now.weekday() >= 5:
                await asyncio.sleep(14400)
                continue
            
            interval = 1800 if 8 <= now.hour < 21 else 3600
            task_metrics.record_success()
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            error(AI_NEWS_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 8: G20 MACRO FETCHER
# ============================================================================

MACRO_TASK = "MACRO_FETCHER"

G20_MACRO_COUNTRIES = {
    "US": {"name": "United States", "currency": "USD"},
    "UK": {"name": "United Kingdom", "currency": "GBP"},
    "DE": {"name": "Germany", "currency": "EUR"},
    "FR": {"name": "France", "currency": "EUR"},
    "IT": {"name": "Italy", "currency": "EUR"},
    "JP": {"name": "Japan", "currency": "JPY"},
    "CN": {"name": "China", "currency": "CNY"},
    "IN": {"name": "India", "currency": "INR"},
    "BR": {"name": "Brazil", "currency": "BRL"},
    "RU": {"name": "Russia", "currency": "RUB"},
    "CA": {"name": "Canada", "currency": "CAD"},
    "AU": {"name": "Australia", "currency": "AUD"},
    "MX": {"name": "Mexico", "currency": "MXN"},
    "KR": {"name": "South Korea", "currency": "KRW"},
    "ID": {"name": "Indonesia", "currency": "IDR"},
    "TR": {"name": "Turkey", "currency": "TRY"},
    "SA": {"name": "Saudi Arabia", "currency": "SAR"},
    "ZA": {"name": "South Africa", "currency": "ZAR"},
    "AR": {"name": "Argentina", "currency": "ARS"},
    "EU": {"name": "European Union", "currency": "EUR"},
}

MACRO_INDICATORS = {
    "CPI YoY": ["Inflation Rate", "CPI", "Consumer Price Index"],
    "GDP QoQ": ["GDP Growth Rate", "Gross Domestic Product"],
    "Unemployment": ["Unemployment Rate", "Jobless Rate"],
    "Interest Rate": ["Interest Rate Decision"],
    "Manufacturing PMI": ["ISM Manufacturing PMI", "Manufacturing PMI"],
    "Retail Sales MoM": ["Retail Sales"],
}


def fetch_country_macro_events(country_code: str) -> Tuple[List[Dict], Optional[str]]:
    """Fetch macro events for a country. Returns (events, error)."""
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    
    url = "https://eodhd.com/api/economic-events"
    params = {
        "api_token": EODHD_API_KEY,
        "country": country_code,
        "from": from_date,
        "to": to_date,
        "limit": 500,
        "fmt": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json(), None
        return [], f"HTTP {response.status_code}"
    except Exception as e:
        return [], str(e)


def upsert_macro_indicator(data: Dict) -> Tuple[bool, Optional[str]]:
    """Upsert macro indicator. Returns (success, error)."""
    try:
        sb.table("macro_indicators").upsert(
            data,
            on_conflict="country_code,indicator"
        ).execute()
        return True, None
    except Exception as e:
        return False, str(e)


async def macro_fetcher_task():
    """Task 8: Fetch G20 macro indicators."""
    task_metrics = metrics.get_or_create(MACRO_TASK)
    
    info(MACRO_TASK, f"G20 Macro Fetcher started ({len(G20_MACRO_COUNTRIES)} countries)")
    await asyncio.sleep(30)  # Stagger startup
    
    while not tick_streamer._shutdown.is_set():
        try:
            info(MACRO_TASK, "Starting macro fetch cycle...")
            
            total_updated = 0
            total_errors = 0
            
            loop = asyncio.get_event_loop()
            
            for country_code, country_info in G20_MACRO_COUNTRIES.items():
                events, fetch_error = await loop.run_in_executor(
                    None, fetch_country_macro_events, country_code
                )
                
                if fetch_error:
                    debug(MACRO_TASK, f"  {country_code}: Fetch error - {fetch_error}")
                    total_errors += 1
                    continue
                
                if not events:
                    continue
                
                # Process most recent events for each indicator type
                for indicator_name, keywords in MACRO_INDICATORS.items():
                    for event in events:
                        event_type = event.get("type", "").lower()
                        
                        if any(kw.lower() in event_type for kw in keywords):
                            actual = event.get("actual")
                            if actual is None:
                                continue
                            
                            data = {
                                "country_code": country_code,
                                "country_name": country_info["name"],
                                "currency": country_info["currency"],
                                "indicator": indicator_name,
                                "event_type": event.get("type"),
                                "actual": float(actual) if actual else None,
                                "previous": float(event.get("previous")) if event.get("previous") else None,
                                "estimate": float(event.get("estimate")) if event.get("estimate") else None,
                                "event_date": event.get("date", "").split(" ")[0],
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                            }
                            
                            success, upsert_error = upsert_macro_indicator(data)
                            if success:
                                total_updated += 1
                            else:
                                total_errors += 1
                            break
                
                await asyncio.sleep(0.2)  # Rate limit
            
            info(MACRO_TASK, f"Macro cycle complete: {total_updated} updated, {total_errors} errors")
            
            task_metrics.record_success()
            task_metrics.custom_metrics.update({
                "last_updated": total_updated,
                "last_errors": total_errors,
            })
            
            await asyncio.sleep(MACRO_FETCH_INTERVAL)
            
        except Exception as e:
            error(MACRO_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(300)


# ============================================================================
#                    TASK 9: INDEX POLLER
# ============================================================================

INDEX_TASK = "INDEX_POLLER"

# Track minute-level OHLC in memory
_index_minute_ohlc: Dict[str, Dict[str, Dict[str, float]]] = {}
_index_ohlc_lock = threading.Lock()


def update_minute_ohlc(symbol: str, minute_ts: str, price: float) -> Dict[str, float]:
    """Track minute-level OHLC since API only provides daily values."""
    with _index_ohlc_lock:
        if symbol not in _index_minute_ohlc:
            _index_minute_ohlc[symbol] = {}
        
        candles = _index_minute_ohlc[symbol]
        
        # Cleanup old minutes
        if len(candles) > 10:
            sorted_minutes = sorted(candles.keys())
            for old_minute in sorted_minutes[:-5]:
                del candles[old_minute]
        
        if minute_ts not in candles:
            candles[minute_ts] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price
            }
        else:
            c = candles[minute_ts]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
        
        return candles[minute_ts].copy()


def fetch_index_realtime(eodhd_symbol: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch real-time index quote. Returns (data, error)."""
    url = f"https://eodhd.com/api/real-time/{eodhd_symbol}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and data.get("close") and data.get("close") != "NA":
                return data, None
            return None, "Invalid data"
        return None, f"HTTP {response.status_code}"
        
    except Exception as e:
        return None, str(e)


def fetch_all_indices_parallel() -> Dict[str, Tuple[Optional[Dict], Optional[str]]]:
    """Fetch all indices in parallel."""
    results = {}
    
    with ThreadPoolExecutor(max_workers=11) as executor:
        futures = {
            executor.submit(fetch_index_realtime, sym): sym 
            for sym in INDEX_SYMBOLS.keys()
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data, err = future.result()
                results[symbol] = (data, err)
            except Exception as e:
                results[symbol] = (None, str(e))
    
    return results


def run_index_poll_cycle() -> Tuple[int, int, List[str]]:
    """Run one index polling cycle. Returns (updated, errors, error_messages)."""
    results = fetch_all_indices_parallel()
    updated = 0
    error_count = 0
    error_messages = []
    
    now = datetime.now(timezone.utc)
    minute_ts = now.replace(second=0, microsecond=0).isoformat()
    
    for eodhd_symbol, (display_symbol, candle_table) in INDEX_SYMBOLS.items():
        data, fetch_error = results.get(eodhd_symbol, (None, "Not fetched"))
        
        if fetch_error or not data:
            error_count += 1
            if fetch_error:
                error_messages.append(f"{display_symbol}: {fetch_error}")
            continue
        
        try:
            price = float(data["close"])
            minute_ohlc = update_minute_ohlc(display_symbol, minute_ts, price)
            
            # Insert to tickdata (use 'volume' column)
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
                error_messages.append(f"{display_symbol} tickdata: {str(e)[:50]}")
            
            # Upsert candle
            try:
                candle = {
                    "timestamp": minute_ts,
                    "open": minute_ohlc["open"],
                    "high": minute_ohlc["high"],
                    "low": minute_ohlc["low"],
                    "close": minute_ohlc["close"],
                    "volume": None,
                }
                sb.table(candle_table).upsert(candle, on_conflict="timestamp").execute()
            except Exception as e:
                error_messages.append(f"{display_symbol} candle: {str(e)[:50]}")
            
            updated += 1
            
        except Exception as e:
            error_count += 1
            error_messages.append(f"{display_symbol}: {str(e)[:50]}")
    
    return updated, error_count, error_messages


async def index_poller_task():
    """Task 9: Poll global indices via REST API."""
    task_metrics = metrics.get_or_create(INDEX_TASK)
    
    info(INDEX_TASK, f"Index Poller started ({len(INDEX_SYMBOLS)} indices, every {INDEX_POLL_INTERVAL}s)")
    await asyncio.sleep(5)
    
    poll_count = 0
    total_updates = 0
    
    while not tick_streamer._shutdown.is_set():
        try:
            poll_count += 1
            
            loop = asyncio.get_event_loop()
            updated, errors, error_messages = await loop.run_in_executor(
                None, run_index_poll_cycle
            )
            
            total_updates += updated
            
            # Log progress
            if poll_count <= 10 or poll_count % 30 == 0:
                info(INDEX_TASK, f"Poll #{poll_count}: {updated}/{len(INDEX_SYMBOLS)} updated, {errors} errors")
            
            # Log errors in debug mode
            if DEBUG_MODE and error_messages:
                for msg in error_messages[:5]:
                    debug(INDEX_TASK, f"  {msg}")
            
            if updated > 0:
                task_metrics.record_success()
            elif errors > 0:
                task_metrics.record_error(f"{errors} fetch errors")
            
            task_metrics.custom_metrics.update({
                "poll_count": poll_count,
                "total_updates": total_updates,
                "last_updated": updated,
                "last_errors": errors,
            })
            
            await asyncio.sleep(INDEX_POLL_INTERVAL)
            
        except Exception as e:
            error(INDEX_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 10: SECTOR SENTIMENT ENGINE
# ============================================================================
# AI-powered market sentiment analysis with Gemini explanations
#
# UPDATE SCHEDULE (UTC - all at :05 past the hour):
# - London (07:00-12:00): Every 2 hours → 07:05, 09:05, 11:05
# - US/London Overlap (12:00-16:00): Every 1 hour → 12:05, 13:05, 14:05, 15:05
# - US Session (16:00-21:00): Every 2 hours → 16:05, 18:05, 20:05
# - Off-Hours (21:00-07:00): Every 4 hours → 21:05, 01:05, 05:05
# - Weekend: Every 12 hours → 07:05, 19:05
# ============================================================================

SENTIMENT_TASK = "SENTIMENT"

SENTIMENT_SECTORS = {
    "tech": {
        "name": "Technology",
        "symbols": ["AAPL.US", "MSFT.US", "NVDA.US", "META.US", "GOOGL.US", "AMZN.US", "TSM.US", "AVGO.US"],
        "tags": ["technology", "ai", "artificial intelligence"],
        "weight": 0.30,
        "emoji": "💻",
        "color": "#3B82F6"
    },
    "crypto": {
        "name": "Cryptocurrency",
        "symbols": ["BTC-USD.CC", "ETH-USD.CC", "SOL-USD.CC", "XRP-USD.CC", "ADA-USD.CC", "BNB-USD.CC"],
        "tags": ["crypto", "bitcoin", "ethereum", "blockchain"],
        "weight": 0.20,
        "emoji": "₿",
        "color": "#F59E0B"
    },
    "commodities": {
        "name": "Commodities",
        "symbols": [],
        "tags": ["oil", "gold", "silver", "commodities", "natural gas", "copper"],
        "weight": 0.20,
        "emoji": "🛢️",
        "color": "#10B981"
    },
    "us_market": {
        "name": "US Market",
        "symbols": ["GSPC.INDX", "DJI.INDX", "IXIC.INDX"],
        "tags": ["economy", "fed", "federal reserve", "interest rate", "inflation", "earnings"],
        "weight": 0.30,
        "emoji": "🇺🇸",
        "color": "#EF4444"
    }
}

SENTIMENT_SCHEDULE = {
    "london": {"hours": range(7, 12), "times": ["07:05", "09:05", "11:05"]},
    "overlap": {"hours": range(12, 16), "times": ["12:05", "13:05", "14:05", "15:05"]},
    "us": {"hours": range(16, 21), "times": ["16:05", "18:05", "20:05"]},
    "off_hours": {"hours": list(range(21, 24)) + list(range(0, 7)), "times": ["21:05", "01:05", "05:05"]},
    "weekend": {"times": ["07:05", "19:05"]}
}


def get_sentiment_session() -> str:
    """Determine current trading session."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return "weekend"
    hour = now.hour
    if 7 <= hour < 12:
        return "london"
    elif 12 <= hour < 16:
        return "overlap"
    elif 16 <= hour < 21:
        return "us"
    return "off_hours"


def get_next_sentiment_update() -> Tuple[datetime, str]:
    """Calculate next scheduled sentiment update time."""
    now = datetime.now(timezone.utc)
    session = get_sentiment_session()
    schedule = SENTIMENT_SCHEDULE[session]
    
    for time_str in schedule["times"]:
        hour, minute = map(int, time_str.split(":"))
        scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if scheduled > now:
            return scheduled, session
    
    tomorrow = now + timedelta(days=1)
    next_time = "07:05"
    if tomorrow.weekday() >= 5:
        next_time = SENTIMENT_SCHEDULE["weekend"]["times"][0]
    
    hour, minute = map(int, next_time.split(":"))
    return tomorrow.replace(hour=hour, minute=minute, second=0, microsecond=0), session


def should_run_sentiment_update() -> bool:
    """Check if we should run sentiment update now."""
    now = datetime.now(timezone.utc)
    session = get_sentiment_session()
    schedule = SENTIMENT_SCHEDULE[session]
    
    for time_str in schedule["times"]:
        hour, minute = map(int, time_str.split(":"))
        scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if abs((now - scheduled).total_seconds()) <= 120:
            return True
    return False


def sentiment_transform_score(raw_score: float) -> int:
    """Transform raw sentiment (-1 to +1) to final score (-95 to +95)."""
    import math
    raw_score = max(-1.0, min(1.0, raw_score))
    compressed = math.tanh(1.5 * raw_score)
    
    if compressed >= 0:
        scaled = math.pow(compressed, 0.7)
        final = scaled * 95
    else:
        scaled = -math.pow(abs(compressed), 0.7)
        final = scaled * 95
    
    if abs(final) > 85:
        excess = abs(final) - 85
        dampened_excess = 10 * math.log10(1 + excess / 10)
        final = math.copysign(85 + dampened_excess, final)
    
    return int(max(-95, min(95, final)))


def sentiment_get_label(score: int) -> str:
    """Get sentiment label from score."""
    if score <= -85:
        return "EXTREME FEAR"
    elif score <= -60:
        return "VERY BEARISH"
    elif score <= -35:
        return "BEARISH"
    elif score <= -15:
        return "CAUTIOUS"
    elif score <= 15:
        return "NEUTRAL"
    elif score <= 35:
        return "OPTIMISTIC"
    elif score <= 60:
        return "BULLISH"
    elif score <= 85:
        return "VERY BULLISH"
    return "EXTREME GREED"


def fetch_eodhd_sentiment(symbol: str, days_back: int = 3) -> List[Dict]:
    """Fetch daily aggregated sentiment from EODHD."""
    try:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        r = requests.get(
            "https://eodhd.com/api/sentiments",
            params={"s": symbol, "from": from_date, "to": to_date, "api_token": EODHD_API_KEY, "fmt": "json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            return data.get(symbol, [])
        return []
    except Exception as e:
        debug(SENTIMENT_TASK, f"Error fetching sentiment for {symbol}: {e}")
        return []


def fetch_eodhd_news_by_symbol(symbol: str, limit: int = 15) -> List[Dict]:
    """Fetch news articles by symbol."""
    try:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        
        r = requests.get(
            "https://eodhd.com/api/news",
            params={"s": symbol, "from": from_date, "to": to_date, "limit": limit, "api_token": EODHD_API_KEY, "fmt": "json"},
            timeout=30
        )
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        debug(SENTIMENT_TASK, f"Error fetching news for {symbol}: {e}")
        return []


def fetch_eodhd_news_by_tag(tag: str, limit: int = 20) -> List[Dict]:
    """Fetch news articles by topic tag."""
    try:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        
        r = requests.get(
            "https://eodhd.com/api/news",
            params={"t": tag, "from": from_date, "to": to_date, "limit": limit, "api_token": EODHD_API_KEY, "fmt": "json"},
            timeout=30
        )
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        debug(SENTIMENT_TASK, f"Error fetching news for tag {tag}: {e}")
        return []


def generate_sentiment_explanation(sector_name: str, score: int, label: str, 
                                   article_count: int, top_articles: List[Dict]) -> str:
    """Generate AI explanation using Gemini."""
    if not GOOGLE_GEMINI_API_KEY:
        return f"{sector_name} sentiment is {label.lower()} based on {article_count} articles analyzed."
    
    headlines = "\n".join([f"- {a.get('title', '')[:120]}" for a in top_articles[:10]])
    
    prompt = f"""You are a senior financial analyst writing a market intelligence briefing.

SECTOR: {sector_name}
SENTIMENT SCORE: {score:+d} (scale: -95 to +95)
SENTIMENT LABEL: {label}
ARTICLES ANALYZED: {article_count}

TOP HEADLINES:
{headlines}

Write a 4-5 sentence analysis explaining the {sector_name} sentiment:
1. State the overall sentiment and primary driver (name specific companies/events)
2. Explain a supporting factor with concrete details
3. Mention any counterbalancing risks
4. What this means for traders/investors
5. Any upcoming catalysts to watch

Be specific with company names and events. Professional tone. No bullet points. 80-120 words."""

    try:
        response = requests.post(
            f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.4, "maxOutputTokens": 300}
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        debug(SENTIMENT_TASK, f"Gemini error: {e}")
    
    return f"{sector_name} sentiment is {label.lower()} based on {article_count} articles."


def generate_overall_sentiment_explanation(overall_score: int, overall_label: str, 
                                           sector_data: Dict[str, Dict]) -> str:
    """Generate overall market explanation using Gemini."""
    if not GOOGLE_GEMINI_API_KEY:
        return f"Overall market sentiment is {overall_label.lower()}."
    
    sector_summary = "\n".join([
        f"- {s['name']}: {s['score']:+d} ({s['label']})"
        for sid, s in sector_data.items() if sid != 'overall'
    ])
    
    prompt = f"""You are a senior market strategist writing a daily market intelligence summary.

OVERALL SCORE: {overall_score:+d} (scale: -95 to +95)
OVERALL LABEL: {overall_label}

SECTOR BREAKDOWN:
{sector_summary}

Write a 4-5 sentence executive summary:
1. Declare overall market mood and dominant theme
2. Which sectors are leading and why
3. Note any sector divergences
4. What this suggests for market direction
5. Key risk or catalyst to monitor

Professional tone for institutional investors. No bullet points. 90-130 words."""

    try:
        response = requests.post(
            f"{GEMINI_API_URL}?key={GOOGLE_GEMINI_API_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.4, "maxOutputTokens": 350}
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        debug(SENTIMENT_TASK, f"Gemini error: {e}")
    
    return f"Market sentiment is {overall_label.lower()} across major sectors."


def extract_sentiment_key_drivers(articles: List[Dict], top_n: int = 5) -> List[str]:
    """Extract key drivers from article titles."""
    if not articles:
        return []
    
    sorted_articles = sorted(
        articles,
        key=lambda a: abs(a.get("sentiment", {}).get("pos", 0) - a.get("sentiment", {}).get("neg", 0)),
        reverse=True
    )
    
    drivers = []
    for article in sorted_articles[:top_n]:
        title = article.get("title", "")[:80]
        sentiment = article.get("sentiment", {})
        pos = sentiment.get("pos", 0)
        neg = sentiment.get("neg", 0)
        direction = "📈" if pos > neg else "📉" if neg > pos else "➡️"
        drivers.append(f"{direction} {title}")
    
    return drivers


def analyze_sentiment_sector(sector_id: str, sector_config: Dict) -> Dict:
    """Analyze sentiment for a single sector."""
    debug(SENTIMENT_TASK, f"Analyzing {sector_config['name']}...")
    
    all_articles = []
    sentiment_scores = []
    
    for symbol in sector_config.get("symbols", []):
        daily_sent = fetch_eodhd_sentiment(symbol, days_back=3)
        if daily_sent:
            latest = daily_sent[0]
            normalized = latest.get("normalized", 0)
            count = latest.get("count", 0)
            sentiment_scores.append((normalized, count))
            debug(SENTIMENT_TASK, f"  {symbol}: {normalized:.3f} ({count} articles)")
        
        articles = fetch_eodhd_news_by_symbol(symbol, limit=15)
        all_articles.extend(articles)
    
    for tag in sector_config.get("tags", []):
        articles = fetch_eodhd_news_by_tag(tag, limit=20)
        all_articles.extend(articles)
    
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article.get("link", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)
    
    debug(SENTIMENT_TASK, f"  Total unique articles: {len(unique_articles)}")
    
    if sentiment_scores:
        total_weight = sum(count for _, count in sentiment_scores)
        if total_weight > 0:
            raw_score = sum(score * count for score, count in sentiment_scores) / total_weight
        else:
            raw_score = sum(score for score, _ in sentiment_scores) / len(sentiment_scores)
    elif unique_articles:
        scores = []
        for article in unique_articles:
            sent = article.get("sentiment", {})
            if sent:
                pos = sent.get("pos", 0)
                neg = sent.get("neg", 0)
                polarity = sent.get("polarity", 0.5)
                scores.append((pos - neg) * polarity)
        raw_score = sum(scores) / len(scores) if scores else 0
    else:
        raw_score = 0
    
    final_score = sentiment_transform_score(raw_score)
    label = sentiment_get_label(final_score)
    confidence = min(1.0, len(unique_articles) / 50)
    key_drivers = extract_sentiment_key_drivers(unique_articles, top_n=5)
    
    explanation = generate_sentiment_explanation(
        sector_config["name"], final_score, label, len(unique_articles), unique_articles[:10]
    )
    
    return {
        "sector_id": sector_id,
        "name": sector_config["name"],
        "emoji": sector_config["emoji"],
        "color": sector_config["color"],
        "score": final_score,
        "raw_score": round(raw_score, 4),
        "label": label,
        "confidence": round(confidence, 2),
        "explanation": explanation,
        "key_drivers": key_drivers,
        "article_count": len(unique_articles),
        "top_articles": [
            {
                "title": a.get("title", "")[:100],
                "url": a.get("link", ""),
                "sentiment": {"pos": a.get("sentiment", {}).get("pos", 0), "neg": a.get("sentiment", {}).get("neg", 0)}
            }
            for a in unique_articles[:10]
        ]
    }


def store_sentiment_to_supabase(sector_data: Dict, session: str) -> bool:
    """Store sentiment data in Supabase."""
    try:
        record = {
            "sector_id": sector_data["sector_id"],
            "name": sector_data["name"],
            "emoji": sector_data["emoji"],
            "color": sector_data["color"],
            "score": sector_data["score"],
            "raw_score": sector_data["raw_score"],
            "label": sector_data["label"],
            "confidence": sector_data["confidence"],
            "explanation": sector_data["explanation"],
            "key_drivers": sector_data["key_drivers"],
            "article_count": sector_data["article_count"],
            "top_articles": sector_data["top_articles"],
            "session": session,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        sb.table("sector_sentiment").upsert(record, on_conflict="sector_id").execute()
        
        history_record = {
            "sector_id": sector_data["sector_id"],
            "score": sector_data["score"],
            "raw_score": sector_data["raw_score"],
            "label": sector_data["label"],
            "session": session
        }
        sb.table("sector_sentiment_history").insert(history_record).execute()
        
        return True
    except Exception as e:
        error(SENTIMENT_TASK, f"Failed to store {sector_data['sector_id']}: {e}")
        return False


async def sector_sentiment_task():
    """Task 10: Sector Sentiment Engine with AI analysis."""
    task_metrics = metrics.get_or_create(SENTIMENT_TASK)
    
    info(SENTIMENT_TASK, f"Sector Sentiment Engine started ({len(SENTIMENT_SECTORS)} sectors)")
    
    last_run_key = None
    
    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            session = get_sentiment_session()
            
            if should_run_sentiment_update():
                current_key = f"{now.hour}:{now.minute // 5 * 5}"
                if last_run_key == current_key:
                    await asyncio.sleep(60)
                    continue
                last_run_key = current_key
                
                info(SENTIMENT_TASK, f"Starting sentiment analysis ({session} session)")
                
                sector_results = {}
                loop = asyncio.get_event_loop()
                
                for sector_id, sector_config in SENTIMENT_SECTORS.items():
                    result = await loop.run_in_executor(None, analyze_sentiment_sector, sector_id, sector_config)
                    sector_results[sector_id] = result
                    info(SENTIMENT_TASK, f"  {result['name']}: {result['score']:+d} ({result['label']})")
                    await asyncio.sleep(0.5)
                
                total_weight = sum(SENTIMENT_SECTORS[s]["weight"] for s in sector_results)
                overall_raw = sum(
                    sector_results[s]["raw_score"] * SENTIMENT_SECTORS[s]["weight"]
                    for s in sector_results
                ) / total_weight
                
                overall_score = sentiment_transform_score(overall_raw)
                overall_label = sentiment_get_label(overall_score)
                total_articles = sum(s["article_count"] for s in sector_results.values())
                
                overall_explanation = await loop.run_in_executor(
                    None, generate_overall_sentiment_explanation, overall_score, overall_label, sector_results
                )
                
                overall_data = {
                    "sector_id": "overall",
                    "name": "Overall Market",
                    "emoji": "🌍",
                    "color": "#6366F1",
                    "score": overall_score,
                    "raw_score": round(overall_raw, 4),
                    "label": overall_label,
                    "confidence": 0.85,
                    "explanation": overall_explanation,
                    "key_drivers": [],
                    "article_count": total_articles,
                    "top_articles": []
                }
                
                info(SENTIMENT_TASK, f"  Overall: {overall_score:+d} ({overall_label})")
                
                stored = 0
                for sector_id, data in sector_results.items():
                    if store_sentiment_to_supabase(data, session):
                        stored += 1
                
                if store_sentiment_to_supabase(overall_data, session):
                    stored += 1
                
                info(SENTIMENT_TASK, f"Complete: {stored}/{len(sector_results) + 1} sectors stored")
                
                next_update, _ = get_next_sentiment_update()
                info(SENTIMENT_TASK, f"Next update: {next_update.strftime('%H:%M')} UTC")
                
                task_metrics.record_success()
                task_metrics.custom_metrics.update({
                    "last_overall_score": overall_score,
                    "last_session": session,
                    "sectors_analyzed": len(sector_results),
                    "total_articles": total_articles
                })
            
            await asyncio.sleep(30)
            
        except Exception as e:
            error(SENTIMENT_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    TASK 11: NEWS ARTICLES SCRAPER
# ============================================================================
# Scrapes financial news from multiple RSS sources with content filtering
# Updates: Every 15 min during market hours, every 30 min off-hours, every 2 hours weekend
# ============================================================================

NEWS_ARTICLES_TASK = "NEWS_ARTICLES"

# News sources configuration
NEWS_SOURCES_CONFIG = {
    "yahoo_finance": {
        "name": "Yahoo Finance",
        "logo": "https://s.yimg.com/cv/apiv2/default/20190501/yahoo_finance_en-US_2x.png",
        "color": "#6001D2",
        "rss_feeds": ["https://finance.yahoo.com/news/rssindex"],
        "categories": ["markets", "stocks", "economy"],
    },
    "marketwatch": {
        "name": "MarketWatch",
        "logo": "https://mw3.wsj.net/mw5/content/logos/mw_logo_social.png",
        "color": "#00AC4E",
        "rss_feeds": [
            "http://feeds.marketwatch.com/marketwatch/topstories/",
            "http://feeds.marketwatch.com/marketwatch/marketpulse/",
        ],
        "categories": ["markets", "stocks"],
    },
    "cnbc": {
        "name": "CNBC",
        "logo": "https://sc.cnbcfm.com/applications/cnbc.com/staticcontent/img/cnbc_logo.gif",
        "color": "#005594",
        "rss_feeds": [
            "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
            "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
        ],
        "categories": ["markets", "stocks", "economy"],
    },
    "coindesk": {
        "name": "CoinDesk",
        "logo": "https://www.coindesk.com/resizer/V5yJVoHqWN6hPBs1nB_mzVmBDjk=/800x600/cloudfront-us-east-1.images.arcpublishing.com/coindesk/ZJKJPH5BVBHFNMU32B6PJVGSUY.png",
        "color": "#0033FF",
        "rss_feeds": ["https://www.coindesk.com/arc/outboundfeeds/rss/"],
        "categories": ["crypto", "bitcoin", "ethereum"],
    },
    "cointelegraph": {
        "name": "CoinTelegraph",
        "logo": "https://cointelegraph.com/assets/img/logo.svg",
        "color": "#000000",
        "rss_feeds": ["https://cointelegraph.com/rss"],
        "categories": ["crypto", "blockchain", "defi"],
    },
    "benzinga": {
        "name": "Benzinga",
        "logo": "https://www.benzinga.com/next-assets/images/schema-image.png",
        "color": "#00C805",
        "rss_feeds": ["https://www.benzinga.com/feed"],
        "categories": ["stocks", "markets", "trading"],
    }
}

# Content filtering - reject these
NEWS_REJECT_KEYWORDS = [
    "palestinian", "bethlehem", "gaza", "israel conflict", "christmas",
    "holiday gift", "thanksgiving", "family gathering",
    "investor news: if you have suffered", "class action", "lawsuit alert",
    "securities fraud", "investor alert", "legal action",
    "how to talk to your family", "gift guide",
    "shibarium", "shib explorer", "meme coin", "pump and dump",
    "airdrop alert", "free tokens", "moonshot",
    "geekstake", "sponsored content", "partner content",
    "press release:", "prn newswire", "business wire",
]

# Priority keywords - boost these
NEWS_PRIORITY_KEYWORDS = [
    "breaking:", "just in:", "alert:", "developing:",
    "earnings", "revenue", "profit", "quarterly results",
    "beats estimates", "misses estimates", "guidance",
    "surges", "plunges", "rallies", "tumbles", "soars",
    "all-time high", "52-week", "record high", "record low",
    "fed", "federal reserve", "rate decision", "rate hike", "rate cut",
    "inflation", "cpi", "jobs report", "unemployment", "gdp",
    "ipo", "merger", "acquisition", "buyout",
    "upgrade", "downgrade", "price target", "initiates coverage",
    "bitcoin", "btc", "ethereum", "eth", "sec crypto", "etf approval",
]


def news_should_reject(title: str, summary: str) -> bool:
    """Check if article should be rejected."""
    text = f"{title} {summary}".lower()
    for keyword in NEWS_REJECT_KEYWORDS:
        if keyword.lower() in text:
            return True
    if "investor" in text and ("loss" in text or "suffered" in text):
        return True
    return False


def news_calculate_priority(title: str, summary: str, symbols: List[str]) -> int:
    """Calculate priority score (0-100)."""
    text = f"{title} {summary}".lower()
    score = 50
    
    for keyword in NEWS_PRIORITY_KEYWORDS:
        if keyword.lower() in text:
            score += 5
    
    score += len(symbols) * 3
    
    if any(x in title.lower() for x in ["breaking", "just in", "alert"]):
        score += 15
    if any(x in text for x in ["earnings", "quarterly", "revenue beat"]):
        score += 10
    if any(x in text for x in ["federal reserve", "fed ", "rate decision"]):
        score += 10
    if any(x in title.lower() for x in ["my favorite", "could", "should you"]):
        score -= 10
    if any(x in title.lower() for x in ["top 10", "5 stocks", "3 reasons"]):
        score -= 5
    
    return min(100, max(0, score))


def news_detect_symbols(text: str) -> List[str]:
    """Detect stock symbols in text."""
    import re
    patterns = [r'\$([A-Z]{1,5})\b', r'\(([A-Z]{1,5})\)', r'\b([A-Z]{2,5})(?::\s)']
    symbols = set()
    for pattern in patterns:
        symbols.update(re.findall(pattern, text))
    false_positives = {'CEO', 'CFO', 'IPO', 'ETF', 'NYSE', 'SEC', 'GDP', 'CPI', 'USD', 'EUR', 'THE', 'FOR', 'AND'}
    return [s for s in symbols if s not in false_positives][:5]


def news_clean_html(text: str) -> str:
    """Remove HTML tags."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ")
    import re
    return re.sub(r'\s+', ' ', text).strip()[:500]


def news_extract_image(entry) -> Optional[str]:
    """Extract image URL from RSS entry."""
    if hasattr(entry, 'media_content') and entry.media_content:
        for media in entry.media_content:
            if media.get('url'):
                return media.get('url')
    if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
        for thumb in entry.media_thumbnail:
            if thumb.get('url'):
                return thumb.get('url')
    if hasattr(entry, 'enclosures') and entry.enclosures:
        for enc in entry.enclosures:
            if enc.get('type', '').startswith('image'):
                return enc.get('url')
    return None


def news_parse_date(entry) -> str:
    """Parse publication date."""
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
        except:
            pass
    if hasattr(entry, 'updated_parsed') and entry.updated_parsed:
        try:
            return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc).isoformat()
        except:
            pass
    return datetime.now(timezone.utc).isoformat()


def scrape_news_source(source_id: str, source_config: dict) -> List[Dict]:
    """Scrape a single news source."""
    import feedparser
    import hashlib
    
    articles = []
    
    for feed_url in source_config.get("rss_feeds", []):
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:15]:
                url = entry.get('link', '')
                if not url:
                    continue
                
                title = news_clean_html(entry.get('title', ''))
                if not title:
                    continue
                
                summary = news_clean_html(entry.get('summary', entry.get('description', '')))
                if not summary:
                    summary = title
                
                # Filter
                if news_should_reject(title, summary):
                    continue
                
                symbols = news_detect_symbols(f"{title} {summary}")
                priority = news_calculate_priority(title, summary, symbols)
                
                if priority < 30:
                    continue
                
                article = {
                    "id": hashlib.md5(url.encode()).hexdigest()[:16],
                    "title": title[:200],
                    "summary": summary[:400],
                    "url": url,
                    "source_id": source_id,
                    "source_name": source_config["name"],
                    "source_logo": source_config.get("logo", ""),
                    "source_color": source_config.get("color", "#333333"),
                    "image_url": news_extract_image(entry),
                    "published_at": news_parse_date(entry),
                    "categories": source_config.get("categories", []),
                    "symbols": symbols,
                    "sentiment": None,
                    "ai_summary": None,
                    "priority": priority,
                }
                
                articles.append(article)
                
        except Exception as e:
            debug(NEWS_ARTICLES_TASK, f"Feed error {source_id}: {e}")
    
    return articles


def store_news_articles(articles: List[Dict]) -> int:
    """Store articles to Supabase."""
    stored = 0
    for article in articles:
        try:
            record = {
                "id": article["id"],
                "title": article["title"],
                "summary": article["summary"],
                "url": article["url"],
                "source_id": article["source_id"],
                "source_name": article["source_name"],
                "source_logo": article["source_logo"],
                "source_color": article["source_color"],
                "image_url": article["image_url"],
                "published_at": article["published_at"],
                "categories": article["categories"],
                "symbols": article["symbols"],
                "sentiment": article["sentiment"],
                "ai_summary": article["ai_summary"],
                "priority": article["priority"],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            sb.table("news_articles").upsert(record, on_conflict="id").execute()
            stored += 1
        except Exception as e:
            debug(NEWS_ARTICLES_TASK, f"Failed to store article: {e}")
    return stored


async def news_articles_task():
    """Task 11: Scrape and store financial news articles."""
    task_metrics = metrics.get_or_create(NEWS_ARTICLES_TASK)
    
    info(NEWS_ARTICLES_TASK, f"News Articles Scraper started ({len(NEWS_SOURCES_CONFIG)} sources)")
    
    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            
            # Determine interval based on time
            if now.weekday() >= 5:  # Weekend
                interval = 7200  # 2 hours
            elif 8 <= now.hour < 21:  # Market hours
                interval = 900  # 15 minutes
            else:  # Off hours
                interval = 1800  # 30 minutes
            
            info(NEWS_ARTICLES_TASK, "Starting news scrape cycle...")
            
            all_articles = []
            loop = asyncio.get_event_loop()
            
            for source_id, source_config in NEWS_SOURCES_CONFIG.items():
                articles = await loop.run_in_executor(None, scrape_news_source, source_id, source_config)
                all_articles.extend(articles)
                debug(NEWS_ARTICLES_TASK, f"  {source_config['name']}: {len(articles)} articles")
                await asyncio.sleep(0.5)  # Rate limit
            
            # Deduplicate by URL
            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    unique_articles.append(article)
            
            # Sort by priority
            unique_articles.sort(key=lambda x: x["priority"], reverse=True)
            unique_articles = unique_articles[:100]  # Limit
            
            # Store to database
            stored = await loop.run_in_executor(None, store_news_articles, unique_articles)
            
            info(NEWS_ARTICLES_TASK, f"Scraped {len(unique_articles)} articles, stored {stored}")
            
            task_metrics.record_success()
            task_metrics.custom_metrics.update({
                "last_article_count": len(unique_articles),
                "last_stored": stored,
            })
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            error(NEWS_ARTICLES_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(300)


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point."""
    
    # Run startup diagnostics
    run_startup_diagnostics()
    
    log.info("=" * 70)
    log.info("LONDON STRATEGIC EDGE - WORKER v9.0 (DEBUG)")
    log.info("=" * 70)
    log.info("Starting all tasks...")
    log.info("=" * 70)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        log.info(f"Received signal {sig}, initiating shutdown...")
        tick_streamer.shutdown()
        whale_tracker.shutdown()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create all tasks
    tasks = [
        asyncio.create_task(tick_streaming_task(), name="tick_stream"),
        asyncio.create_task(economic_calendar_task(), name="calendar"),
        asyncio.create_task(financial_news_task(), name="news"),
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
        asyncio.create_task(bond_yield_task(), name="bonds"),
        asyncio.create_task(whale_flow_task(), name="whale"),
        asyncio.create_task(ai_news_desk_task(), name="ai_news"),
        asyncio.create_task(macro_fetcher_task(), name="macro"),
        asyncio.create_task(index_poller_task(), name="index"),
        asyncio.create_task(sector_sentiment_task(), name="sentiment"),
        asyncio.create_task(news_articles_task(), name="news_articles"),
    ]
    
    # Periodic metrics logging
    async def metrics_logger():
        while not tick_streamer._shutdown.is_set():
            await asyncio.sleep(300)  # Every 5 minutes
            metrics.log_summary()
            log.info(f"Supabase stats: {sb.get_stats()}")
    
    tasks.append(asyncio.create_task(metrics_logger(), name="metrics"))
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log.error(f"Main loop error: {e}", exc_info=True)
    finally:
        tick_streamer.shutdown()
        whale_tracker.shutdown()
        
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        
        # Final metrics report
        log.info("")
        metrics.log_summary()
        log.info("Worker shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(0)
