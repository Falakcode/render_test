#!/usr/bin/env python3
"""
================================================================================
LONDON STRATEGIC EDGE - BULLETPROOF WORKER v9.1 (DEBUG EDITION)
================================================================================
Production-grade data pipeline with comprehensive debugging and diagnostics.

DEBUG FEATURES:
  - Startup schema validation
  - Request/response logging with payloads
  - Detailed error context and stack traces
  - Per-task health metrics
  - Configurable debug verbosity

STARTUP:
  - Gap Fill (one-time) - Auto-fills gaps > 15min from last 24h (market-aware)

TASKS:
  1. Tick Streaming     - EODHD WebSocket (280 symbols across 7 connections)
  2. Economic Calendar  - EODHD API (every 5 min)
  3. Financial News     - RSS feeds + Gemini AI classification
  4. Gap Fill Service   - Auto-detect and report missing candle data
  5. Whale Flow Tracker - BTC whale movements via Blockchain.com WebSocket
  6. AI News Desk       - Placeholder for article synthesis
  7. Sector Sentiment   - AI market sentiment analysis (scheduled updates)
  8. News Articles      - Multi-source news scraper with filtering
  9. OANDA Streaming    - Commodities & indices real-time (24 instruments)
  10. Bond Yields 4H    - Government bonds via OANDA streaming (6 bonds)
  11. Stock Data Sync   - EODHD financial data (earnings, dividends, insider trades)

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
import pytz

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

# OANDA API Configuration
OANDA_API_KEY = os.environ.get("OANDA_API_KEY", "bc3eda63cee6780c003d88b4fab38d65-d6ee2b3133bfb3646462c6debb163fb0")
OANDA_STREAM_URL = "https://stream-fxtrade.oanda.com"
OANDA_REST_URL = "https://api-fxtrade.oanda.com"
OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID", "001-004-19240534-003")

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
# ============================================================================
#                          MARKET HOURS FILTER
# ============================================================================
# Prevents bad tick data during market close from corrupting charts.
# Uses pytz for automatic DST handling.

# Eastern Time zone (handles DST automatically)
ET = pytz.timezone('America/New_York')

# Crypto symbols that trade 24/7
CRYPTO_BASE_SYMBOLS = {
    'BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'BNB', 'AVAX', 'DOT', 'LINK',
    'ATOM', 'LTC', 'ALGO', 'NEAR', 'FIL', 'AAVE', 'UNI', 'COMP', 'CRV', 'XLM',
    'ARB', 'OP', 'SAND', 'MANA', 'LDO', 'SNX', 'VET', 'MATIC', 'FTM', 'ICP',
    'IMX', 'APT', 'AXS', 'BLUR', 'CHZ', 'ENJ', 'GALA', 'GMX', 'MKR', 'RPL',
    'DAI', 'USDC', 'USDT', 'BUSD', 'TUSD'
}


def is_crypto_symbol(symbol: str) -> bool:
    """Check if symbol is a crypto pair (trades 24/7)."""
    symbol_upper = symbol.upper().replace('/', '').replace('-', '')
    # Check if any crypto base is in the symbol
    for crypto in CRYPTO_BASE_SYMBOLS:
        if crypto in symbol_upper:
            return True
    return False


def is_market_open(symbol: str) -> bool:
    """
    Check if market is open for a given symbol.
    
    - Crypto: Always open (24/7)
    - Forex/Stocks/ETFs: Closed Friday 5PM ET to Sunday 5PM ET
    
    Uses pytz for automatic DST handling.
    """
    # Crypto trades 24/7
    if is_crypto_symbol(symbol):
        return True
    
    # Get current time in Eastern Time (DST-aware)
    now_et = datetime.now(ET)
    weekday = now_et.weekday()  # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
    hour = now_et.hour
    
    # Forex/Stocks closed: Friday 5PM ET to Sunday 5PM ET
    # Friday (4) after 5PM = closed
    if weekday == 4 and hour >= 17:
        return False
    
    # Saturday (5) = closed all day
    if weekday == 5:
        return False
    
    # Sunday (6) before 5PM = closed
    if weekday == 6 and hour < 17:
        return False
    
    # All other times = open
    return True


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
    log.info(f"  CALENDAR_FETCH_INTERVAL: {CALENDAR_FETCH_INTERVAL}s")
    
    # Log symbol counts
    log.info("")
    log.info("Symbol Coverage:")
    log.info(f"  Forex: {len(ALL_FOREX)}")
    log.info(f"  Crypto: {len(ALL_CRYPTO)}")
    log.info(f"  US Stocks: {len(ALL_US_STOCKS)}")
    log.info(f"  ETFs: {len(ALL_ETFS)}")
    log.info(f"  TOTAL: {TOTAL_SYMBOLS}")
    
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
            
            # Skip ticks when market is closed (prevents weekend/after-hours garbage data)
            if not is_market_open(normalized):
                return
            
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
#                    TASK 6: BTC WHALE FLOW TRACKER v3.2
# ============================================================================
"""
Smart whale detection with 7 methods:
1. Known wallet matching (25 verified exchange addresses)
2. Learned wallet matching (addresses seen 3+ times)
3. Consolidation patterns (5+ inputs â†’ 2 outputs)
4. Distribution/batch payout (2 inputs â†’ 5+ outputs)
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
                log.debug(f"ðŸ‹ BTC price from tickdata: ${self._btc_price:,.0f}")
                return
            
            # Fallback to CoinGecko
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd',
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self._btc_price = data['bitcoin']['usd']
                log.debug(f"ðŸ‹ BTC price from CoinGecko: ${self._btc_price:,.0f}")
        except Exception as e:
            log.warning(f"âš ï¸ Failed to fetch BTC price: {e}")
    
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
        
        # METHOD 3: Consolidation pattern (5+ inputs â†’ 1-2 outputs)
        if len(inputs) >= 5 and len(outputs) <= 2:
            self._stats['pattern_consolidation'] += 1
            return ('consolidation', 'Pattern', 'consolidation')
        
        # METHOD 4: Distribution/batch payout (1-2 inputs â†’ 5+ outputs)
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
                f"ðŸ‹ {emoji} WHALE: {total_btc:.2f} BTC (${total_usd:,.0f}) "
                f"| {flow_type.upper()} {exchange_str} {method_str}"
            )
            
            # Store to Supabase
            await self._store_flow(flow)
            
        except Exception as e:
            log.warning(f"âš ï¸ Error handling whale tx: {e}")
    
    def _get_flow_emoji(self, flow_type: str) -> str:
        """Get emoji for flow type."""
        emojis = {
            'exchange_inflow': 'ðŸ”´',      # Bearish
            'exchange_outflow': 'ðŸŸ¢',     # Bullish
            'distribution': 'ðŸŸ¢',         # Bullish (batch payout)
            'consolidation': 'ðŸ”„',        # Neutral
            'likely_withdrawal': 'ðŸŸ¡',    # Likely bullish
            'unknown': 'âšª'               # Unknown
        }
        return emojis.get(flow_type, 'âšª')
    
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
                log.debug(f"âš ï¸ Failed to store whale flow: {e}")
    
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
                log.debug(f"âš ï¸ Failed to store momentum: {e}")
    
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
                    net_direction = "ðŸ“ˆ" if net_flow > 0 else "ðŸ“‰" if net_flow < 0 else "âž¡ï¸"
                    
                    log.info(
                        f"ðŸ‹ WHALE STATS | Score: {momentum['flow_score']:+d} {momentum['regime']} | "
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
                log.warning(f"âš ï¸ Periodic task error: {e}")
                await asyncio.sleep(10)
    
    async def _connect_and_stream(self):
        """Connect to Blockchain.com WebSocket and stream transactions."""
        self._connection_count += 1
        log.info(f"ðŸ‹ Whale tracker connection attempt #{self._connection_count}...")
        
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
            log.info("ðŸ‹ Connected to Blockchain.com - streaming whale transactions")
            
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
                    log.warning(f"âš ï¸ Error processing message: {e}")
                    
        except asyncio.TimeoutError:
            log.error("â±ï¸ Whale tracker connection timeout")
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"ðŸ”Œ Whale tracker connection closed: {e}")
        except Exception as e:
            log.error(f"âŒ Whale tracker error: {e}")
        finally:
            self._is_connected = False
            if self._ws:
                with suppress(Exception):
                    await self._ws.close()
                self._ws = None
    
    async def run(self):
        """Main run loop with reconnection logic."""
        log.info("=" * 60)
        log.info("ðŸ‹ BTC WHALE FLOW TRACKER v3.2 STARTING")
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
                    log.info(f"ðŸ”„ Whale tracker reconnecting in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(30, backoff * 2)
                    
                except Exception as e:
                    log.error(f"ðŸ’¥ Whale tracker main loop error: {e}")
                    await asyncio.sleep(5)
                    
        finally:
            log.info("ðŸ›‘ Shutting down whale tracker...")
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
    "reuters": {
        "name": "Reuters",
        "logo": "https://www.reuters.com/pf/resources/images/reuters/reuters-default.png",
        "color": "#FF8000",
        "rss_feeds": [
            "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
        ],
        "categories": ["markets", "economy", "breaking"],
    },
    "wsj": {
        "name": "Wall Street Journal",
        "logo": "https://s.wsj.net/img/meta/wsj-social-share.png",
        "color": "#0274B6",
        "rss_feeds": [
            "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
            "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
        ],
        "categories": ["markets", "business", "economy"],
    },
    "bloomberg": {
        "name": "Bloomberg",
        "logo": "https://assets.bwbx.io/s3/javelin/public/hub/images/social-default-a4f15fa7ee.jpg",
        "color": "#2800D7",
        "rss_feeds": [
            "https://feeds.bloomberg.com/markets/news.rss",
        ],
        "categories": ["markets", "economy", "stocks"],
    },
    "yahoo_finance": {
        "name": "Yahoo Finance",
        "logo": "https://s.yimg.com/cv/apiv2/default/20190501/yahoo_finance_en-US_2x.png",
        "color": "#6001D2",
        "rss_feeds": ["https://finance.yahoo.com/news/rssindex"],
        "categories": ["markets", "stocks", "economy"],
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
    # TheStreet REMOVED - too much clickbait content
    "ft": {
    "name": "Financial Times",
    "logo": "https://upload.wikimedia.org/wikipedia/commons/5/5a/Financial_Times_logo_%282019%29.svg",
    "color": "#FFE6D5",
    "rss_feeds": [
        "https://www.ft.com/?format=rss",
        "https://www.ft.com/markets?format=rss",
        "https://www.ft.com/world?format=rss"
    ],
    "categories": ["markets", "economy", "stocks", "world"],
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
    # Geopolitical/Holiday noise
    "palestinian", "bethlehem", "gaza", "israel conflict", "christmas",
    "holiday gift", "thanksgiving", "family gathering",
    "how to talk to your family", "gift guide",
    
    # Legal spam
    "investor news: if you have suffered", "class action", "lawsuit alert",
    "securities fraud", "investor alert", "legal action",
    
    # Crypto spam
    "shibarium", "shib explorer", "meme coin", "pump and dump",
    "airdrop alert", "free tokens", "moonshot",
    
    # Sponsored/PR content
    "geekstake", "sponsored content", "partner content",
    "press release:", "prn newswire", "business wire",
    
    # Clickbait patterns
    "you won't believe", "shocking", "this dangerous", "at risk from this",
    "perfect gift", "bestselling", "limited time", "deal alert",
    "millionaires are buying", "get rich", "passive income",
    "one stock to buy", "this stock could", "history says",
    "will soar", "going to explode", "next big thing",
    "don't miss", "before it's too late", "secret",
    "wall street doesn't want you", "they don't want you to know",
    "guaranteed returns", "100% returns", "double your money",
    
    # Lifestyle/Non-financial
    "birthstone", "necklace", "jewelry", "fashion week",
    "horoscope", "zodiac", "astrology",
    "recipe", "cooking tips", "diet plan", "weight loss",
    "drivers at risk", "homeowners at risk", "retirees at risk",
    "simple trick", "weird trick", "doctors hate",
    "celebrity", "kardashian", "royal family", "meghan markle",
    "quiz:", "poll:", "vote:",
    
    # Listicle/Opinion patterns  
    "top 10", "top 5", "5 reasons", "7 ways", "3 stocks", "10 best",
    "should you buy", "is it time to buy", "time to sell",
    "my favorite stock", "i'm buying", "i'm selling",
    "here's why i", "why i think", "in my opinion",
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
    """Calculate priority score (0-100). Higher = more important factual news."""
    text = f"{title} {summary}".lower()
    title_lower = title.lower()
    score = 50
    
    # BOOST: Factual keywords
    for keyword in NEWS_PRIORITY_KEYWORDS:
        if keyword.lower() in text:
            score += 5
    
    # BOOST: Has stock symbols mentioned
    score += len(symbols) * 3
    
    # BOOST: Breaking/Alert news
    if any(x in title_lower for x in ["breaking", "just in", "alert", "confirms", "announces"]):
        score += 15
    
    # BOOST: Earnings/Financial results (factual)
    if any(x in text for x in ["earnings", "quarterly", "revenue beat", "reports q", "fiscal year"]):
        score += 12
    
    # BOOST: Central bank/Economic data (factual)
    if any(x in text for x in ["federal reserve", "fed ", "rate decision", "fomc", "ecb ", "boe "]):
        score += 12
    if any(x in text for x in ["cpi ", "inflation data", "jobs report", "nonfarm", "gdp growth"]):
        score += 10
    
    # BOOST: Regulatory/Official actions
    if any(x in text for x in ["sec ", "regulatory", "approved", "rejected", "investigation"]):
        score += 8
    
    # BOOST: Corporate actions (factual)
    if any(x in text for x in ["acquisition", "merger", "buyout", "deal worth", "agrees to"]):
        score += 10
    if any(x in text for x in ["ipo price", "goes public", "files for ipo"]):
        score += 8
    
    # PENALIZE: Opinion/Prediction patterns
    if any(x in title_lower for x in ["my favorite", "could ", "should you", "i think", "i believe"]):
        score -= 20
    if any(x in title_lower for x in ["top 10", "5 stocks", "3 reasons", "best stocks", "worst stocks"]):
        score -= 20
    if any(x in title_lower for x in ["will soar", "going to", "predicted to", "expected to rally"]):
        score -= 15
    if any(x in title_lower for x in ["opinion:", "analysis:", "why i", "my take", "my view"]):
        score -= 20
    if any(x in title_lower for x in ["history says", "according to history", "historically"]):
        score -= 15
    if any(x in title_lower for x in ["this is why", "here's why", "here is why", "the reason"]):
        score -= 10
    
    # PENALIZE: Question headlines (often clickbait)
    if "?" in title and len(title) < 70:
        score -= 12
    
    # PENALIZE: Vague/Sensational language
    if any(x in title_lower for x in ["this stock", "one stock", "the stock"]) and not any(s in title for s in symbols):
        score -= 15
    if any(x in title_lower for x in ["you need to know", "what you should", "don't miss"]):
        score -= 15
    if any(x in title_lower for x in ["can ", "may ", "might "]) and "?" not in title:
        score -= 8
    
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
                
                if priority < 45:  # Raised from 30 - stricter quality filter
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
#                    TASK 11: OANDA STREAMING (Commodities & Indices)
# ============================================================================
# Real-time price streaming for commodities and indices via OANDA API.
# Uses HTTP streaming (Server-Sent Events style) for persistent connection.
#
# INSTRUMENTS (24 total - excludes bonds):
#   Metals (5): XAU, XAG, XPT, XPD, XCU
#   Energy (3): WTICO, BCO, NATGAS  
#   Agriculture (4): CORN, WHEAT, SOYBN, SUGAR
#   US Indices (4): US30, SPX500, NAS100, US2000
#   EU Indices (4): UK100, DE30, EU50, FR40
#   Asia Indices (4): JP225, AU200, HK33, CN50
# ============================================================================

OANDA_TASK = "OANDA_STREAM"

# OANDA instruments: API symbol -> (display_symbol, candle_table)
OANDA_INSTRUMENTS = {
    # Metals
    "XAU_USD": ("XAU/USD", "candles_xau_usd"),
    "XAG_USD": ("XAG/USD", "candles_xag_usd"),
    "XPT_USD": ("XPT/USD", "candles_xpt_usd"),
    "XPD_USD": ("XPD/USD", "candles_xpd_usd"),
    "XCU_USD": ("XCU/USD", "candles_xcu_usd"),
    # Energy
    "WTICO_USD": ("WTICO/USD", "candles_wtico_usd"),
    "BCO_USD": ("BCO/USD", "candles_bco_usd"),
    "NATGAS_USD": ("NATGAS/USD", "candles_natgas_usd"),
    # Agriculture
    "CORN_USD": ("CORN/USD", "candles_corn_usd"),
    "WHEAT_USD": ("WHEAT/USD", "candles_wheat_usd"),
    "SOYBN_USD": ("SOYBN/USD", "candles_soybn_usd"),
    "SUGAR_USD": ("SUGAR/USD", "candles_sugar_usd"),
    # US Indices
    "US30_USD": ("US30/USD", "candles_us30_usd"),
    "SPX500_USD": ("SPX500/USD", "candles_spx500_usd"),
    "NAS100_USD": ("NAS100/USD", "candles_nas100_usd"),
    "US2000_USD": ("US2000/USD", "candles_us2000_usd"),
    # European Indices
    "UK100_GBP": ("UK100/GBP", "candles_uk100_gbp"),
    "DE30_EUR": ("DE30/EUR", "candles_de30_eur"),
    "EU50_EUR": ("EU50/EUR", "candles_eu50_eur"),
    "FR40_EUR": ("FR40/EUR", "candles_fr40_eur"),
    # Asia-Pacific Indices
    "JP225_USD": ("JP225/USD", "candles_jp225_usd"),
    "AU200_AUD": ("AU200/AUD", "candles_au200_aud"),
    "HK33_HKD": ("HK33/HKD", "candles_hk33_hkd"),
    "CN50_USD": ("CN50/USD", "candles_cn50_usd"),
}

# Track OANDA minute-level OHLC in memory for candle building
_oanda_minute_ohlc: Dict[str, Dict[str, Dict[str, float]]] = {}
_oanda_ohlc_lock = threading.Lock()


def oanda_update_minute_ohlc(symbol: str, minute_ts: str, price: float) -> Dict[str, float]:
    """Track minute-level OHLC for OANDA instruments."""
    with _oanda_ohlc_lock:
        if symbol not in _oanda_minute_ohlc:
            _oanda_minute_ohlc[symbol] = {}
        
        candles = _oanda_minute_ohlc[symbol]
        
        # Cleanup old minutes (keep last 5)
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


def oanda_detect_gaps(symbol: str, table_name: str, lookback_hours: int = 4) -> List[Tuple[datetime, datetime]]:
    """Detect gaps in OANDA candle data since last backfill."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        
        result = sb.table(table_name).select("timestamp").gte(
            "timestamp", since
        ).order("timestamp", desc=False).execute()
        
        if not result.data or len(result.data) < 2:
            return []
        
        gaps = []
        timestamps = [
            datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
            for r in result.data
        ]
        
        for i in range(1, len(timestamps)):
            gap_minutes = (timestamps[i] - timestamps[i-1]).total_seconds() / 60
            if gap_minutes > 5:  # 5 minute gap threshold for OANDA data
                gaps.append((timestamps[i-1], timestamps[i]))
        
        return gaps
        
    except Exception as e:
        debug(OANDA_TASK, f"Gap detection error for {symbol}: {e}")
        return []


def oanda_fill_gaps_from_api(symbol: str, table_name: str, display_symbol: str, gaps: List[Tuple[datetime, datetime]]) -> int:
    """Fill detected gaps using OANDA REST API historical candles."""
    if not gaps:
        return 0
    
    filled_count = 0
    headers = {
        "Authorization": f"Bearer {OANDA_API_KEY}",
        "Accept-Datetime-Format": "RFC3339",
    }
    
    for gap_start, gap_end in gaps:
        try:
            url = f"{OANDA_REST_URL}/v3/instruments/{symbol}/candles"
            params = {
                "granularity": "M1",
                "price": "M",
                "from": gap_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": gap_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                continue
            
            candles = resp.json().get("candles", [])
            
            for c in candles:
                if not c.get("complete", False):
                    continue
                
                mid = c.get("mid", {})
                candle_record = {
                    "timestamp": c["time"],
                    "open": float(mid.get("o", 0)),
                    "high": float(mid.get("h", 0)),
                    "low": float(mid.get("l", 0)),
                    "close": float(mid.get("c", 0)),
                    "volume": int(c.get("volume", 0)),
                    "symbol": display_symbol,
                }
                
                try:
                    sb.table(table_name).upsert(candle_record, on_conflict="timestamp").execute()
                    filled_count += 1
                except Exception:
                    pass
            
            time.sleep(0.25)  # Rate limit
            
        except Exception as e:
            debug(OANDA_TASK, f"Gap fill error for {symbol}: {e}")
    
    return filled_count


def oanda_run_gap_detection_and_fill() -> Dict[str, int]:
    """Run gap detection and fill for all OANDA instruments."""
    results = {}
    
    for oanda_symbol, (display_symbol, table_name) in OANDA_INSTRUMENTS.items():
        gaps = oanda_detect_gaps(oanda_symbol, table_name)
        if gaps:
            filled = oanda_fill_gaps_from_api(oanda_symbol, table_name, display_symbol, gaps)
            results[display_symbol] = filled
            if filled > 0:
                debug(OANDA_TASK, f"  {display_symbol}: Filled {filled} gap candles")
    
    return results



def _oanda_stream_worker(shutdown_event: threading.Event, task_metrics):
    """Worker thread for OANDA streaming - runs blocking requests in separate thread."""
    
    instruments_str = ",".join(OANDA_INSTRUMENTS.keys())
    url = f"{OANDA_STREAM_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    params = {"instruments": instruments_str}
    
    tick_count = 0
    reconnect_count = 0
    last_health_log = datetime.now(timezone.utc)
    instruments_seen = set()
    
    while not shutdown_event.is_set():
        try:
            reconnect_count += 1
            info(OANDA_TASK, f"Connecting to OANDA stream (attempt #{reconnect_count})...")
            
            with requests.get(url, headers=headers, params=params, stream=True, timeout=60) as resp:
                if resp.status_code != 200:
                    error(OANDA_TASK, f"Stream connection failed: HTTP {resp.status_code}")
                    task_metrics.record_error(f"HTTP {resp.status_code}")
                    time.sleep(30)
                    continue
                
                info(OANDA_TASK, f"Connected! Streaming {len(OANDA_INSTRUMENTS)} instruments...")
                
                for line in resp.iter_lines():
                    if shutdown_event.is_set():
                        break
                    
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line.decode('utf-8'))
                        msg_type = data.get("type", "")
                        
                        if msg_type == "PRICE":
                            instrument = data.get("instrument", "")
                            if instrument not in OANDA_INSTRUMENTS:
                                continue
                            
                            display_symbol, candle_table = OANDA_INSTRUMENTS[instrument]
                            instruments_seen.add(display_symbol)
                            
                            bids = data.get("bids", [])
                            asks = data.get("asks", [])
                            
                            if not bids or not asks:
                                continue
                            
                            bid = float(bids[0].get("price", 0))
                            ask = float(asks[0].get("price", 0))
                            price = (bid + ask) / 2
                            
                            bid_liquidity = int(bids[0].get("liquidity", 0))
                            ask_liquidity = int(asks[0].get("liquidity", 0))
                            volume = bid_liquidity + ask_liquidity
                            
                            now = datetime.now(timezone.utc)
                            minute_ts = now.replace(second=0, microsecond=0).isoformat()
                            
                            minute_ohlc = oanda_update_minute_ohlc(display_symbol, minute_ts, price)
                            
                            tick = {
                                "symbol": display_symbol,
                                "price": price,
                                "bid": bid,
                                "ask": ask,
                                "volume": volume,
                                "ts": now.isoformat(),
                            }
                            
                            try:
                                sb.table(SUPABASE_TABLE).insert(tick).execute()
                            except Exception as e:
                                if tick_count % 1000 == 0:
                                    debug(OANDA_TASK, f"Tick insert error: {e}")
                            
                            candle = {
                                "timestamp": minute_ts,
                                "open": minute_ohlc["open"],
                                "high": minute_ohlc["high"],
                                "low": minute_ohlc["low"],
                                "close": minute_ohlc["close"],
                                "volume": volume,
                                "symbol": display_symbol,
                            }
                            
                            try:
                                sb.table(candle_table).upsert(candle, on_conflict="timestamp").execute()
                            except Exception as e:
                                if tick_count % 1000 == 0:
                                    debug(OANDA_TASK, f"Candle upsert error: {e}")
                            
                            tick_count += 1
                            task_metrics.record_success()
                            
                        elif msg_type == "HEARTBEAT":
                            pass
                        
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        debug(OANDA_TASK, f"Tick processing error: {e}")
                    
                    now = datetime.now(timezone.utc)
                    if (now - last_health_log).total_seconds() >= 60:
                        info(OANDA_TASK, f"HEALTH: {tick_count:,} ticks | {len(instruments_seen)}/{len(OANDA_INSTRUMENTS)} instruments active")
                        task_metrics.custom_metrics.update({
                            "tick_count": tick_count,
                            "instruments_active": len(instruments_seen),
                            "reconnect_count": reconnect_count,
                        })
                        last_health_log = now
        
        except requests.exceptions.RequestException as e:
            error(OANDA_TASK, f"Stream error: {e}")
            task_metrics.record_error(str(e))
            time.sleep(10)
        except Exception as e:
            error(OANDA_TASK, f"Unexpected error: {e}")
            task_metrics.record_error(str(e))
            time.sleep(30)
    
    info(OANDA_TASK, "OANDA stream worker shutting down")


async def oanda_streaming_task():
    """Task 9: Stream real-time prices from OANDA for commodities & indices."""
    task_metrics = metrics.get_or_create(OANDA_TASK)
    
    info(OANDA_TASK, f"OANDA Streaming started ({len(OANDA_INSTRUMENTS)} instruments)")
    
    # Run initial gap detection and fill
    info(OANDA_TASK, "Running startup gap detection...")
    loop = asyncio.get_event_loop()
    gap_results = await loop.run_in_executor(None, oanda_run_gap_detection_and_fill)
    if gap_results:
        total_filled = sum(gap_results.values())
        info(OANDA_TASK, f"Gap fill complete: {total_filled} candles filled across {len(gap_results)} instruments")
    else:
        info(OANDA_TASK, "No gaps detected")
    
    # Create shutdown event for thread
    shutdown_event = threading.Event()
    
    # Start streaming in a separate thread to avoid blocking asyncio
    stream_thread = threading.Thread(
        target=_oanda_stream_worker,
        args=(shutdown_event, task_metrics),
        daemon=True,
        name="oanda_stream"
    )
    stream_thread.start()
    info(OANDA_TASK, "OANDA stream thread started")
    
    # Wait for main shutdown signal
    while not tick_streamer._shutdown.is_set():
        await asyncio.sleep(1)
    
    # Signal thread to stop
    shutdown_event.set()
    stream_thread.join(timeout=5)
    info(OANDA_TASK, "OANDA streaming task complete")


# ============================================================================
#                    TASK 10: BOND YIELDS STREAMING (4-Hour Candles)
# ============================================================================
# Streams government bond prices via OANDA and builds 4H candles.
# Bonds: US 2Y/5Y/10Y/30Y Treasury, UK 10Y Gilt, DE 10Y Bund
# Stores in unified bond_yields table.
# ============================================================================

BOND_4H_TASK = "BOND_4H"

# OANDA symbol -> (short_symbol, country, maturity)
BOND_SYMBOLS = {
    "USB02Y_USD": ("USB02Y", "US", "2Y"),
    "USB05Y_USD": ("USB05Y", "US", "5Y"),
    "USB10Y_USD": ("USB10Y", "US", "10Y"),
    "USB30Y_USD": ("USB30Y", "US", "30Y"),
    "UK10YB_GBP": ("UK10YB", "UK", "10Y"),
    "DE10YB_EUR": ("DE10YB", "DE", "10Y"),
}

# Track 4-hour OHLC in memory
_bond_4h_ohlc: Dict[str, Dict[str, Dict[str, float]]] = {}
_bond_4h_lock = threading.Lock()


def get_4h_bucket(dt: datetime) -> str:
    """Get the 4-hour bucket timestamp for a given datetime."""
    hour_bucket = (dt.hour // 4) * 4
    return dt.replace(hour=hour_bucket, minute=0, second=0, microsecond=0).isoformat()


def bond_update_4h_ohlc(symbol: str, bucket_ts: str, price: float) -> Tuple[Dict[str, float], bool]:
    """Track 4-hour OHLC for bonds. Returns (current_ohlc, is_new_bucket)."""
    with _bond_4h_lock:
        if symbol not in _bond_4h_ohlc:
            _bond_4h_ohlc[symbol] = {}
        
        candles = _bond_4h_ohlc[symbol]
        is_new_bucket = False
        
        if len(candles) > 5:
            sorted_buckets = sorted(candles.keys())
            for old_bucket in sorted_buckets[:-3]:
                del candles[old_bucket]
        
        if bucket_ts not in candles:
            is_new_bucket = len(candles) > 0
            candles[bucket_ts] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }
        else:
            c = candles[bucket_ts]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
        
        return candles[bucket_ts].copy(), is_new_bucket


def get_previous_4h_candle(symbol: str, current_bucket: str) -> Optional[Tuple[str, Dict[str, float]]]:
    """Get the completed previous 4H candle if it exists."""
    with _bond_4h_lock:
        if symbol not in _bond_4h_ohlc:
            return None
        
        candles = _bond_4h_ohlc[symbol]
        sorted_buckets = sorted(candles.keys())
        
        for i, bucket in enumerate(sorted_buckets):
            if bucket == current_bucket and i > 0:
                prev_bucket = sorted_buckets[i - 1]
                return (prev_bucket, candles[prev_bucket].copy())
        
        return None


def save_bond_candle(symbol: str, country: str, maturity: str, timestamp: str, ohlc: Dict[str, float]) -> bool:
    """Save a completed 4H bond candle to Supabase."""
    try:
        record = {
            "symbol": symbol,
            "country": country,
            "maturity": maturity,
            "timestamp": timestamp,
            "open": ohlc["open"],
            "high": ohlc["high"],
            "low": ohlc["low"],
            "close": ohlc["close"],
            "volume": 0,
        }
        
        sb.table("bond_yields").upsert(record, on_conflict="symbol,timestamp").execute()
        return True
        
    except Exception as e:
        debug(BOND_4H_TASK, f"Error saving {symbol} candle: {e}")
        return False


def _bond_stream_worker(shutdown_event: threading.Event, task_metrics):
    """Worker thread for Bond streaming - runs blocking requests in separate thread."""
    
    instruments_str = ",".join(BOND_SYMBOLS.keys())
    url = f"{OANDA_STREAM_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    params = {"instruments": instruments_str}
    
    tick_count = 0
    candles_saved = 0
    reconnect_count = 0
    last_health_log = datetime.now(timezone.utc)
    
    while not shutdown_event.is_set():
        try:
            reconnect_count += 1
            info(BOND_4H_TASK, f"Connecting to OANDA bond stream (attempt #{reconnect_count})...")
            
            with requests.get(url, headers=headers, params=params, stream=True, timeout=60) as resp:
                if resp.status_code != 200:
                    error(BOND_4H_TASK, f"Stream connection failed: HTTP {resp.status_code}")
                    task_metrics.record_error(f"HTTP {resp.status_code}")
                    time.sleep(30)
                    continue
                
                info(BOND_4H_TASK, f"Connected! Streaming {len(BOND_SYMBOLS)} bonds...")
                
                for line in resp.iter_lines():
                    if shutdown_event.is_set():
                        break
                    
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line.decode('utf-8'))
                        msg_type = data.get("type", "")
                        
                        if msg_type == "PRICE":
                            instrument = data.get("instrument", "")
                            if instrument not in BOND_SYMBOLS:
                                continue
                            
                            short_symbol, country, maturity = BOND_SYMBOLS[instrument]
                            
                            bids = data.get("bids", [])
                            asks = data.get("asks", [])
                            
                            if not bids or not asks:
                                continue
                            
                            bid = float(bids[0].get("price", 0))
                            ask = float(asks[0].get("price", 0))
                            price = (bid + ask) / 2
                            
                            now = datetime.now(timezone.utc)
                            bucket_ts = get_4h_bucket(now)
                            
                            current_ohlc, is_new_bucket = bond_update_4h_ohlc(short_symbol, bucket_ts, price)
                            
                            tick_count += 1
                            
                            if is_new_bucket:
                                prev = get_previous_4h_candle(short_symbol, bucket_ts)
                                if prev:
                                    prev_ts, prev_ohlc = prev
                                    saved = save_bond_candle(short_symbol, country, maturity, prev_ts, prev_ohlc)
                                    if saved:
                                        candles_saved += 1
                                        info(BOND_4H_TASK, f"Saved 4H candle: {short_symbol} @ {prev_ts[:16]} close={prev_ohlc['close']:.4f}")
                            
                            task_metrics.record_success()
                            
                        elif msg_type == "HEARTBEAT":
                            pass
                        
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        debug(BOND_4H_TASK, f"Tick processing error: {e}")
                    
                    now = datetime.now(timezone.utc)
                    if (now - last_health_log).total_seconds() >= 60:
                        info(BOND_4H_TASK, f"HEALTH: {tick_count:,} ticks | {candles_saved} 4H candles saved")
                        task_metrics.custom_metrics.update({
                            "tick_count": tick_count,
                            "candles_saved": candles_saved,
                            "reconnect_count": reconnect_count,
                        })
                        last_health_log = now
        
        except requests.exceptions.RequestException as e:
            error(BOND_4H_TASK, f"Stream error: {e}")
            task_metrics.record_error(str(e))
            time.sleep(10)
        except Exception as e:
            error(BOND_4H_TASK, f"Unexpected error: {e}")
            task_metrics.record_error(str(e))
            time.sleep(30)
    
    info(BOND_4H_TASK, "Bond stream worker shutting down")


async def bond_yields_streaming_task():
    """Task 10: Stream bond prices and build 4H candles."""
    task_metrics = metrics.get_or_create(BOND_4H_TASK)
    
    info(BOND_4H_TASK, f"Bond Yields Streaming started ({len(BOND_SYMBOLS)} bonds)")
    
    # Create shutdown event for thread
    shutdown_event = threading.Event()
    
    # Start streaming in a separate thread to avoid blocking asyncio
    stream_thread = threading.Thread(
        target=_bond_stream_worker,
        args=(shutdown_event, task_metrics),
        daemon=True,
        name="bond_stream"
    )
    stream_thread.start()
    info(BOND_4H_TASK, "Bond stream thread started")
    
    # Wait for main shutdown signal
    while not tick_streamer._shutdown.is_set():
        await asyncio.sleep(1)
    
    # Signal thread to stop
    shutdown_event.set()
    stream_thread.join(timeout=5)
    info(BOND_4H_TASK, "Bond yields streaming task complete")


# ============================================================================
#                    STARTUP GAP FILL (One-time on startup)
# ============================================================================
# Automatically fills gaps > 15 minutes on worker startup.
# 
# Data sources (~300 symbols total):
#   OANDA (49 symbols): Major forex, commodities, indices - NO delay
#   EODHD (255 symbols): Crypto, stocks, ETFs, exotic forex - 12-24h delay
#
# Smart features:
#   - Skips expected market closures (weekends, holidays)
#   - Filters out fake weekend data from EODHD for stocks/forex
#   - EODHD gaps may not fill on first run, but will fill on next day's startup
#   - Symbol list generated dynamically from streaming symbol lists
# ============================================================================

STARTUP_GAP_TASK = "STARTUP_GAP_FILL"
STARTUP_GAP_THRESHOLD_MINUTES = 15
STARTUP_GAP_LOOKBACK_HOURS = 24

# US Market Holidays 2025 (YYYY-MM-DD format)
US_MARKET_HOLIDAYS_2025 = {
    "2025-01-01",  # New Year's Day
    "2025-01-20",  # MLK Day
    "2025-02-17",  # Presidents Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-06-19",  # Juneteenth
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-12-25",  # Christmas
}

# Symbol configurations for gap fill
# OANDA: No delay (major forex, commodities, indices) - fills recent gaps immediately
# EODHD: 12-24h delay (crypto, stocks, ETFs, exotic forex) - fills gaps eventually
# Format: (display_symbol, table_name, api_symbol, source, asset_type)

def build_gap_fill_symbols():
    """
    Dynamically build gap fill symbols from existing streaming symbol lists.
    Returns list of (display_symbol, table_name, api_symbol, source, asset_type) tuples.
    """
    symbols = []
    
    # Helper to convert symbol to table name
    def to_table_name(symbol: str, asset_type: str) -> str:
        if asset_type in ("stock", "etf"):
            return f"candles_{symbol.lower()}"
        else:
            # Forex/crypto: EURUSD -> candles_eur_usd, BTC-USD -> candles_btc_usd
            clean = symbol.replace("-", "").lower()
            # Find the quote currency (USD, EUR, GBP, JPY, etc.)
            quotes = ["usd", "eur", "gbp", "jpy", "cad", "aud", "chf", "nzd", "btc", 
                      "sek", "nok", "dkk", "mxn", "zar", "try", "brl", "pln", "huf", 
                      "czk", "sgd", "hkd", "cnh", "thb", "krw", "inr", "idr"]
            for q in quotes:
                if clean.endswith(q):
                    base = clean[:-len(q)]
                    return f"candles_{base}_{q}"
            return f"candles_{clean}"
    
    # Helper to convert to display symbol
    def to_display_symbol(symbol: str, asset_type: str) -> str:
        if asset_type in ("stock", "etf"):
            return symbol.upper()
        elif "-" in symbol:
            # Already has separator: BTC-USD -> BTC/USD
            return symbol.replace("-", "/")
        else:
            # Forex: EURUSD -> EUR/USD
            quotes = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "NZD", "BTC",
                      "SEK", "NOK", "DKK", "MXN", "ZAR", "TRY", "BRL", "PLN", "HUF",
                      "CZK", "SGD", "HKD", "CNH", "THB", "KRW", "INR", "IDR"]
            upper = symbol.upper()
            for q in quotes:
                if upper.endswith(q):
                    base = upper[:-len(q)]
                    return f"{base}/{q}"
            return upper
    
    # OANDA - Major Forex (no delay, real-time)
    OANDA_FOREX = [
        "EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "USD_CHF", "NZD_USD",
        "EUR_GBP", "EUR_JPY", "GBP_JPY", "AUD_JPY", "EUR_AUD", "GBP_AUD", "EUR_CAD",
        "GBP_CAD", "EUR_CHF", "GBP_CHF", "CAD_JPY", "CHF_JPY", "AUD_CAD", "AUD_CHF",
        "AUD_NZD", "NZD_JPY", "NZD_CAD", "NZD_CHF",
    ]
    for s in OANDA_FOREX:
        display = s.replace("_", "/")
        table = f"candles_{s.lower()}"
        symbols.append((display, table, s, "oanda", "forex"))
    
    # OANDA - Metals
    OANDA_METALS = ["XAU_USD", "XAG_USD", "XPT_USD", "XPD_USD", "XCU_USD"]
    for s in OANDA_METALS:
        display = s.replace("_", "/")
        table = f"candles_{s.lower()}"
        symbols.append((display, table, s, "oanda", "commodity"))
    
    # OANDA - Energy
    OANDA_ENERGY = ["WTICO_USD", "BCO_USD", "NATGAS_USD"]
    for s in OANDA_ENERGY:
        display = s.replace("_", "/")
        table = f"candles_{s.lower()}"
        symbols.append((display, table, s, "oanda", "commodity"))
    
    # OANDA - Agriculture
    OANDA_AGRI = ["CORN_USD", "WHEAT_USD", "SOYBN_USD", "SUGAR_USD"]
    for s in OANDA_AGRI:
        display = s.replace("_", "/")
        table = f"candles_{s.lower()}"
        symbols.append((display, table, s, "oanda", "commodity"))
    
    # OANDA - Indices
    OANDA_INDICES = [
        "US30_USD", "SPX500_USD", "NAS100_USD", "US2000_USD",
        "UK100_GBP", "DE30_EUR", "EU50_EUR", "FR40_EUR",
        "JP225_USD", "AU200_AUD", "HK33_HKD", "CN50_USD",
    ]
    for s in OANDA_INDICES:
        display = s.replace("_", "/")
        table = f"candles_{s.lower()}"
        symbols.append((display, table, s, "oanda", "index"))
    
    # EODHD - All Crypto (from CRYPTO_SYMBOLS)
    for s in CRYPTO_SYMBOLS:
        display = s.replace("-", "/")
        table = to_table_name(s, "crypto")
        api_symbol = f"{s}.CC"
        symbols.append((display, table, api_symbol, "eodhd", "crypto"))
    
    # EODHD - All US Stocks (from US_STOCKS)
    for s in US_STOCKS:
        display = s.upper()
        table = f"candles_{s.lower()}"
        api_symbol = f"{s}.US"
        symbols.append((display, table, api_symbol, "eodhd", "stock"))
    
    # EODHD - All ETFs (from ETFS)
    for s in ETFS:
        display = s.upper()
        table = f"candles_{s.lower()}"
        api_symbol = f"{s}.US"
        symbols.append((display, table, api_symbol, "eodhd", "etf"))
    
    # EODHD - Exotic Forex (pairs not covered by OANDA)
    OANDA_FOREX_SET = {s.replace("_", "").upper() for s in OANDA_FOREX}
    for s in FOREX_SYMBOLS:
        if s.upper() not in OANDA_FOREX_SET:
            display = to_display_symbol(s, "forex")
            table = to_table_name(s, "forex")
            api_symbol = f"{s}.FOREX"
            symbols.append((display, table, api_symbol, "eodhd", "forex"))
    
    return symbols

# Build the gap fill symbol list
STARTUP_GAP_SYMBOLS = build_gap_fill_symbols()


def is_us_stock_market_open(dt: datetime) -> bool:
    """Check if US stock market is open at a given datetime."""
    # Convert to ET
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_et = dt.astimezone(ET)
    
    # Check if it's a holiday
    date_str = dt_et.strftime("%Y-%m-%d")
    if date_str in US_MARKET_HOLIDAYS_2025:
        return False
    
    # Weekend check
    if dt_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    
    # Market hours: 9:30 AM - 4:00 PM ET
    market_open = dt_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = dt_et.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= dt_et <= market_close


def is_forex_market_open(dt: datetime) -> bool:
    """Check if forex market is open at a given datetime."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_et = dt.astimezone(ET)
    
    weekday = dt_et.weekday()
    hour = dt_et.hour
    
    # Forex closed: Friday 5PM ET to Sunday 5PM ET
    if weekday == 4 and hour >= 17:  # Friday after 5PM
        return False
    if weekday == 5:  # Saturday
        return False
    if weekday == 6 and hour < 17:  # Sunday before 5PM
        return False
    
    return True


def is_gap_during_expected_closure(asset_type: str, gap_start: datetime, gap_end: datetime) -> bool:
    """
    Check if a gap falls entirely within expected market closure.
    Returns True if the gap is expected (should be skipped).
    """
    # Crypto never has expected closures
    if asset_type == "crypto":
        return False
    
    # For stocks: check if gap falls outside market hours or on holiday
    if asset_type == "stock":
        # If gap START is outside market hours, it's expected
        if not is_us_stock_market_open(gap_start):
            # Check if gap END is also outside market hours
            # If so, entire gap is during closure
            if not is_us_stock_market_open(gap_end):
                return True
        return False
    
    # For forex, commodities, indices: check weekend closure
    if asset_type in ("forex", "commodity", "index"):
        if not is_forex_market_open(gap_start) and not is_forex_market_open(gap_end):
            return True
        return False
    
    return False


def startup_detect_gaps(table_name: str, lookback_hours: int = STARTUP_GAP_LOOKBACK_HOURS) -> List[Dict]:
    """Detect gaps in candle data for startup fill."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        
        result = sb.table(table_name).select("timestamp").gte(
            "timestamp", since
        ).order("timestamp", desc=False).execute()
        
        if not result.data or len(result.data) < 2:
            return []
        
        gaps = []
        timestamps = []
        
        for r in result.data:
            ts_str = r["timestamp"]
            if ts_str.endswith("Z"):
                ts_str = ts_str.replace("Z", "+00:00")
            elif "+" not in ts_str and "-" not in ts_str[-6:]:
                ts_str = ts_str + "+00:00"
            timestamps.append(datetime.fromisoformat(ts_str))
        
        for i in range(1, len(timestamps)):
            gap_minutes = (timestamps[i] - timestamps[i-1]).total_seconds() / 60
            if gap_minutes > STARTUP_GAP_THRESHOLD_MINUTES:
                gaps.append({
                    "start": timestamps[i-1] + timedelta(minutes=1),
                    "end": timestamps[i] - timedelta(minutes=1),
                    "minutes": int(gap_minutes) - 1
                })
        
        return gaps
        
    except Exception as e:
        debug(STARTUP_GAP_TASK, f"Gap detection error for {table_name}: {e}")
        return []


def startup_fetch_eodhd_candles(api_symbol: str, start_dt: datetime, end_dt: datetime) -> List[Dict]:
    """Fetch candles from EODHD REST API."""
    url = f"https://eodhd.com/api/intraday/{api_symbol}"
    
    params = {
        "api_token": EODHD_API_KEY,
        "interval": "1m",
        "from": int(start_dt.timestamp()),
        "to": int(end_dt.timestamp()),
        "fmt": "json"
    }
    
    debug(STARTUP_GAP_TASK, f"    Fetching: {url}?interval=1m&from={params['from']}&to={params['to']}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            log.warning(f"    EODHD API error: HTTP {response.status_code} for {api_symbol}")
            return []
        
        data = response.json()
        
        # EODHD returns empty list or error object when no data
        if isinstance(data, dict) and "error" in data:
            log.warning(f"    EODHD API error: {data.get('error', 'Unknown error')}")
            return []
        
        if not isinstance(data, list):
            log.warning(f"    EODHD unexpected response type: {type(data)}")
            return []
        
        if len(data) == 0:
            log.info(f"    EODHD returned 0 candles (market may have been closed)")
        else:
            log.info(f"    EODHD returned {len(data)} candles")
        
        return data
        
    except requests.exceptions.Timeout:
        log.warning(f"    EODHD timeout for {api_symbol}")
        return []
    except Exception as e:
        log.warning(f"    EODHD fetch error for {api_symbol}: {e}")
        return []


def startup_fetch_oanda_candles(api_symbol: str, start_dt: datetime, end_dt: datetime) -> List[Dict]:
    """Fetch candles from OANDA REST API."""
    url = f"{OANDA_REST_URL}/v3/instruments/{api_symbol}/candles"
    
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    params = {
        "granularity": "M1",
        "from": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price": "M"
    }
    
    debug(STARTUP_GAP_TASK, f"    Fetching OANDA: {api_symbol} from {params['from']} to {params['to']}")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            log.warning(f"    OANDA API error: HTTP {response.status_code} for {api_symbol}")
            try:
                error_data = response.json()
                log.warning(f"    OANDA error: {error_data.get('errorMessage', 'Unknown')}")
            except:
                pass
            return []
        
        data = response.json()
        candles = data.get("candles", [])
        
        if len(candles) == 0:
            log.info(f"    OANDA returned 0 candles (market may have been closed)")
        else:
            log.info(f"    OANDA returned {len(candles)} candles")
        
        return candles
        
    except requests.exceptions.Timeout:
        log.warning(f"    OANDA timeout for {api_symbol}")
        return []
    except Exception as e:
        log.warning(f"    OANDA fetch error for {api_symbol}: {e}")
        return []


def startup_fill_single_gap(display_symbol: str, table_name: str, api_symbol: str, source: str, asset_type: str, gap: Dict) -> int:
    """Fill a single gap using OANDA or EODHD API."""
    start = gap["start"]
    end = gap["end"]
    
    # Fetch candles based on source
    if source == "oanda":
        candles = startup_fetch_oanda_candles(api_symbol, start, end)
    else:
        candles = startup_fetch_eodhd_candles(api_symbol, start, end)
    
    if not candles:
        return 0
    
    inserted = 0
    errors = 0
    skipped_weekend = 0
    
    for c in candles:
        try:
            if source == "oanda":
                # OANDA format
                if not c.get("complete", False):
                    continue
                
                # Parse OANDA timestamp (handles nanosecond precision)
                ts_str = c.get("time", "")
                if ".000000000Z" in ts_str:
                    ts_str = ts_str.replace(".000000000Z", "+00:00")
                elif "." in ts_str and "Z" in ts_str:
                    parts = ts_str.replace("Z", "").split(".")
                    if len(parts) == 2 and len(parts[1]) > 6:
                        ts_str = parts[0] + "." + parts[1][:6] + "+00:00"
                    else:
                        ts_str = ts_str.replace("Z", "+00:00")
                else:
                    ts_str = ts_str.replace("Z", "+00:00")
                
                ts = datetime.fromisoformat(ts_str)
                mid = c.get("mid", {})
                
                record = {
                    "timestamp": ts.replace(second=0, microsecond=0).isoformat(),
                    "open": float(mid.get("o", 0)),
                    "high": float(mid.get("h", 0)),
                    "low": float(mid.get("l", 0)),
                    "close": float(mid.get("c", 0)),
                    "volume": int(c.get("volume", 0)),
                    "symbol": display_symbol,
                }
            else:
                # EODHD format
                ts = datetime.fromtimestamp(c.get("timestamp", 0), tz=timezone.utc)
                
                # Skip weekend candles for stocks/ETFs and exotic forex (EODHD returns fake weekend data)
                # Stocks/ETFs: Don't trade Saturday/Sunday
                # Forex: Closes Friday 5PM ET (~10PM UTC), opens Sunday 5PM ET (~10PM UTC)
                if asset_type in ("stock", "etf") and ts.weekday() >= 5:  # Saturday=5, Sunday=6
                    skipped_weekend += 1
                    continue
                
                if asset_type == "forex":
                    # Skip Saturday entirely
                    if ts.weekday() == 5:
                        skipped_weekend += 1
                        continue
                    # Skip Sunday before 10PM UTC (forex opens ~5PM ET = 10PM UTC)
                    if ts.weekday() == 6 and ts.hour < 22:
                        skipped_weekend += 1
                        continue
                
                record = {
                    "timestamp": ts.isoformat(),
                    "open": float(c.get("open", 0)),
                    "high": float(c.get("high", 0)),
                    "low": float(c.get("low", 0)),
                    "close": float(c.get("close", 0)),
                    "volume": int(c.get("volume", 0)),
                    "symbol": display_symbol,
                }
            
            sb.table(table_name).upsert(record, on_conflict="timestamp").execute()
            inserted += 1
            
        except Exception as e:
            errors += 1
            if errors <= 3:
                log.warning(f"    Insert error: {e}")
    
    if errors > 3:
        log.warning(f"    ... and {errors - 3} more insert errors")
    
    if skipped_weekend > 0:
        log.info(f"    Skipped {skipped_weekend} weekend candles")
    
    return inserted


def run_startup_gap_fill():
    """
    Main startup gap fill function.
    Scans last 24 hours and fills gaps > 15 minutes.
    Skips gaps that fall during expected market closures.
    """
    log.info("=" * 70)
    log.info("STARTUP GAP FILL - Scanning for data gaps...")
    log.info("=" * 70)
    log.info(f"  Threshold: > {STARTUP_GAP_THRESHOLD_MINUTES} minutes")
    log.info(f"  Lookback: {STARTUP_GAP_LOOKBACK_HOURS} hours")
    log.info(f"  Symbols: {len(STARTUP_GAP_SYMBOLS)}")
    log.info("")
    
    total_gaps = 0
    total_filled = 0
    symbols_with_gaps = []
    
    for display_symbol, table_name, api_symbol, source, asset_type in STARTUP_GAP_SYMBOLS:
        try:
            # Detect gaps
            gaps = startup_detect_gaps(table_name)
            
            if not gaps:
                continue
            
            # Filter out expected closures
            unexpected_gaps = []
            for gap in gaps:
                if is_gap_during_expected_closure(asset_type, gap["start"], gap["end"]):
                    debug(STARTUP_GAP_TASK, f"  {display_symbol}: Skipping expected closure gap")
                else:
                    unexpected_gaps.append(gap)
            
            if not unexpected_gaps:
                continue
            
            total_gaps += len(unexpected_gaps)
            gap_info = f"{display_symbol}({len(unexpected_gaps)})"
            symbols_with_gaps.append(gap_info)
            
            # Fill each gap
            for gap in unexpected_gaps:
                log.info(f"  {display_symbol}: Filling {gap['start'].strftime('%m/%d %H:%M')} → {gap['end'].strftime('%H:%M')} ({gap['minutes']} min)")
                filled = startup_fill_single_gap(display_symbol, table_name, api_symbol, source, asset_type, gap)
                total_filled += filled
                if filled > 0:
                    log.info(f"    → Inserted {filled} candles")
            
            # Rate limiting
            time.sleep(0.3)
            
        except Exception as e:
            log.warning(f"  {display_symbol}: Error - {e}")
    
    log.info("")
    log.info("=" * 70)
    if total_gaps > 0:
        log.info(f"STARTUP GAP FILL COMPLETE: {total_gaps} gaps found, {total_filled} candles inserted")
        if symbols_with_gaps:
            log.info(f"  Symbols with gaps: {', '.join(symbols_with_gaps)}")
    else:
        log.info("STARTUP GAP FILL COMPLETE: No unexpected gaps found ✓")
    log.info("=" * 70)
    log.info("")

# ============================================================================
#                    TASK 12: STOCK FINANCIAL DATA SYNC
# ============================================================================
# Syncs financial data for tracked stocks from EODHD API:
#   - Earnings calendar (daily)
#   - Dividends (daily)
#   - Insider trades (daily)
#   - Financial statements (daily - catches quarterly releases)
#   - Stock news with sentiment (every 6 hours)
#
# UPDATE SCHEDULE (UTC):
#   - Full sync: Daily at 06:00 UTC (before London open)
#   - News refresh: Every 6 hours (06:00, 12:00, 18:00, 00:00)
#   - Weekend: News only at 12:00 UTC
#
# Stocks: AAPL, NVDA, MSFT, AMZN, GOOGL (expandable)
# ============================================================================

STOCK_DATA_TASK = "STOCK_DATA"

# Stocks to track - add more here as needed
# Old:
#   TRACKED_STOCKS = ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"]
#
# New (all 96 US stocks):

TRACKED_STOCKS = [
    # Big Tech (7)
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    
    # Semiconductors (12)
    "AMD", "INTC", "AVGO", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC", "ADI", "MRVL", "ON",
    
    # Enterprise Software (15)
    "CRM", "ADBE", "NOW", "SNOW", "PLTR", "PANW", "CRWD", "ZS", "DDOG", "NET",
    "TEAM", "MDB", "WDAY", "SPLK", "ZM",
    
    # Fintech & Payments (8)
    "V", "MA", "PYPL", "SQ", "COIN", "HOOD", "AFRM", "UPST",
    
    # Banks (8)
    "JPM", "BAC", "WFC", "GS", "MS", "C", "USB", "SCHW",
    
    # Healthcare (10)
    "UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "BMY", "GILD", "AMGN", "BIIB",
    
    # Retail & Consumer (10)
    "WMT", "COST", "TGT", "HD", "LOW", "NKE", "SBUX", "MCD", "DIS", "NFLX",
    
    # Industrial & Aerospace (10)
    "BA", "LMT", "RTX", "GE", "CAT", "DE", "HON", "MMM", "UPS", "FDX",
    
    # Energy (8)
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "PSX", "VLO",
    
    # REITs (4)
    "AMT", "PLD", "CCI", "EQIX",
    
    # Telecom (4)
    "T", "VZ", "TMUS", "CMCSA",
    
    # Auto (4)
    "F", "GM", "RIVN", "LCID",
]

# Sync schedule (UTC hours)
STOCK_DATA_FULL_SYNC_HOUR = 6  # 6 AM UTC - before London opens
STOCK_DATA_NEWS_HOURS = [0, 6, 12, 18]  # News refresh times

# How far back to look for new data
STOCK_DATA_LOOKBACK_DAYS = 30  # For insider trades, news
STOCK_DATA_EARNINGS_DAYS_AHEAD = 90  # Future earnings dates


def stock_data_fetch_eodhd(endpoint: str, params: Dict = None) -> Optional[any]:
    """Generic EODHD API fetcher for stock data."""
    if params is None:
        params = {}
    params["api_token"] = EODHD_API_KEY
    params["fmt"] = "json"
    
    url = f"https://eodhd.com/api/{endpoint}"
    
    try:
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            debug(STOCK_DATA_TASK, f"API error {response.status_code}: {response.text[:100]}")
            return None
    except Exception as e:
        debug(STOCK_DATA_TASK, f"API request failed: {e}")
        return None


def stock_data_safe_float(value, default=None):
    """Safely convert to float."""
    if value is None or value == "" or value == "None":
        return default
    try:
        return float(value)
    except:
        return default


def stock_data_safe_int(value, default=None):
    """Safely convert to int."""
    if value is None or value == "" or value == "None":
        return default
    try:
        return int(float(value))
    except:
        return default


# -----------------------------------------------------------------------------
# EARNINGS CALENDAR SYNC
# -----------------------------------------------------------------------------

def stock_data_sync_earnings() -> Tuple[int, int]:
    """Sync earnings calendar for tracked stocks."""
    debug(STOCK_DATA_TASK, "Syncing earnings calendar...")
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    future = (datetime.now(timezone.utc) + timedelta(days=STOCK_DATA_EARNINGS_DAYS_AHEAD)).strftime("%Y-%m-%d")
    past = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    
    symbols_str = ",".join([f"{s}.US" for s in TRACKED_STOCKS])
    
    data = stock_data_fetch_eodhd("calendar/earnings", {
        "symbols": symbols_str,
        "from": past,
        "to": future
    })
    
    if not data or "earnings" not in data:
        return 0, 0
    
    inserted = 0
    errors = 0
    
    for e in data.get("earnings", []):
        try:
            report_date = e.get("report_date")
            if not report_date:
                continue
            
            code = e.get("code") or ""
            symbol = code.replace(".US", "")
            fiscal_date = e.get("date") or ""
            
            # Determine quarter
            quarter = "Q?"
            year = 0
            if fiscal_date:
                try:
                    dt = datetime.strptime(fiscal_date, "%Y-%m-%d")
                    quarter = f"Q{(dt.month - 1) // 3 + 1}"
                    year = dt.year
                except:
                    pass
            
            eps_est = stock_data_safe_float(e.get("estimate"))
            eps_act = stock_data_safe_float(e.get("actual"))
            
            eps_surprise = None
            eps_surprise_pct = None
            if eps_est is not None and eps_act is not None:
                eps_surprise = round(eps_act - eps_est, 4)
                if eps_est != 0:
                    eps_surprise_pct = round((eps_surprise / abs(eps_est)) * 100, 2)
            
            is_upcoming = report_date >= today
            
            # Build title
            if eps_act is not None and eps_est is not None:
                result = "BEAT" if eps_act > eps_est else "MISS" if eps_act < eps_est else "MET"
                title = f"{symbol} {quarter} {year} Earnings: {result} (${eps_act:.2f} vs ${eps_est:.2f} est)"
            elif eps_est is not None:
                title = f"{symbol} {quarter} {year} Earnings Report (Est: ${eps_est:.2f})"
            else:
                title = f"{symbol} {quarter} {year} Earnings Report"
            
            record = {
                "symbol": symbol,
                "event_date": report_date,
                "event_type": "earnings",
                "event_time": e.get("before_after_market") or None,
                "is_upcoming": is_upcoming,
                "fiscal_quarter": quarter,
                "fiscal_year": year,
                "eps_estimate": eps_est,
                "eps_actual": eps_act,
                "eps_surprise": eps_surprise,
                "eps_surprise_pct": eps_surprise_pct,
                "insider_name": "",
                "shares_traded": 0,
                "title": title[:300],
                "description": f"Fiscal period ending {fiscal_date}" if fiscal_date else None,
                "impact_level": "high",
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            sb.table("financial_calendar").upsert(
                record, 
                on_conflict="symbol,event_date,event_type,insider_name,shares_traded"
            ).execute()
            inserted += 1
            
        except Exception as e:
            errors += 1
            if errors <= 3:
                debug(STOCK_DATA_TASK, f"Earnings insert error: {e}")
    
    return inserted, errors


# -----------------------------------------------------------------------------
# DIVIDENDS SYNC
# -----------------------------------------------------------------------------

def stock_data_sync_dividends() -> Tuple[int, int]:
    """Sync dividend data for tracked stocks."""
    debug(STOCK_DATA_TASK, "Syncing dividends...")
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    past = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    
    inserted = 0
    errors = 0
    
    for symbol in TRACKED_STOCKS:
        try:
            data = stock_data_fetch_eodhd(f"div/{symbol}.US", {"from": past})
            
            if not data:
                continue
            
            for d in data:
                try:
                    ex_date = d.get("date")
                    if not ex_date:
                        continue
                    
                    amount = stock_data_safe_float(d.get("value"), 0)
                    payment_date = d.get("paymentDate")
                    is_upcoming = ex_date >= today
                    
                    # Insert into dividends table
                    div_record = {
                        "symbol": symbol,
                        "dividend_amount": amount,
                        "currency": d.get("currency") or "USD",
                        "declaration_date": d.get("declarationDate"),
                        "ex_dividend_date": ex_date,
                        "record_date": d.get("recordDate"),
                        "payment_date": payment_date,
                        "dividend_type": "Regular",
                    }
                    
                    sb.table("dividends").upsert(
                        div_record,
                        on_conflict="symbol,ex_dividend_date,dividend_amount"
                    ).execute()
                    
                    # Also insert into financial_calendar
                    title = f"{symbol} Ex-Dividend ${amount:.4f}"
                    if payment_date:
                        title += f" (Payment: {payment_date})"
                    
                    cal_record = {
                        "symbol": symbol,
                        "event_date": ex_date,
                        "event_type": "dividend_ex",
                        "event_time": "Market Open",
                        "is_upcoming": is_upcoming,
                        "dividend_amount": amount,
                        "payment_date": payment_date,
                        "insider_name": "",
                        "shares_traded": 0,
                        "title": title[:300],
                        "description": f"Must own shares before {ex_date} to receive dividend",
                        "impact_level": "medium",
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    
                    sb.table("financial_calendar").upsert(
                        cal_record,
                        on_conflict="symbol,event_date,event_type,insider_name,shares_traded"
                    ).execute()
                    
                    inserted += 1
                    
                except Exception as e:
                    errors += 1
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            errors += 1
            debug(STOCK_DATA_TASK, f"Dividend sync error for {symbol}: {e}")
    
    return inserted, errors


# -----------------------------------------------------------------------------
# INSIDER TRADES SYNC
# -----------------------------------------------------------------------------

def stock_data_sync_insider_trades() -> Tuple[int, int]:
    """Sync insider trades for tracked stocks."""
    debug(STOCK_DATA_TASK, "Syncing insider trades...")
    
    cutoff = (datetime.now(timezone.utc) - timedelta(days=STOCK_DATA_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    
    type_map = {
        "P": ("Purchase", "insider_buy"),
        "S": ("Sale", "insider_sell"),
        "A": ("Award", "insider_award"),
        "M": ("Exercise", "insider_exercise"),
        "G": ("Gift", "insider_gift"),
        "D": ("Disposed", "insider_disposed")
    }
    
    inserted = 0
    errors = 0
    
    for symbol in TRACKED_STOCKS:
        try:
            data = stock_data_fetch_eodhd("insider-transactions", {
                "code": f"{symbol}.US",
                "from": cutoff,
                "limit": 100
            })
            
            if not data:
                continue
            
            for t in data:
                try:
                    trans_date = t.get("transactionDate")
                    if not trans_date:
                        continue
                    
                    code = t.get("transactionCode") or ""
                    trans_type, event_type = type_map.get(code, (code, "insider_other"))
                    
                    shares = stock_data_safe_int(t.get("transactionAmount"))
                    price = stock_data_safe_float(t.get("transactionPrice"))
                    total_value = round(shares * price, 2) if shares and price else None
                    
                    insider_name = t.get("ownerName") or "Unknown"
                    insider_title = t.get("ownerTitle") or ""
                    
                    # Insert into insider_trades table
                    trade_record = {
                        "symbol": symbol,
                        "insider_name": insider_name[:200],
                        "insider_title": insider_title[:100] if insider_title else None,
                        "insider_relationship": t.get("ownerRelationship"),
                        "transaction_type": trans_type,
                        "transaction_code": code,
                        "shares_traded": shares,
                        "price_per_share": price,
                        "total_value": total_value,
                        "shares_owned_after": t.get("postTransactionAmount"),
                        "transaction_date": trans_date,
                        "filing_date": t.get("reportDate"),
                        "sec_link": t.get("link"),
                    }
                    
                    sb.table("insider_trades").upsert(
                        trade_record,
                        on_conflict="symbol,insider_name,transaction_date,transaction_code,shares_traded"
                    ).execute()
                    
                    inserted += 1
                    
                except Exception as e:
                    errors += 1
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            errors += 1
            debug(STOCK_DATA_TASK, f"Insider trades sync error for {symbol}: {e}")
    
    return inserted, errors


# -----------------------------------------------------------------------------
# FINANCIAL STATEMENTS SYNC
# -----------------------------------------------------------------------------

def stock_data_sync_financial_statements() -> Tuple[int, int]:
    """Sync financial statements for tracked stocks."""
    debug(STOCK_DATA_TASK, "Syncing financial statements...")
    
    # Only fetch last 2 years for daily updates (full history done separately)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
    
    # All fields for financial_statements table
    all_fields = [
        "symbol", "statement_type", "period_type", "fiscal_date", "currency",
        "total_revenue", "cost_of_revenue", "gross_profit", "operating_income",
        "net_income", "ebitda", "operating_expenses", "research_development",
        "selling_general_admin", "interest_expense", "income_tax_expense",
        "net_interest_income", "total_assets", "total_liabilities", "total_equity",
        "cash_and_equivalents", "short_term_investments", "total_current_assets",
        "total_current_liabilities", "long_term_debt", "short_term_debt",
        "total_debt", "retained_earnings", "common_stock", "inventory",
        "accounts_receivable", "accounts_payable", "operating_cash_flow",
        "investing_cash_flow", "financing_cash_flow", "free_cash_flow",
        "capital_expenditures", "dividends_paid", "stock_repurchased",
        "net_change_in_cash", "depreciation_amortization", "updated_at",
    ]
    
    income_fields = {
        "totalRevenue": "total_revenue", "costOfRevenue": "cost_of_revenue",
        "grossProfit": "gross_profit", "operatingIncome": "operating_income",
        "netIncome": "net_income", "ebitda": "ebitda",
        "operatingExpenses": "operating_expenses", "researchDevelopment": "research_development",
        "sellingGeneralAdministrative": "selling_general_admin",
        "interestExpense": "interest_expense", "incomeTaxExpense": "income_tax_expense",
        "netInterestIncome": "net_interest_income",
    }
    
    balance_fields = {
        "totalAssets": "total_assets", "totalLiab": "total_liabilities",
        "totalStockholderEquity": "total_equity", "cash": "cash_and_equivalents",
        "shortTermInvestments": "short_term_investments",
        "totalCurrentAssets": "total_current_assets",
        "totalCurrentLiabilities": "total_current_liabilities",
        "longTermDebt": "long_term_debt", "shortTermDebt": "short_term_debt",
        "shortLongTermDebtTotal": "total_debt", "retainedEarnings": "retained_earnings",
        "commonStock": "common_stock", "inventory": "inventory",
        "netReceivables": "accounts_receivable", "accountsPayable": "accounts_payable",
    }
    
    cashflow_fields = {
        "totalCashFromOperatingActivities": "operating_cash_flow",
        "totalCashflowsFromInvestingActivities": "investing_cash_flow",
        "totalCashFromFinancingActivities": "financing_cash_flow",
        "freeCashFlow": "free_cash_flow", "capitalExpenditures": "capital_expenditures",
        "dividendsPaid": "dividends_paid", "salePurchaseOfStock": "stock_repurchased",
        "changeInCash": "net_change_in_cash", "depreciation": "depreciation_amortization",
    }
    
    inserted = 0
    errors = 0
    
    for symbol in TRACKED_STOCKS:
        try:
            data = stock_data_fetch_eodhd(f"fundamentals/{symbol}.US")
            
            if not data or "Financials" not in data:
                continue
            
            financials = data.get("Financials", {})
            
            statement_configs = [
                ("Income_Statement", "income_statement", income_fields),
                ("Balance_Sheet", "balance_sheet", balance_fields),
                ("Cash_Flow", "cash_flow", cashflow_fields),
            ]
            
            for eodhd_key, stmt_type, field_map in statement_configs:
                statement_data = financials.get(eodhd_key, {})
                
                for period_type, period_data in [("quarterly", statement_data.get("quarterly", {})),
                                                  ("yearly", statement_data.get("yearly", {}))]:
                    for fiscal_date, values in period_data.items():
                        if fiscal_date < cutoff or not values:
                            continue
                        
                        try:
                            record = {field: None for field in all_fields}
                            record["symbol"] = symbol
                            record["statement_type"] = stmt_type
                            record["period_type"] = period_type
                            record["fiscal_date"] = fiscal_date
                            record["currency"] = values.get("currencySymbol", "USD") or "USD"
                            record["updated_at"] = datetime.now(timezone.utc).isoformat()
                            
                            for eodhd_field, db_field in field_map.items():
                                value = values.get(eodhd_field)
                                if value is not None and value != "None" and value != "":
                                    try:
                                        record[db_field] = int(float(value))
                                    except:
                                        pass
                            
                            sb.table("financial_statements").upsert(
                                record,
                                on_conflict="symbol,statement_type,period_type,fiscal_date"
                            ).execute()
                            inserted += 1
                            
                        except Exception as e:
                            errors += 1
            
            time.sleep(0.3)  # Rate limiting - fundamentals is heavy
            
        except Exception as e:
            errors += 1
            debug(STOCK_DATA_TASK, f"Financial statements sync error for {symbol}: {e}")
    
    return inserted, errors


# -----------------------------------------------------------------------------
# STOCK NEWS SYNC
# -----------------------------------------------------------------------------

def stock_data_sync_news() -> Tuple[int, int]:
    """Sync stock-specific news with sentiment."""
    debug(STOCK_DATA_TASK, "Syncing stock news...")
    
    from_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    
    inserted = 0
    errors = 0
    
    for symbol in TRACKED_STOCKS:
        try:
            data = stock_data_fetch_eodhd("news", {
                "s": f"{symbol}.US",
                "limit": 30,
                "from": from_date
            })
            
            if not data:
                continue
            
            for article in data:
                try:
                    title = article.get("title", "")
                    url = article.get("link")
                    
                    if not title or not url:
                        continue
                    
                    # Parse sentiment
                    sentiment_data = article.get("sentiment", {})
                    if isinstance(sentiment_data, dict):
                        polarity = sentiment_data.get("polarity", 0) or 0
                        sentiment = "positive" if polarity > 0.1 else "negative" if polarity < -0.1 else "neutral"
                        sentiment_score = polarity
                    else:
                        sentiment = "neutral"
                        sentiment_score = 0
                    
                    record = {
                        "symbol": symbol,
                        "title": title[:500],
                        "summary": (article.get("content") or article.get("description", ""))[:1000],
                        "url": url,
                        "source": article.get("source"),
                        "sentiment": sentiment,
                        "sentiment_score": sentiment_score,
                        "published_at": article.get("date"),
                    }
                    
                    sb.table("stock_news").upsert(
                        record,
                        on_conflict="symbol,url"
                    ).execute()
                    inserted += 1
                    
                except Exception as e:
                    errors += 1
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            errors += 1
            debug(STOCK_DATA_TASK, f"Stock news sync error for {symbol}: {e}")
    
    return inserted, errors


# -----------------------------------------------------------------------------
# CALENDAR UPDATE (is_upcoming flag)
# -----------------------------------------------------------------------------

def stock_data_update_calendar_flags():
    """Update is_upcoming flags in financial_calendar."""
    debug(STOCK_DATA_TASK, "Updating calendar flags...")
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        # This requires raw SQL, so we'll do it via REST
        # Mark past events as not upcoming
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        # Update past events
        url = f"{SUPABASE_URL}/rest/v1/financial_calendar?event_date=lt.{today}&is_upcoming=eq.true"
        requests.patch(url, headers=headers, json={"is_upcoming": False}, timeout=30)
        
        debug(STOCK_DATA_TASK, "Calendar flags updated")
        
    except Exception as e:
        debug(STOCK_DATA_TASK, f"Calendar flag update error: {e}")


# -----------------------------------------------------------------------------
# MAIN SYNC ORCHESTRATOR
# -----------------------------------------------------------------------------

def stock_data_should_run_full_sync() -> bool:
    """Check if we should run full sync (daily at 6 AM UTC)."""
    now = datetime.now(timezone.utc)
    # Run at 6:00-6:15 AM UTC
    return now.hour == STOCK_DATA_FULL_SYNC_HOUR and now.minute < 15


def stock_data_should_run_news_sync() -> bool:
    """Check if we should run news sync (every 6 hours)."""
    now = datetime.now(timezone.utc)
    # Run at :00-:15 of scheduled hours
    return now.hour in STOCK_DATA_NEWS_HOURS and now.minute < 15


def stock_data_run_full_sync() -> Dict[str, Tuple[int, int]]:
    """Run full data sync for all data types."""
    results = {}
    
    info(STOCK_DATA_TASK, f"Starting full sync for {len(TRACKED_STOCKS)} stocks...")
    
    # Update calendar flags first
    stock_data_update_calendar_flags()
    
    # Sync earnings
    results["earnings"] = stock_data_sync_earnings()
    info(STOCK_DATA_TASK, f"  Earnings: {results['earnings'][0]} inserted, {results['earnings'][1]} errors")
    
    # Sync dividends
    results["dividends"] = stock_data_sync_dividends()
    info(STOCK_DATA_TASK, f"  Dividends: {results['dividends'][0]} inserted, {results['dividends'][1]} errors")
    
    # Sync insider trades
    results["insider_trades"] = stock_data_sync_insider_trades()
    info(STOCK_DATA_TASK, f"  Insider trades: {results['insider_trades'][0]} inserted, {results['insider_trades'][1]} errors")
    
    # Sync financial statements
    results["financial_statements"] = stock_data_sync_financial_statements()
    info(STOCK_DATA_TASK, f"  Financial statements: {results['financial_statements'][0]} inserted, {results['financial_statements'][1]} errors")
    
    # Sync news
    results["news"] = stock_data_sync_news()
    info(STOCK_DATA_TASK, f"  Stock news: {results['news'][0]} inserted, {results['news'][1]} errors")
    
    total_inserted = sum(r[0] for r in results.values())
    total_errors = sum(r[1] for r in results.values())
    
    info(STOCK_DATA_TASK, f"Full sync complete: {total_inserted} total inserted, {total_errors} total errors")
    
    return results


async def stock_data_sync_task():
    """Task 11: Stock financial data sync."""
    task_metrics = metrics.get_or_create(STOCK_DATA_TASK)
    
    info(STOCK_DATA_TASK, f"Stock data sync task started ({len(TRACKED_STOCKS)} stocks)")
    info(STOCK_DATA_TASK, f"  Stocks: {', '.join(TRACKED_STOCKS)}")
    info(STOCK_DATA_TASK, f"  Full sync: Daily at {STOCK_DATA_FULL_SYNC_HOUR}:00 UTC")
    info(STOCK_DATA_TASK, f"  News sync: Every 6 hours ({STOCK_DATA_NEWS_HOURS})")
    
    last_full_sync = None
    last_news_sync = None
    
    while not tick_streamer._shutdown.is_set():
        try:
            now = datetime.now(timezone.utc)
            loop = asyncio.get_event_loop()
            
            # Check for full sync (daily)
            if stock_data_should_run_full_sync():
                if last_full_sync is None or (now - last_full_sync).total_seconds() > 3600:
                    info(STOCK_DATA_TASK, "Running scheduled full sync...")
                    
                    results = await loop.run_in_executor(None, stock_data_run_full_sync)
                    
                    total_inserted = sum(r[0] for r in results.values())
                    total_errors = sum(r[1] for r in results.values())
                    
                    task_metrics.record_success()
                    task_metrics.custom_metrics.update({
                        "last_full_sync": now.isoformat(),
                        "last_total_inserted": total_inserted,
                        "last_total_errors": total_errors,
                    })
                    
                    last_full_sync = now
                    last_news_sync = now  # News is included in full sync
            
            # Check for news-only sync (every 6 hours, skip if just did full sync)
            elif stock_data_should_run_news_sync():
                if last_news_sync is None or (now - last_news_sync).total_seconds() > 3600:
                    info(STOCK_DATA_TASK, "Running scheduled news sync...")
                    
                    inserted, errors = await loop.run_in_executor(None, stock_data_sync_news)
                    
                    info(STOCK_DATA_TASK, f"News sync complete: {inserted} inserted, {errors} errors")
                    
                    task_metrics.record_success()
                    task_metrics.custom_metrics.update({
                        "last_news_sync": now.isoformat(),
                        "last_news_inserted": inserted,
                    })
                    
                    last_news_sync = now
            
            # Sleep for 5 minutes before checking again
            await asyncio.sleep(300)
            
        except Exception as e:
            error(STOCK_DATA_TASK, f"Task error: {e}", exc_info=DEBUG_MODE)
            task_metrics.record_error(str(e))
            await asyncio.sleep(60)


# ============================================================================
#                    END OF TASK 12: STOCK FINANCIAL DATA SYNC
# ============================================================================


# ============================================================================
#                              MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point."""
    
    # Run startup diagnostics
    run_startup_diagnostics()
    
    # Run startup gap fill (one-time, fills gaps > 15 min from last 24h)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_startup_gap_fill)
    
    log.info("=" * 70)
    log.info("LONDON STRATEGIC EDGE - WORKER v9.1 (DEBUG)")
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
        # asyncio.create_task(financial_news_task(), name="news"),  # DISABLED - uses non-existent financial_news table, replaced by news_articles_task
        asyncio.create_task(gap_fill_task(), name="gap_fill"),
        asyncio.create_task(whale_flow_task(), name="whale"),
        asyncio.create_task(ai_news_desk_task(), name="ai_news"),
        asyncio.create_task(sector_sentiment_task(), name="sentiment"),
        asyncio.create_task(news_articles_task(), name="news_articles"),
        asyncio.create_task(oanda_streaming_task(), name="oanda_stream"),
        asyncio.create_task(bond_yields_streaming_task(), name="bond_4h"),
        asyncio.create_task(stock_data_sync_task(), name="stock_data"),  # NEW: Stock financial data sync
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
