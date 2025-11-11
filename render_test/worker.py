# worker.py
import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import websockets
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

async def main():
    backoff = 1
    while not _stop.is_set():
        try:
            await _run_once()
            backoff = 1
        except Exception as e:
            log.warning("⚠️ WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

if __name__ == "__main__":
    asyncio.run(main())


