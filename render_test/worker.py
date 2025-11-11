import os, json, asyncio, logging
from datetime import datetime, timezone
import websockets
from supabase import create_client

# ── Env ──────────────────────────────────────────────────────────────────────
TD_API_KEY   = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Write *only* here
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "ticks_crypto")

# Symbols to stream (edit if you like)
SYMBOLS = (
    "BTC/USD,ETH/USD,XRP/USD,XMR/USD,SOL/USD,BNB/USD,ADA/USD,"
    "XAU/USD,EUR/USD,GBP/USD,USD/CAD,GBP/AUD,AUD/CAD,EUR/GBP,USD/JPY"
)

WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

# ── Supabase client ──────────────────────────────────────────────────────────
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Batching ─────────────────────────────────────────────────────────────────
BATCH_MAX = 500
BATCH_FLUSH_SECS = 2

batch = []
lock = asyncio.Lock()
stop_event = asyncio.Event()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("td-stream")

def _to_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _to_ts(ts_val):
    """Return aware UTC datetime from ms/s epoch or ISO string."""
    if ts_val is None:
        return datetime.now(timezone.utc)
    # epoch milliseconds or seconds
    try:
        t = float(ts_val)
        if t > 10**12:
            t /= 1000.0
        return datetime.fromtimestamp(t, tz=timezone.utc)
    except Exception:
        pass
    # ISO
    try:
        # If TZ missing, assume UTC
        dt = datetime.fromisoformat(str(ts_val).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

async def flush():
    global batch
    async with lock:
        if not batch:
            return
        payload, batch = batch, []

    try:
        sb.table(SUPABASE_TABLE).insert(payload).execute()
        log.info("✅ Inserted %d rows into %s", len(payload), SUPABASE_TABLE)
    except Exception:
        # Put rows back if insert fails
        log.exception("❌ Insert failed; re-queueing %d rows", len(payload))
        async with lock:
            batch[:0] = payload  # prepend to keep order

async def periodic_flush():
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=BATCH_FLUSH_SECS)
        except asyncio.TimeoutError:
            await flush()

async def handle_message(msg: dict):
    # We only care about price events
    if msg.get("event") != "price":
        return

    row = {
        "symbol": msg.get("symbol"),
        "ts": _to_ts(msg.get("timestamp")).isoformat(),
        "price": _to_float(msg.get("price")),
        "bid": _to_float(msg.get("bid")),
        "ask": _to_float(msg.get("ask")),
        # Try multiple common keys for volume
        "day_volume": (
            _to_float(msg.get("day_volume"))
            or _to_float(msg.get("dayVolume"))
            or _to_float(msg.get("volume"))
        ),
    }

    # Require at least symbol + price
    if not row["symbol"] or row["price"] is None:
        return

    async with lock:
        batch.append(row)
        if len(batch) >= BATCH_MAX:
            await flush()

async def run_once():
    async with websockets.connect(
        WS_URL, ping_interval=20, ping_timeout=20, max_queue=1000
    ) as ws:
        # Subscribe to symbols
        await ws.send(
            json.dumps({"action": "subscribe", "params": {"symbols": SYMBOLS}})
        )
        log.info("🚀 Subscribed to: %s", SYMBOLS)

        flusher = asyncio.create_task(periodic_flush())
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                await handle_message(msg)
        finally:
            stop_event.set()
            await flush()
            flusher.cancel()

async def main():
    backoff = 1
    while not stop_event.is_set():
        try:
            await run_once()
            backoff = 1
        except Exception as e:
            log.warning("⚠️ WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())

