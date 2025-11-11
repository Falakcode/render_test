import os, json, asyncio, signal, logging
from datetime import datetime, timezone
import websockets
from supabase import create_client, Client

TD_API_KEY = os.environ["TWELVEDATA_API_KEY"]

# All 16 symbols
SYMBOLS = "BTC/USD,ETH/USD,XRP/USD,XMR/USD,SOL/USD,BNB/USD,ADA/USD,DOGE/USD,XAU/USD,EUR/USD,GBP/USD,USD/CAD,GBP/AUD,AUD/CAD,EUR/GBP,USD/JPY"

WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_TABLE = "ticks_crypto"

BATCH_MAX = 200
BATCH_FLUSH_SECS = 5

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("td-stream")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
_batch = []
_batch_lock = asyncio.Lock()
_stop = asyncio.Event()

def _to_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

async def flush_batch():
    global _batch
    async with _batch_lock:
        if not _batch:
            return
        payload, _batch = _batch, []
    try:
        sb.table(SUPABASE_TABLE).insert(payload).execute()
        log.info("✅ Inserted %d ticks into ticks_crypto", len(payload))
    except Exception as e:
        log.exception("❌ Insert failed; re-queueing %d rows", len(payload))
        async with _batch_lock:
            _batch.extend(payload)

async def periodic_flush():
    while not _stop.is_set():
        try:
            await asyncio.wait_for(_stop.wait(), timeout=BATCH_FLUSH_SECS)
        except asyncio.TimeoutError:
            await flush_batch()

async def handle_message(msg: dict):
    if msg.get("event") == "price":
        ts = msg.get("timestamp")
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            dt = datetime.now(timezone.utc)

        # Only store symbol, ts, and price
        row = {
            "symbol": msg.get("symbol"),
            "ts": dt.isoformat(),
            "price": _to_float(msg.get("price")),
        }

        if row["price"] is None:
            log.warning("⚠️ Skipping tick with no price: %s", msg.get("symbol"))
            return

        async with _batch_lock:
            _batch.append(row)
            if len(_batch) >= BATCH_MAX:
                asyncio.create_task(flush_batch())

    elif msg.get("event") == "subscribe-status":
        log.info("📊 Subscribe status: %s", msg)
    else:
        log.debug("Other event: %s", msg)

async def heartbeat(ws):
    while not _stop.is_set():
        await asyncio.sleep(10)
        try:
            await ws.send(json.dumps({"action": "heartbeat"}))
        except Exception:
            return

async def run_once():
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        max_queue=1000,
    ) as ws:
        await ws.send(
            json.dumps({"action": "subscribe", "params": {"symbols": SYMBOLS}})
        )
        log.info("🚀 Subscribed to: %s", SYMBOLS)

        hb_task = asyncio.create_task(heartbeat(ws))

        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                log.warning("Non-JSON message: %s", raw)
                continue
            await handle_message(data)

        hb_task.cancel()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop.set)

    flusher = asyncio.create_task(periodic_flush())

    backoff = 1
    while not _stop.is_set():
        try:
            await run_once()
            backoff = 1
        except Exception as e:
            log.warning("⚠️ WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    await flush_batch()
    flusher.cancel()

if __name__ == "__main__":
    asyncio.run(main())
