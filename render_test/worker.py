_health_metrics = {
    "last_tick_time": None,
    "ticks_per_minute": 0,
    "gaps_detected": 0,
    "gaps_filled": 0,
    "backfills_completed": 0,
    "ws_reconnects": 0,
    "last_gap_check": None,
    "startup_time": "...",
}
```

---

### 🚀 Startup Sequence
```
1. Worker starts
2. 24-hour candle backfill runs (all 16 symbols)
3. All 6 tasks start in parallel
4. Watchdog monitors tick flow
5. Gap detection runs every 5 min
```

---

### 📋 Expected Logs
```
======================================================================
🚀 LONDON STRATEGIC EDGE - BULLETPROOF WORKER v4.0
======================================================================
Starting 6 parallel tasks:
   1️⃣  Tick streaming (TwelveData + Watchdog)
   2️⃣  Economic calendar (Quarter-hour scraping)
   3️⃣  Financial news (15 RSS feeds + DeepSeek)
   4️⃣  Macro data (FRED API)
   5️⃣  AI Market Briefs (DeepSeek - 30 min)
   6️⃣  Gap Detection & Auto-Backfill
----------------------------------------------------------------------
🔧 Reliability Settings:
   • Watchdog timeout: 30s
   • Startup backfill: 24 hours
   • Gap check interval: 300s
   • Gap scan window: 6 hours
======================================================================
🔄 Running startup backfill...
   ⬇️ Fetching BTC/USD (attempt 1)...
   ✅ BTC/USD: 1440 candles backfilled
   ⬇️ Fetching ETH/USD (attempt 1)...
   ✅ ETH/USD: 1440 candles backfilled
   ...
✅ Backfill complete: 23040 candles for 16/16 symbols
✅ Startup backfill complete - starting all tasks
📡 Tick streaming task started with watchdog
🚀 WebSocket connected - Subscribed to 16 symbols
📅 Economic calendar scraper started
📰 Financial news scraper started
📊 Macro data task started
📝 Market brief generator started (every 30 minutes)
🔍 Gap detection task started (every 300s)
