import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional, List, Dict
from decimal import Decimal, ROUND_DOWN, InvalidOperation

import websockets
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import feedparser
import openai

# ------------------------------ ENV ------------------------------
TD_API_KEY   = os.environ["TWELVEDATA_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
FRED_API_KEY = os.environ.get("FRED_API_KEY")

# Allow override, but default to the table you requested
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# The 16 pairs to stream
# The 16 pairs to stream
SYMBOLS = (
    "BTC/USD,ETH/USD,XRP/USD,XMR/USD,SOL/USD,BNB/USD,ADA/USD,DOGE/USD,"
    "XAU/USD,EUR/USD,GBP/USD,USD/CAD,GBP/AUD,AUD/CAD,EUR/GBP,USD/JPY"
)

# Request maximum precision from TwelveData (8 decimal places)
# This ensures we get full precision for forex (5dp) and small crypto (8dp)
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}&dp=8"

# Economic calendar scraping interval (in seconds)
ECON_SCRAPE_INTERVAL = 3600  # Scrape every hour

# FRED API Base URL
FRED_API_BASE = "https://api.stlouisfed.org/fred"

# --------------------------- SYMBOL CLASSIFICATION -------------------------
# Forex pairs (5 decimal places for micropip precision)
FOREX_PAIRS = {
    "EUR/USD", "GBP/USD", "USD/CAD", "GBP/AUD", "AUD/CAD", "EUR/GBP"
}

# JPY pairs (3 decimal places - JPY is different)
JPY_PAIRS = {
    "USD/JPY", "EUR/JPY", "GBP/JPY", "AUD/JPY"
}

# Commodities (2 decimal places)
COMMODITY_PAIRS = {
    "XAU/USD", "XAG/USD"
}

# Crypto pairs (dynamic based on price)
CRYPTO_PAIRS = {
    "BTC/USD", "ETH/USD", "XRP/USD", "XMR/USD", "SOL/USD", 
    "BNB/USD", "ADA/USD", "DOGE/USD"
}

def get_decimal_places(symbol: str, price_value: Optional[Decimal] = None) -> int:
    """
    Determine the appropriate number of decimal places for a symbol.
    
    - Forex: 5 decimal places (micropip precision)
    - JPY pairs: 3 decimal places
    - Commodities (Gold/Silver): 2 decimal places
    - Crypto >= $1: 2 decimal places
    - Crypto < $1: 5 decimal places (or more for very small values)
    """
    if symbol in FOREX_PAIRS:
        return 5  # Micropip precision for forex
    elif symbol in JPY_PAIRS:
        return 3  # JPY pairs use 3dp
    elif symbol in COMMODITY_PAIRS:
        return 2  # Gold/Silver use 2dp
    elif symbol in CRYPTO_PAIRS:
        if price_value is not None:
            if price_value < Decimal("0.001"):
                return 8  # Very small crypto (like some altcoins)
            elif price_value < Decimal("1"):
                return 5  # Small crypto under $1
            else:
                return 2  # Crypto >= $1
        return 2  # Default for crypto
    else:
        return 5  # Default to 5dp for unknown symbols

# --------------------------- CLIENTS/LOG -------------------------
sb = create_client(SUPABASE_URL, SUPABASE_KEY)
deepseek_client = openai.OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com") if DEEPSEEK_API_KEY else None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("tick-stream")

# ----------------------- BATCH / FLUSH SETTINGS ------------------
BATCH_MAX = 500
BATCH_FLUSH_SECS = 2

_batch = []
_lock = asyncio.Lock()
_stop = asyncio.Event()

# ======================== PRECISION-AWARE TICK STREAMING ========================

def _to_decimal(x) -> Optional[Decimal]:
    """
    Convert value to Decimal, preserving full precision.
    This is critical for forex micropip movements.
    """
    if x is None:
        return None
    try:
        # Convert to string first to preserve precision
        # (avoids float conversion which loses precision)
        if isinstance(x, (int, float)):
            # For floats, convert via string repr to preserve digits
            return Decimal(repr(x)) if isinstance(x, float) else Decimal(x)
        else:
            return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

def _format_price(symbol: str, value: Optional[Decimal]) -> Optional[float]:
    """
    Format price with appropriate decimal places for the symbol type.
    Returns float for JSON serialization but with controlled precision.
    """
    if value is None:
        return None
    
    dp = get_decimal_places(symbol, value)
    # Round to appropriate decimal places
    quantize_str = '0.' + '0' * dp
    rounded = value.quantize(Decimal(quantize_str), rounding=ROUND_DOWN)
    return float(rounded)

def _to_float(x) -> Optional[float]:
    """
    Legacy float conversion for non-price fields (volume, etc.)
    """
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _to_ts(v):
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

async def _flush():
    global _batch
    async with _lock:
        if not _batch:
            return
        payload, _batch = _batch, []

    try:
        sb.table(SUPABASE_TABLE).insert(payload).execute()
        log.info("✅ Inserted %d rows into %s", len(payload), SUPABASE_TABLE)
    except Exception:
        log.exception("❌ Insert failed, re-queuing %d rows", len(payload))
        async with _lock:
            _batch[:0] = payload

async def _periodic_flush():
    while not _stop.is_set():
        try:
            await asyncio.wait_for(_stop.wait(), timeout=BATCH_FLUSH_SECS)
        except asyncio.TimeoutError:
            await _flush()

async def _handle(msg: dict):
    if msg.get("event") != "price":
        return

    symbol = msg.get("symbol")
    if not symbol:
        return
    
    # Use Decimal for price precision
    raw_price = msg.get("price")
    price_decimal = _to_decimal(raw_price)
    
    if price_decimal is None:
        return
    
    # Format price with appropriate decimal places for this symbol
    formatted_price = _format_price(symbol, price_decimal)
    
    # Also format bid/ask with same precision
    bid_decimal = _to_decimal(msg.get("bid"))
    ask_decimal = _to_decimal(msg.get("ask"))
    
    row = {
        "symbol": symbol,
        "ts": _to_ts(msg.get("timestamp")).isoformat(),
        "price": formatted_price,
        "bid": _format_price(symbol, bid_decimal) if bid_decimal else None,
        "ask": _format_price(symbol, ask_decimal) if ask_decimal else None,
        "day_volume": (
            _to_float(msg.get("day_volume"))
            or _to_float(msg.get("dayVolume"))
            or _to_float(msg.get("volume"))
        ),
    }

    # Debug logging for forex pairs to verify precision
    if symbol in FOREX_PAIRS and raw_price:
        log.debug(f"📊 {symbol}: raw={raw_price} -> formatted={formatted_price} (5dp)")

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
        await ws.send(json.dumps({"action": "subscribe", "params": {"symbols": SYMBOLS}}))
        log.info("🚀 Subscribed to: %s", SYMBOLS)
        log.info("📐 Precision settings: Forex=5dp, JPY=3dp, Crypto>=1=2dp, Crypto<1=5dp")

        flusher = asyncio.create_task(_periodic_flush())
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                await _handle(data)
        finally:
            _stop.set()
            await _flush()
            flusher.cancel()

async def tick_streaming_task():
    """Main tick streaming task with reconnection logic."""
    backoff = 1
    while not _stop.is_set():
        try:
            await _run_once()
            backoff = 1
        except Exception as e:
            log.warning("⚠️ WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

# ====================== ECONOMIC CALENDAR ======================

def scrape_trading_economics():
    """Scrape economic calendar data from Trading Economics"""
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        log.info("🌐 Fetching Trading Economics calendar...")
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

        log.info(f"📊 Scraped {len(events)} events from Trading Economics")
        return events

    except Exception as e:
        log.error(f"❌ Error scraping Trading Economics: {e}")
        return []

def upsert_economic_events(events):
    """Insert or update economic events in Supabase."""
    if not events:
        return

    try:
        for event in events:
            try:
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
                else:
                    sb.table('Economic_calander').insert(event).execute()

            except Exception as e:
                log.warning(f"⚠️ Error upserting event {event['event']}: {e}")
                continue

        log.info(f"✅ Successfully processed {len(events)} economic events")

    except Exception as e:
        log.error(f"❌ Error upserting events to Supabase: {e}")

async def economic_calendar_task():
    """Periodically scrape and update economic calendar."""
    log.info("🗓️ Economic calendar scraper started")

    while not _stop.is_set():
        try:
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, scrape_trading_economics)

            if events:
                await loop.run_in_executor(None, upsert_economic_events, events)

            await asyncio.sleep(ECON_SCRAPE_INTERVAL)

        except Exception as e:
            log.error(f"❌ Error in economic calendar task: {e}")
            await asyncio.sleep(60)

# ==================== FINANCIAL NEWS SCRAPER ====================

RSS_FEEDS = [
    {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC News (Main)"},
    {"url": "http://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC News (Business)"},
    {"url": "http://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC News (World)"},
    {"url": "https://www.reuters.com/rssfeed/businessNews", "name": "Reuters (Business)"},
    {"url": "https://www.reuters.com/rssfeed/worldNews", "name": "Reuters (World)"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC (Top News)"},
    {"url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "name": "CNBC (World)"},
    {"url": "https://www.cnbc.com/id/15837362/device/rss/rss.html", "name": "CNBC (US News)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines", "name": "MarketWatch (Real-time)"},
    {"url": "http://feeds.marketwatch.com/marketwatch/topstories", "name": "MarketWatch (Top Stories)"},
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk"},
    {"url": "https://cointelegraph.com/rss", "name": "Cointelegraph"},
    {"url": "https://www.theguardian.com/world/rss", "name": "The Guardian (World)"},
    {"url": "https://www.theguardian.com/uk/business/rss", "name": "The Guardian (Business)"},
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera"},
]

INCLUDE_KEYWORDS = [
    "war", "attack", "invasion", "military", "strike", "missile", "conflict", "terrorism", "coup",
    "trump", "biden", "election", "president", "prime minister", "government", "resign",
    "fed", "federal reserve", "powell", "ecb", "lagarde", "bank of england", "rate", "interest",
    "monetary", "qe", "quantitative", "inflation", "cpi", "ppi", "gdp", "unemployment", "jobs",
    "nfp", "payroll", "retail sales", "economic", "recession", "tariff", "sanction", "trade war",
    "import", "export", "ban", "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "crash", "plunge", "surge", "rally", "collapse", "soar", "tumble", "selloff",
    "oil", "opec", "gold", "crude", "brent", "wti", "commodity", "bank", "banking", "bailout",
    "liquidity", "credit", "earthquake", "hurricane", "disaster", "emergency", "crisis", "pandemic",
    "outbreak",
]

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "cricket", "tennis", "basketball", "baseball",
    "celebrity", "actor", "actress", "movie", "film", "tv show", "television",
    "recipe", "cooking", "fashion", "style", "beauty", "wedding", "divorce", "dating",
]

def get_news_scan_interval():
    now = datetime.utcnow()
    day_of_week = now.weekday()
    hour_utc = now.hour
    
    if day_of_week >= 5:
        return 14400, "weekend"
    
    if (8 <= hour_utc < 20) or (0 <= hour_utc < 1):
        return 900, "active"
    
    return 1800, "quiet"

def should_prefilter_article(title: str, summary: str) -> bool:
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
    if not articles:
        return []

    log.info(f"💰 Cost optimization: Starting with {len(articles)} articles")

    articles_with_images = [a for a in articles if a.get('image_url')]
    removed_no_images = len(articles) - len(articles_with_images)
    log.info(f"   Removed {removed_no_images} articles without images")
    log.info(f"   Remaining with images: {len(articles_with_images)}")

    unique_articles = []
    seen_topics = []

    for article in articles_with_images:
        is_duplicate = False
        for seen_title in seen_topics:
            similarity = calculate_title_similarity(article['title'], seen_title)
            if similarity >= 0.75:
                log.debug(f"   🔄 SIMILAR: {article['title'][:50]}... ({int(similarity*100)}% match)")
                is_duplicate = True
                break

        if not is_duplicate:
            unique_articles.append(article)
            seen_topics.append(article['title'])

    removed_similar = len(articles_with_images) - len(unique_articles)
    log.info(f"   Removed {removed_similar} similar articles")

    category_counts = {'crypto': 0, 'political': 0, 'central_bank': 0, 'market': 0}
    category_limits = {'crypto': 2, 'political': 2, 'central_bank': 2, 'market': 2}

    crypto_kw = ['bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'solana', 'xrp']
    political_kw = ['trump', 'biden', 'election', 'president', 'white house']
    central_bank_kw = ['fed', 'powell', 'ecb', 'lagarde', 'rate', 'monetary']
    market_kw = ['stock', 'nasdaq', 's&p', 'dow', 'market']

    final_articles = []
    for article in unique_articles:
        text = f"{article['title']} {article['summary']}".lower()

        category = None
        if any(kw in text for kw in crypto_kw):
            category = 'crypto'
        elif any(kw in text for kw in political_kw):
            category = 'political'
        elif any(kw in text for kw in central_bank_kw):
            category = 'central_bank'
        elif any(kw in text for kw in market_kw):
            category = 'market'

        if category and category in category_limits:
            if category_counts[category] >= category_limits[category]:
                log.debug(f"   🚫 LIMIT: {article['title'][:50]}... ({category} limit reached)")
                continue
            category_counts[category] += 1

        final_articles.append(article)

    removed_by_limit = len(unique_articles) - len(final_articles)
    total_removed = len(articles) - len(final_articles)
    savings_pct = int((total_removed / len(articles)) * 100) if articles else 0

    log.info(f"   Category breakdown: {dict(category_counts)}")
    log.info(f"   Removed {removed_by_limit} articles by category limits")
    log.info(f"   💰 Total savings: {total_removed} articles ({savings_pct}% cost reduction)")
    log.info(f"   ✅ Final articles to classify: {len(final_articles)}")

    return final_articles

def fetch_rss_feed(feed_url: str, source_name: str) -> list:
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; FinancialNewsBot/1.0)'}
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

def classify_article_with_deepseek(article: dict) -> dict:
    """Classify article using DeepSeek API (80% cheaper than Claude)"""
    if not deepseek_client:
        log.warning("⚠️ DeepSeek API key not configured")
        return None
        
    try:
        prompt = f"""You are a professional financial news analyst. Analyze this article and determine if it will impact financial markets.

TITLE: {article['title']}
SUMMARY: {article['summary']}
SOURCE: {article['source']}

Respond with ONLY valid JSON (no markdown, no explanation):
{{"is_important": true, "event_type": "war", "impact_level": "high", "affected_symbols": ["BTC/USD", "XAU/USD"], "sentiment": "bearish", "summary": "2-3 sentence market impact summary", "keywords": ["keyword1", "keyword2"]}}

Valid event_types: war, political_shock, economic_data, central_bank, trade_war, commodity_shock, market_crash, crypto_crash, crypto_rally, natural_disaster, bank_crisis, energy_crisis, pandemic_health, market_rally
Valid impact_levels: critical, high, medium, low
Valid sentiments: bullish, bearish, neutral"""

        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.1
        )

        response_text = response.choices[0].message.content.strip()
        
        # Clean markdown if present
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()
        if response_text.endswith("```"):
            response_text = response_text[:-3].strip()

        return json.loads(response_text)

    except json.JSONDecodeError as e:
        log.warning(f"⚠️ DeepSeek JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"⚠️ DeepSeek API error: {e}")
        return None


def store_news_article(article: dict, classification: dict) -> bool:
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
    log.info("📰 Financial news scraper started (smart scheduling + cost optimization)")

    while not _stop.is_set():
        try:
            interval, session_name = get_news_scan_interval()
            session_emoji = "🔥" if session_name == "active" else "😴" if session_name == "quiet" else "🌙"
            
            log.info(f"🔍 Starting news scan cycle ({session_emoji} {session_name} session)")

            all_articles = []
            loop = asyncio.get_event_loop()

            for feed in RSS_FEEDS:
                articles = await loop.run_in_executor(None, fetch_rss_feed, feed['url'], feed['name'])
                all_articles.extend(articles)
                await asyncio.sleep(0.5)

            log.info(f"📊 Fetched {len(all_articles)} total articles")

            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article['url'] not in seen_urls:
                    seen_urls.add(article['url'])
                    unique_articles.append(article)

            log.info(f"🔗 After deduplication: {len(unique_articles)} unique articles")

            filtered_articles = [
                a for a in unique_articles
                if should_prefilter_article(a['title'], a['summary'])
            ]

            log.info(f"🎯 After pre-filtering: {len(filtered_articles)} articles")

            filtered_articles = optimize_articles_for_cost(filtered_articles)

            stored_count = 0
            classified_count = 0

            for article in filtered_articles:
                classification = await loop.run_in_executor(None, classify_article_with_deepseek, article)
                classified_count += 1

                if not classification:
                    continue

                if not classification['is_important'] or classification['impact_level'] not in ['critical', 'high']:
                    continue

                impact_emoji = "🚨" if classification['impact_level'] == 'critical' else "⚡"
                log.info(f"{impact_emoji} IMPORTANT: {article['title'][:60]}... ({classification['impact_level']})")

                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored_count += 1

                await asyncio.sleep(1)

            log.info(f"✅ News cycle complete: {stored_count} stored, {classified_count} classified")

            log.info(f"😴 Next scan in {interval // 60} minutes ({session_emoji} {session_name} session)")
            await asyncio.sleep(interval)

        except Exception as e:
            log.error(f"❌ Error in financial news task: {e}")
            await asyncio.sleep(60)

# ==================== FRED MACRO DATA SCRAPER ====================

def fetch_fred_series_no_vintage(
    series_id: str,
    observation_start: str,
    observation_end: str,
    limit: Optional[int] = None
) -> List[Dict]:
    if not FRED_API_KEY:
        log.error("❌ FRED_API_KEY not set!")
        return []
    
    url = f"{FRED_API_BASE}/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': observation_start,
        'observation_end': observation_end,
        'sort_order': 'desc'
    }
    
    if limit:
        params['limit'] = limit
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            log.warning(f"No observations found for {series_id}")
            return []
        
        observations = [
            {'date': obs['date'], 'value': obs['value']}
            for obs in data['observations']
            if obs['value'] != '.'
        ]
        
        return observations
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            log.warning(f"⚠️ FRED API 400 error for {series_id}")
        else:
            log.error(f"Error fetching FRED series {series_id}: {e}")
        return []
    except Exception as e:
        log.error(f"Error fetching FRED series {series_id}: {e}")
        return []


def fetch_fred_series(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    limit: Optional[int] = None,
    vintage_date: Optional[str] = None
) -> List[Dict]:
    if not FRED_API_KEY:
        log.error("❌ FRED_API_KEY not set!")
        return []
    
    if not observation_start:
        observation_start = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
    if not observation_end:
        observation_end = datetime.now().strftime('%Y-%m-%d')
    if not vintage_date:
        vintage_date = datetime.now().strftime('%Y-%m-%d')
    
    url = f"{FRED_API_BASE}/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': observation_start,
        'observation_end': observation_end,
        'sort_order': 'desc',
        'realtime_start': vintage_date,
        'realtime_end': vintage_date
    }
    
    if limit:
        params['limit'] = limit
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            log.warning(f"No observations found for {series_id}")
            return []
        
        observations = [
            {'date': obs['date'], 'value': obs['value'], 'vintage_date': vintage_date}
            for obs in data['observations']
            if obs['value'] != '.'
        ]
        
        return observations
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            log.warning(f"⚠️ FRED API 400 error for {series_id}")
        else:
            log.error(f"Error fetching FRED series {series_id}: {e}")
        return []
    except Exception as e:
        log.error(f"Error fetching FRED series {series_id}: {e}")
        return []


def calculate_changes(observations: List[Dict], frequency: str) -> List[Dict]:
    sorted_obs = sorted(observations, key=lambda x: x['date'])
    
    for i, obs in enumerate(sorted_obs):
        try:
            current_value = float(obs['value'])
            
            if frequency == 'Monthly' and i > 0:
                prev_value = float(sorted_obs[i-1]['value'])
                obs['change_mom'] = ((current_value - prev_value) / prev_value) * 100
            
            if frequency == 'Quarterly' and i > 0:
                prev_value = float(sorted_obs[i-1]['value'])
                obs['change_qoq'] = ((current_value - prev_value) / prev_value) * 100
            
            if frequency == 'Monthly' and i >= 12:
                year_ago_value = float(sorted_obs[i-12]['value'])
                obs['change_yoy'] = ((current_value - year_ago_value) / year_ago_value) * 100
            elif frequency == 'Quarterly' and i >= 4:
                year_ago_value = float(sorted_obs[i-4]['value'])
                obs['change_yoy'] = ((current_value - year_ago_value) / year_ago_value) * 100
        except (ValueError, ZeroDivisionError) as e:
            log.warning(f"Error calculating changes for {obs['date']}: {e}")
            continue
    
    return sorted_obs


def format_value(value: float, unit: str, display_format: str) -> str:
    if display_format == 'YoY%' and 'change_yoy' in value:
        return f"{value['change_yoy']:.1f}%"
    elif display_format == 'MoM%' and 'change_mom' in value:
        return f"{value['change_mom']:.1f}%"
    elif display_format == 'QoQ%' and 'change_qoq' in value:
        return f"{value['change_qoq']:.1f}%"
    elif unit == 'Percent':
        return f"{float(value):.2f}%"
    elif unit == 'Billions of Dollars':
        return f"${float(value)/1000:.2f}T"
    elif unit == 'Millions of Dollars':
        return f"${float(value)/1000:.2f}B"
    elif unit == 'Thousands of Persons' or unit == 'Thousands of Units':
        return f"{float(value):.0f}K"
    else:
        return f"{float(value):.2f}"


def get_active_indicators() -> List[Dict]:
    try:
        result = sb.table('macro_indicator_metadata').select('*').eq('is_active', True).execute()
        return result.data if result.data else []
    except Exception as e:
        log.error(f"Error fetching indicator metadata: {e}")
        return []


def get_latest_date_for_indicator(fred_code: str) -> Optional[str]:
    try:
        result = sb.table('macro_indicators').select('date').eq(
            'indicator_code', fred_code
        ).order('date', desc=True).limit(1).execute()
        
        if result.data and len(result.data) > 0:
            return result.data[0]['date']
        return None
    except Exception as e:
        log.error(f"Error getting latest date for {fred_code}: {e}")
        return None


def store_observations(indicator: Dict, observations: List[Dict]) -> int:
    if not observations:
        return 0
    
    rows = []
    for obs in observations:
        try:
            value_float = float(obs['value'])
            vintage_dt = obs.get('vintage_date')
            
            formatted = format_value(obs['value'], indicator['unit'], indicator['display_format'])
            
            row = {
                'country': indicator['country'],
                'indicator_name': indicator['indicator_name'],
                'indicator_code': indicator['fred_code'],
                'date': obs['date'],
                'vintage_value': value_float,
                'vintage_date': vintage_dt,
                'vintage_formatted': formatted,
                'vintage_change_yoy': obs.get('change_yoy'),
                'actual_value': value_float,
                'actual_date': vintage_dt,
                'actual_formatted': formatted,
                'actual_change_yoy': obs.get('change_yoy'),
                'unit': indicator['unit'],
                'frequency': indicator['frequency']
            }
            rows.append(row)
        except Exception as e:
            log.warning(f"Error preparing row for {obs['date']}: {e}")
            continue
    
    if not rows:
        return 0
    
    try:
        sb.table('macro_indicators').upsert(rows, on_conflict='indicator_code,date').execute()
        log.info(f"✅ Stored {len(rows)} observations for {indicator['indicator_name']}")
        return len(rows)
    except Exception as e:
        log.error(f"Error storing observations for {indicator['fred_code']}: {e}")
        return 0


async def backfill_historical_data():
    log.info("📈 Starting historical data backfill (5 years)...")
    
    try:
        result = sb.table('macro_indicators').select('id', count='exact').limit(1).execute()
        if result.count and result.count > 0:
            log.info("✅ Historical data already exists, skipping backfill")
            return
    except Exception as e:
        log.warning(f"Could not check existing data: {e}")
    
    indicators = get_active_indicators()
    if not indicators:
        log.error("❌ No active indicators found in metadata table")
        return
    
    log.info(f"📊 Backfilling {len(indicators)} indicators...")
    
    total_stored = 0
    loop = asyncio.get_event_loop()
    
    today = datetime.now()
    observation_end = today.replace(day=1).strftime('%Y-%m-%d')
    observation_start = (today - timedelta(days=365*5)).strftime('%Y-%m-%d')
    today_vintage = today.strftime('%Y-%m-%d')
    
    for indicator in indicators:
        log.info(f"⬇️  Fetching {indicator['indicator_name']} ({indicator['fred_code']})...")
        
        observations = await loop.run_in_executor(
            None, fetch_fred_series_no_vintage, indicator['fred_code'],
            observation_start, observation_end
        )
        
        if not observations:
            log.warning(f"⚠️  No data found for {indicator['indicator_name']}")
            continue
        
        for obs in observations:
            obs['vintage_date'] = today_vintage
        
        observations_with_changes = calculate_changes(observations, indicator['frequency'])
        
        stored = await loop.run_in_executor(
            None, store_observations, indicator, observations_with_changes
        )
        
        total_stored += stored
        await asyncio.sleep(0.5)
    
    log.info(f"✅ Backfill complete! Stored {total_stored} total observations")


def get_todays_macro_events() -> List[Dict]:
    today = datetime.now().strftime('%Y-%m-%d')
    
    try:
        result = sb.table('Economic_calander').select('*').eq('date', today).execute()
        
        if not result.data:
            return []
        
        indicators = get_active_indicators()
        
        matched_events = []
        for event in result.data:
            event_name = event.get('event', '').lower()
            event_country = event.get('country', '')
            
            if event_country != 'US':
                continue
            
            for indicator in indicators:
                keywords = indicator.get('calendar_event_keywords', [])
                if any(keyword.lower() in event_name for keyword in keywords):
                    matched_events.append({'calendar_event': event, 'indicator': indicator})
                    break
        
        return matched_events
    
    except Exception as e:
        log.error(f"Error getting today's macro events: {e}")
        return []


async def check_and_update_indicator(indicator: Dict, calendar_event: Dict):
    fred_code = indicator['fred_code']
    latest_date = get_latest_date_for_indicator(fred_code)
    
    observation_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    today_vintage = datetime.now().strftime('%Y-%m-%d')
    
    loop = asyncio.get_event_loop()
    observations = await loop.run_in_executor(
        None, fetch_fred_series, fred_code, observation_start,
        datetime.now().strftime('%Y-%m-%d'), None, today_vintage
    )
    
    if not observations:
        log.info(f"🔭 No new data yet for {indicator['indicator_name']}")
        return
    
    latest_obs_date = observations[0]['date']
    
    if latest_date and latest_obs_date <= latest_date:
        log.info(f"✅ Data for {indicator['indicator_name']} is up to date")
        return
    
    log.info(f"🆕 NEW DATA: {indicator['indicator_name']} - {latest_obs_date} = {observations[0]['value']}")
    
    observations_with_changes = calculate_changes(observations, indicator['frequency'])
    
    stored = await loop.run_in_executor(
        None, store_observations, indicator, observations_with_changes
    )
    
    if stored > 0:
        try:
            actual_value = format_value(
                observations[0]['value'], indicator['unit'], indicator['display_format']
            )
            sb.table('Economic_calander').update({
                'actual': actual_value, 'fred_indicator_code': fred_code
            }).eq('id', calendar_event['id']).execute()
            log.info(f"✅ Updated calendar: {indicator['indicator_name']} = {actual_value}")
        except Exception as e:
            log.warning(f"Could not update calendar: {e}")


async def macro_data_task():
    log.info("📅 Macro data event checker started")
    
    await backfill_historical_data()
    
    while not _stop.is_set():
        try:
            log.info("🔍 Checking economic calendar for today's macro events...")
            
            matched_events = get_todays_macro_events()
            
            if not matched_events:
                log.info("🔭 No macro events scheduled for today")
            else:
                log.info(f"📊 Found {len(matched_events)} macro events today")
                
                for event in matched_events:
                    indicator = event['indicator']
                    calendar_event = event['calendar_event']
                    log.info(f"⏰ Checking {indicator['indicator_name']} (scheduled today)...")
                    await check_and_update_indicator(indicator, calendar_event)
                    await asyncio.sleep(1)
            
            log.info("😴 Next check in 1 hour...")
            await asyncio.sleep(3600)
        
        except Exception as e:
            log.error(f"❌ Error in macro data task: {e}")
            await asyncio.sleep(300)

# ====================== MAIN ======================

async def main():
    log.info("🚀 Starting worker with 4 parallel tasks:")
    log.info("   1️⃣ Tick streaming (TwelveData) - Forex=5dp, Crypto=dynamic")
    log.info("   2️⃣ Economic calendar (Trading Economics)")
    log.info("   3️⃣ Financial news (15 RSS feeds + smart scheduling)")
    log.info("   4️⃣ Macro data (FRED API - US indicators)")
    log.info("   📐 Precision: Forex=5dp | JPY=3dp | Crypto>=$1=2dp | Crypto<$1=5dp")

    tick_task = asyncio.create_task(tick_streaming_task())
    econ_task = asyncio.create_task(economic_calendar_task())
    news_task = asyncio.create_task(financial_news_task())
    macro_task = asyncio.create_task(macro_data_task())

    try:
        await asyncio.gather(tick_task, econ_task, news_task, macro_task)
    except Exception as e:
        log.error(f"❌ Main loop error: {e}")
    finally:
        _stop.set()
        tick_task.cancel()
        econ_task.cancel()
        news_task.cancel()
        macro_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
