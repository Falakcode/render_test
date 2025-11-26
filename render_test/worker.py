import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Optional, List, Dict

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
FRED_API_KEY = os.environ.get("FRED_API_KEY")  # NEW!

# Allow override, but default to the table you requested
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "tickdata")

# The 16 pairs to stream
SYMBOLS = (
    "BTC/USD,ETH/USD,XRP/USD,XMR/USD,SOL/USD,BNB/USD,ADA/USD,DOGE/USD,"
    "XAU/USD,EUR/USD,GBP/USD,USD/CAD,GBP/AUD,AUD/CAD,EUR/GBP,USD/JPY"
)

WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

# Economic calendar scraping interval (in seconds)
ECON_SCRAPE_INTERVAL = 3600  # Scrape every hour

# FRED API Base URL
FRED_API_BASE = "https://api.stlouisfed.org/fred"

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

# ======================== TICK STREAMING (UNCHANGED) ========================

def _to_float(x):
    """Convert to float preserving up to 8 decimal places from TwelveData API."""
    if x is None:
        return None
    try:
        # Use Decimal for precise intermediate conversion
        # This preserves full precision from TwelveData (up to 8dp for crypto, 5dp for forex)
        return float(Decimal(str(x)))
    except (InvalidOperation, ValueError, TypeError):
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
        log.info("Ã¢Å“â€¦ Inserted %d rows into %s", len(payload), SUPABASE_TABLE)
    except Exception:
        log.exception("Ã¢ÂÅ’ Insert failed, re-queuing %d rows", len(payload))
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

    row = {
        "symbol": msg.get("symbol"),
        "ts": _to_ts(msg.get("timestamp")).isoformat(),
        "price": _to_float(msg.get("price")),
        "bid": _to_float(msg.get("bid")),
        "ask": _to_float(msg.get("ask")),
        "day_volume": (
            _to_float(msg.get("day_volume"))
            or _to_float(msg.get("dayVolume"))
            or _to_float(msg.get("volume"))
        ),
    }

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
        await ws.send(json.dumps({"action": "subscribe", "params": {"symbols": SYMBOLS}}))
        log.info("Ã°Å¸Å¡â‚¬ Subscribed to: %s", SYMBOLS)

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
            log.warning("Ã¢Å¡Â Ã¯Â¸Â WS error: %s; reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

# ====================== ECONOMIC CALENDAR (UNCHANGED) ======================

def scrape_trading_economics():
    """Scrape economic calendar data from Trading Economics"""
    try:
        url = "https://tradingeconomics.com/calendar"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        log.info("Ã°Å¸Å’Â Fetching Trading Economics calendar...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': 'calendar'})
        if not table:
            log.error("Ã¢ÂÅ’ Could not find calendar table")
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
                log.warning(f"Ã¢Å¡Â Ã¯Â¸Â Error parsing row: {e}")
                continue

        log.info(f"Ã°Å¸â€œÅ  Scraped {len(events)} events from Trading Economics")
        return events

    except Exception as e:
        log.error(f"Ã¢ÂÅ’ Error scraping Trading Economics: {e}")
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
                log.warning(f"Ã¢Å¡Â Ã¯Â¸Â Error upserting event {event['event']}: {e}")
                continue

        log.info(f"Ã¢Å“â€¦ Successfully processed {len(events)} economic events")

    except Exception as e:
        log.error(f"Ã¢ÂÅ’ Error upserting events to Supabase: {e}")

async def economic_calendar_task():
    """Periodically scrape and update economic calendar."""
    log.info("Ã°Å¸â€”â€œÃ¯Â¸Â Economic calendar scraper started")

    while not _stop.is_set():
        try:
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, scrape_trading_economics)

            if events:
                await loop.run_in_executor(None, upsert_economic_events, events)

            await asyncio.sleep(ECON_SCRAPE_INTERVAL)

        except Exception as e:
            log.error(f"Ã¢ÂÅ’ Error in economic calendar task: {e}")
            await asyncio.sleep(60)

# ==================== FINANCIAL NEWS SCRAPER (UNCHANGED) ====================

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

    log.info(f"Ã°Å¸â€™Â° Cost optimization: Starting with {len(articles)} articles")

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
                log.debug(f"   Ã°Å¸â€â€ž SIMILAR: {article['title'][:50]}... ({int(similarity*100)}% match)")
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
                log.debug(f"   Ã°Å¸Å¡Â« LIMIT: {article['title'][:50]}... ({category} limit reached)")
                continue
            category_counts[category] += 1

        final_articles.append(article)

    removed_by_limit = len(unique_articles) - len(final_articles)
    total_removed = len(articles) - len(final_articles)
    savings_pct = int((total_removed / len(articles)) * 100) if articles else 0

    log.info(f"   Category breakdown: {dict(category_counts)}")
    log.info(f"   Removed {removed_by_limit} articles by category limits")
    log.info(f"   Ã°Å¸â€™Â° Total savings: {total_removed} articles ({savings_pct}% cost reduction)")
    log.info(f"   Ã¢Å“â€¦ Final articles to classify: {len(final_articles)}")

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
        log.warning(f"Ã¢Å¡Â Ã¯Â¸Â RSS feed error ({source_name}): {e}")
        return []

def classify_article_with_deepseek(article: dict) -> dict:
    """Classify article using DeepSeek API (80% cheaper than Claude)"""
    if not deepseek_client:
        log.warning("âš ï¸ DeepSeek API key not configured")
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
        log.warning(f"âš ï¸ DeepSeek JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"âš ï¸ DeepSeek API error: {e}")
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
            log.warning(f"Ã¢Å¡Â Ã¯Â¸Â Database error storing news: {e}")
            return False

async def financial_news_task():
    log.info("Ã°Å¸â€œÂ° Financial news scraper started (smart scheduling + cost optimization)")

    while not _stop.is_set():
        try:
            interval, session_name = get_news_scan_interval()
            session_emoji = "Ã°Å¸â€Â¥" if session_name == "active" else "Ã°Å¸ËœÂ´" if session_name == "quiet" else "Ã°Å¸Å’â„¢"
            
            log.info(f"Ã°Å¸â€Â Starting news scan cycle ({session_emoji} {session_name} session)")

            all_articles = []
            loop = asyncio.get_event_loop()

            for feed in RSS_FEEDS:
                articles = await loop.run_in_executor(None, fetch_rss_feed, feed['url'], feed['name'])
                all_articles.extend(articles)
                await asyncio.sleep(0.5)

            log.info(f"Ã°Å¸â€œÅ  Fetched {len(all_articles)} total articles")

            seen_urls = set()
            unique_articles = []
            for article in all_articles:
                if article['url'] not in seen_urls:
                    seen_urls.add(article['url'])
                    unique_articles.append(article)

            log.info(f"Ã°Å¸â€â€” After deduplication: {len(unique_articles)} unique articles")

            filtered_articles = [
                a for a in unique_articles
                if should_prefilter_article(a['title'], a['summary'])
            ]

            log.info(f"Ã°Å¸Å½Â¯ After pre-filtering: {len(filtered_articles)} articles")

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

                impact_emoji = "Ã°Å¸Å¡Â¨" if classification['impact_level'] == 'critical' else "Ã¢Å¡Â¡"
                log.info(f"{impact_emoji} IMPORTANT: {article['title'][:60]}... ({classification['impact_level']})")

                if await loop.run_in_executor(None, store_news_article, article, classification):
                    stored_count += 1

                await asyncio.sleep(1)

            log.info(f"Ã¢Å“â€¦ News cycle complete: {stored_count} stored, {classified_count} classified")

            log.info(f"Ã°Å¸ËœÂ´ Next scan in {interval // 60} minutes ({session_emoji} {session_name} session)")
            await asyncio.sleep(interval)

        except Exception as e:
            log.error(f"Ã¢ÂÅ’ Error in financial news task: {e}")
            await asyncio.sleep(60)

# ==================== FRED MACRO DATA SCRAPER (NEW - TASK 4) ====================

def fetch_fred_series_no_vintage(
    series_id: str,
    observation_start: str,
    observation_end: str,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Fetch FRED data WITHOUT vintage constraints (gets latest revisions)
    Used for backfill to avoid 400 errors from realtime parameters
    """
    if not FRED_API_KEY:
        log.error("Ã¢ÂÅ’ FRED_API_KEY not set!")
        return []
    
    url = f"{FRED_API_BASE}/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': observation_start,
        'observation_end': observation_end,
        'sort_order': 'desc'
        # NO realtime parameters - gets latest revisions
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
            {
                'date': obs['date'], 
                'value': obs['value']
            }
            for obs in data['observations']
            if obs['value'] != '.'  # Filter out missing values
        ]
        
        return observations
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            log.warning(f"Ã¢Å¡Â Ã¯Â¸Â FRED API 400 error for {series_id} (likely no data available for requested dates)")
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
    """
    Fetch time series data from FRED API V2 with vintage support
    
    Args:
        vintage_date: Date for vintage snapshot (YYYY-MM-DD)
                     If None, gets latest revisions
                     If set, gets data as it was known on that date
    """
    if not FRED_API_KEY:
        log.error("Ã¢ÂÅ’ FRED_API_KEY not set!")
        return []
    
    if not observation_start:
        observation_start = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
    
    if not observation_end:
        observation_end = datetime.now().strftime('%Y-%m-%d')
    
    # Use today as vintage_date if not specified (for backfill)
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
        # Vintage parameters (as-of date)
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
            {
                'date': obs['date'], 
                'value': obs['value'],
                'vintage_date': vintage_date  # Track when this snapshot was taken
            }
            for obs in data['observations']
            if obs['value'] != '.'
        ]
        
        return observations
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            log.warning(f"Ã¢Å¡Â Ã¯Â¸Â FRED API 400 error for {series_id} (likely no data available for requested dates)")
        else:
            log.error(f"Error fetching FRED series {series_id}: {e}")
        return []
    except Exception as e:
        log.error(f"Error fetching FRED series {series_id}: {e}")
        return []


def calculate_changes(observations: List[Dict], frequency: str) -> List[Dict]:
    """Calculate MoM%, QoQ%, and YoY% changes"""
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
    """Format value for display"""
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
    """Get all active macro indicators from metadata table"""
    try:
        result = sb.table('macro_indicator_metadata').select('*').eq('is_active', True).execute()
        return result.data if result.data else []
    except Exception as e:
        log.error(f"Error fetching indicator metadata: {e}")
        return []


def get_latest_date_for_indicator(fred_code: str) -> Optional[str]:
    """Get the most recent date we have data for a specific indicator"""
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
    """
    Store FRED observations with 2-column vintage design:
    - vintage_value/vintage_date: Original published value (frozen)
    - actual_value/actual_date: Latest revised value (updated)
    
    For backfill: both vintage and actual are the same (today's revisions)
    """
    if not observations:
        return 0
    
    rows = []
    for obs in observations:
        try:
            value_float = float(obs['value'])
            vintage_dt = obs.get('vintage_date')
            
            # Format the value
            formatted = format_value(
                obs['value'], 
                indicator['unit'], 
                indicator['display_format']
            )
            
            # For backfill: vintage = actual (both same initially)
            row = {
                'country': indicator['country'],
                'indicator_name': indicator['indicator_name'],
                'indicator_code': indicator['fred_code'],
                'date': obs['date'],
                
                # VINTAGE (frozen, original)
                'vintage_value': value_float,
                'vintage_date': vintage_dt,
                'vintage_formatted': formatted,
                'vintage_change_yoy': obs.get('change_yoy'),
                
                # ACTUAL (same as vintage for backfill)
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
        # UPSERT based on indicator_code + date (one row per date)
        result = sb.table('macro_indicators').upsert(
            rows,
            on_conflict='indicator_code,date'  # One row per date
        ).execute()
        
        inserted = len(rows)
        log.info(f"Ã¢Å“â€¦ Stored {inserted} observations for {indicator['indicator_name']}")
        return inserted
    
    except Exception as e:
        log.error(f"Error storing observations for {indicator['fred_code']}: {e}")
        return 0


async def backfill_historical_data():
    """One-time backfill: Fetch last 5 years of data for all indicators"""
    log.info("Ã°Å¸â€â€ž Starting historical data backfill (5 years)...")
    
    try:
        result = sb.table('macro_indicators').select('id', count='exact').limit(1).execute()
        if result.count and result.count > 0:
            log.info("Ã¢Å“â€¦ Historical data already exists, skipping backfill")
            return
    except Exception as e:
        log.warning(f"Could not check existing data: {e}")
    
    indicators = get_active_indicators()
    if not indicators:
        log.error("Ã¢ÂÅ’ No active indicators found in metadata table")
        return
    
    log.info(f"Ã°Å¸â€œÅ  Backfilling {len(indicators)} indicators...")
    
    total_stored = 0
    loop = asyncio.get_event_loop()
    
    # Use dates without realtime constraints for backfill
    # End date = first day of current month (data is typically 1-2 months behind)
    today = datetime.now()
    observation_end = today.replace(day=1).strftime('%Y-%m-%d')  # First of this month
    observation_start = (today - timedelta(days=365*5)).strftime('%Y-%m-%d')  # 5 years ago
    today_vintage = today.strftime('%Y-%m-%d')
    
    for indicator in indicators:
        log.info(f"Ã¢Â¬â€¡Ã¯Â¸Â  Fetching {indicator['indicator_name']} ({indicator['fred_code']})...")
        
        # Fetch without vintage constraints (gets latest revisions)
        observations = await loop.run_in_executor(
            None,
            fetch_fred_series_no_vintage,  # Use simplified version
            indicator['fred_code'],
            observation_start,
            observation_end
        )
        
        if not observations:
            log.warning(f"Ã¢Å¡Â Ã¯Â¸Â  No data found for {indicator['indicator_name']}")
            continue
        
        # Add vintage_date to all observations (today's date)
        for obs in observations:
            obs['vintage_date'] = today_vintage
        
        observations_with_changes = calculate_changes(observations, indicator['frequency'])
        
        stored = await loop.run_in_executor(
            None,
            store_observations,
            indicator,
            observations_with_changes
        )
        
        total_stored += stored
        await asyncio.sleep(0.5)  # Rate limiting
    
    log.info(f"Ã¢Å“â€¦ Backfill complete! Stored {total_stored} total observations")


def get_todays_macro_events() -> List[Dict]:
    """Get economic calendar events happening today that match FRED indicators"""
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
            
            # FRED only has US data, skip non-US events
            if event_country != 'US':
                continue
            
            for indicator in indicators:
                keywords = indicator.get('calendar_event_keywords', [])
                if any(keyword.lower() in event_name for keyword in keywords):
                    matched_events.append({
                        'calendar_event': event,
                        'indicator': indicator
                    })
                    break
        
        return matched_events
    
    except Exception as e:
        log.error(f"Error getting today's macro events: {e}")
        return []


async def check_and_update_indicator(indicator: Dict, calendar_event: Dict):
    """Check FRED for new data and update if available (with vintage tracking)"""
    fred_code = indicator['fred_code']
    
    latest_date = get_latest_date_for_indicator(fred_code)
    
    observation_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    today_vintage = datetime.now().strftime('%Y-%m-%d')  # Today's date as vintage
    
    loop = asyncio.get_event_loop()
    observations = await loop.run_in_executor(
        None,
        fetch_fred_series,
        fred_code,
        observation_start,
        datetime.now().strftime('%Y-%m-%d'),
        None,
        today_vintage  # Pass vintage date (new releases captured with today's date)
    )
    
    if not observations:
        log.info(f"Ã°Å¸â€œÂ­ No new data yet for {indicator['indicator_name']}")
        return
    
    latest_obs_date = observations[0]['date']
    
    if latest_date and latest_obs_date <= latest_date:
        log.info(f"Ã¢Å“â€¦ Data for {indicator['indicator_name']} is up to date")
        return
    
    log.info(f"Ã°Å¸â€ â€¢ NEW DATA: {indicator['indicator_name']} - {latest_obs_date} = {observations[0]['value']} (vintage: {today_vintage})")
    
    observations_with_changes = calculate_changes(observations, indicator['frequency'])
    
    stored = await loop.run_in_executor(
        None,
        store_observations,
        indicator,
        observations_with_changes
    )
    
    if stored > 0:
        try:
            actual_value = format_value(
                observations[0]['value'],
                indicator['unit'],
                indicator['display_format']
            )
            
            sb.table('Economic_calander').update({
                'actual': actual_value,
                'fred_indicator_code': fred_code
            }).eq('id', calendar_event['id']).execute()
            
            log.info(f"Ã¢Å“â€¦ Updated calendar: {indicator['indicator_name']} = {actual_value}")
        except Exception as e:
            log.warning(f"Could not update calendar: {e}")


async def macro_data_task():
    """
    Event-driven macro data updater (TASK 4)
    - Backfills historical data on first run
    - Checks economic calendar for today's events every hour
    - Fetches new data from FRED when scheduled
    """
    log.info("Ã°Å¸â€œâ€¦ Macro data event checker started")
    
    await backfill_historical_data()
    
    while not _stop.is_set():
        try:
            log.info("Ã°Å¸â€Â Checking economic calendar for today's macro events...")
            
            matched_events = get_todays_macro_events()
            
            if not matched_events:
                log.info("Ã°Å¸â€œÂ­ No macro events scheduled for today")
            else:
                log.info(f"Ã°Å¸â€œÅ  Found {len(matched_events)} macro events today")
                
                for event in matched_events:
                    indicator = event['indicator']
                    calendar_event = event['calendar_event']
                    
                    log.info(f"Ã¢ÂÂ° Checking {indicator['indicator_name']} (scheduled today)...")
                    
                    await check_and_update_indicator(indicator, calendar_event)
                    
                    await asyncio.sleep(1)
            
            log.info("Ã°Å¸ËœÂ´ Next check in 1 hour...")
            await asyncio.sleep(3600)
        
        except Exception as e:
            log.error(f"Ã¢ÂÅ’ Error in macro data task: {e}")
            await asyncio.sleep(300)

# ====================== MAIN ======================

async def main():
    """
    Run all FOUR tasks in parallel:
    1. Tick streaming (websockets)
    2. Economic calendar scraping
    3. Financial news scraping (with smart scheduling)
    4. Macro data scraping (FRED API - event-driven)
    """
    log.info("Ã°Å¸Å¡â‚¬ Starting worker with 4 parallel tasks:")
    log.info("   1Ã¯Â¸ÂÃ¢Æ’Â£ Tick streaming (TwelveData)")
    log.info("   2Ã¯Â¸ÂÃ¢Æ’Â£ Economic calendar (Trading Economics)")
    log.info("   3Ã¯Â¸ÂÃ¢Æ’Â£ Financial news (15 RSS feeds + smart scheduling)")
    log.info("   4Ã¯Â¸ÂÃ¢Æ’Â£ Macro data (FRED API - US indicators)")
    log.info("   Ã°Å¸â€œâ€¦ Smart schedule: 15min (active) | 30min (quiet) | 4hr (weekend)")

    tick_task = asyncio.create_task(tick_streaming_task())
    econ_task = asyncio.create_task(economic_calendar_task())
    news_task = asyncio.create_task(financial_news_task())
    macro_task = asyncio.create_task(macro_data_task())  # NEW!

    try:
        await asyncio.gather(tick_task, econ_task, news_task, macro_task)
    except Exception as e:
        log.error(f"Ã¢ÂÅ’ Main loop error: {e}")
    finally:
        _stop.set()
        tick_task.cancel()
        econ_task.cancel()
        news_task.cancel()
        macro_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
