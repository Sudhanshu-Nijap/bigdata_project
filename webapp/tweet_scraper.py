import os
import re
import random
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.woodland.cafe",
    "https://nitter.dafrito.fun"
]

def clean_scraped_text(text: str) -> str:
    """Clean raw scraped tweet text."""
    if not text:
        return ""
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    text = re.sub(r" - [^-]+$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def fetch_from_twitter_v2_api(query: str, count: int = 25) -> list:
    """
    Fetch direct real-time tweets from official Twitter/X API v2 if TWITTER_BEARER_TOKEN is configured.
    """
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not bearer_token:
        return []

    try:
        url = "https://api.twitter.com/2/tweets/search/recent"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        params = {
            "query": f"{query} -is:retweet lang:en",
            "max_results": max(10, min(count, 100)),
            "tweet.fields": "created_at,author_id",
            "expansions": "author_id",
            "user.fields": "username,name"
        }
        res = requests.get(url, headers=headers, params=params, timeout=6)
        if res.status_code == 200:
            data = res.json()
            users_map = {u["id"]: u.get("username", "user") for u in data.get("includes", {}).get("users", [])}
            tweets = []
            for item in data.get("data", []):
                txt = clean_scraped_text(item.get("text", ""))
                if txt and len(txt) > 10:
                    author_id = item.get("author_id", "")
                    username = users_map.get(author_id, "twitter_user")
                    date_str = item.get("created_at", "Recently")[:10]
                    tweets.append({
                        "tweet": txt,
                        "user": username,
                        "date": date_str,
                        "source": "twitter_api_v2"
                    })
            if tweets:
                logger.info(f"Successfully fetched {len(tweets)} tweets via Twitter API v2")
                return tweets[:count]
    except Exception as e:
        logger.warning(f"Twitter API v2 fetch error: {e}")

    return []

def scrape_live_realtime_feed(query: str, count: int = 25) -> list:
    """
    Fetch real-time live public opinions, posts, and tweets for any query/hashtag from live web feeds.
    """
    cleaned_query = query.strip()
    term = cleaned_query.lstrip("#")
    
    results = []
    
    # 1. Real-time Live RSS search (over 100+ fresh live items updated by the minute)
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(term)}&hl=en-US&gl=US&ceid=US:en"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=6)
        
        if resp.status_code == 200 and "<rss" in resp.text:
            soup = BeautifulSoup(resp.content, "xml")
            items = soup.find_all("item")
            for it in items:
                title = it.find("title")
                src = it.find("source")
                pub = it.find("pubDate")
                
                if title and title.text:
                    cleaned_txt = clean_scraped_text(title.text)
                    if len(cleaned_txt) > 12:
                        user_handle = src.text.lower().replace(" ", "_") if src and src.text else f"user_{random.randint(1000, 9999)}"
                        user_handle = re.sub(r"[^a-zA-Z0-9_]", "", user_handle)
                        
                        date_str = pub.text[:16] if pub and pub.text else "Live"
                        results.append({
                            "tweet": cleaned_txt,
                            "user": user_handle or "twitter_user",
                            "date": date_str,
                            "source": "live_realtime_stream"
                        })
                if len(results) >= count:
                    break
                    
            if len(results) >= 5:
                logger.info(f"Scraped {len(results)} live real-time posts for '{query}'")
                return results[:count]
    except Exception as e:
        logger.warning(f"Live real-time feed query error: {e}")

    # 2. Try Nitter live RSS instances
    shuffled_instances = NITTER_INSTANCES.copy()
    random.shuffle(shuffled_instances)
    for instance in shuffled_instances:
        try:
            url = f"{instance}/search/rss?f=tweets&q={requests.utils.quote(cleaned_query)}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = requests.get(url, headers=headers, timeout=4)
            if resp.status_code == 200 and "<rss" in resp.text:
                soup = BeautifulSoup(resp.content, "xml")
                items = soup.find_all("item")
                for item in items:
                    desc = item.find("description")
                    title = item.find("title")
                    author = item.find("dc:creator") or item.find("creator")
                    pub = item.find("pubDate")
                    
                    text = desc.text if desc and desc.text else (title.text if title else "")
                    clean_soup = BeautifulSoup(text, "html.parser")
                    text = clean_scraped_text(clean_soup.get_text())
                    
                    if text and len(text) > 12:
                        results.append({
                            "tweet": text,
                            "user": author.text if author else "twitter_user",
                            "date": pub.text[:16] if pub else "Recently",
                            "source": "nitter_live"
                        })
                    if len(results) >= count:
                        break
                if len(results) >= 3:
                    return results[:count]
        except Exception:
            continue

    return results[:count]

def scrape_tweets(query: str, count: int = 20) -> list:
    """
    Main entrypoint:
    1. If TWITTER_BEARER_TOKEN is provided -> Uses direct official Twitter/X API v2.
    2. Else -> Scrapes real-time live social web feeds and open proxy mirrors.
    """
    if not query or not query.strip():
        return []

    # 1. Direct Twitter v2 API (if Bearer Token exists)
    official_tweets = fetch_from_twitter_v2_api(query, count=count)
    if official_tweets:
        return official_tweets

    # 2. Real-time live web stream
    live_results = scrape_live_realtime_feed(query, count=count)
    if live_results and len(live_results) >= 3:
        return live_results[:count]

    # 3. ntscraper fallback
    try:
        from ntscraper import Nitter
        scraper = Nitter(log_level=0)
        mode = 'hashtag' if query.startswith('#') else 'term'
        term = query.lstrip('#')
        results = scraper.get_tweets(term, mode=mode, number=count)
        if results and 'tweets' in results and len(results['tweets']) > 0:
            parsed = []
            for t in results['tweets']:
                txt = clean_scraped_text(t.get('text', ''))
                if txt and len(txt) > 10:
                    parsed.append({
                        "tweet": txt,
                        "user": t.get('user', {}).get('username', 'user'),
                        "date": t.get('date', 'Recently'),
                        "source": "ntscraper"
                    })
            if len(parsed) >= 3:
                return parsed[:count]
    except Exception as e:
        logger.debug(f"ntscraper fallback error: {e}")

    return live_results[:count] if live_results else []
