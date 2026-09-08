import os
import sys
import json
import time
import argparse
from kafka import KafkaProducer

# Configuration
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "numtest")
STREAM_DELAY = float(os.getenv("STREAM_INTERVAL_SECONDS", "3.0"))

DEFAULT_TOPICS = ["#AI", "#Tesla", "#Crypto", "#Bitcoin", "Apple", "#Tech", "#Nvidia"]

def get_scraper():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    try:
        from webapp.tweet_scraper import scrape_tweets
    except ImportError:
        from tweet_scraper import scrape_tweets
    return scrape_tweets

def stream_scraped_batch(producer, scrape_fn, query, count=25):
    """Scrape live tweets for a query and stream each tweet to Kafka."""
    print(f"[*] Scraping live tweets for: '{query}'...")
    try:
        scraped = scrape_fn(query, count=count)
    except Exception as e:
        print(f"[!] Scrape error for '{query}': {e}")
        return 0

    print(f"[*] Fetched {len(scraped)} tweets for '{query}'. Streaming to Kafka topic '{KAFKA_TOPIC}'...")

    for i, item in enumerate(scraped):
        tweet_text = item.get("tweet", "").strip()
        if not tweet_text:
            continue

        # Kafka payload structure: [id, query_tag, placeholder, text]
        payload = [str(i + 1), query, "Unlabeled", tweet_text]
        producer.send(KAFKA_TOPIC, value=payload)
        user_name = item.get("user", "user")
        print(f"[LIVE >] Sent ({user_name}): {tweet_text[:70]}...")
        time.sleep(STREAM_DELAY)

    producer.flush()
    return len(scraped)

def main():
    parser = argparse.ArgumentParser(description="Live Twitter Scraper & Kafka Stream Producer")
    parser.add_argument("--query", type=str, default=None, help="Specific hashtag/keyword to scrape (default: auto-cycles trending tech topics)")
    parser.add_argument("--count", type=int, default=25, help="Number of tweets per batch")
    parser.add_argument("--continuous", action="store_true", help="Continuously scrape and stream indefinitely")
    args = parser.parse_args()

    scrape_fn = get_scraper()

    print(f"[*] Connecting Kafka Producer to {KAFKA_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in KAFKA_SERVERS.split(",")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 5, 0)
    )
    print(f"[*] Producer ready. Streaming live scraped tweets to topic '{KAFKA_TOPIC}' every {STREAM_DELAY}s...\n")

    topic_index = 0
    try:
        while True:
            if args.query:
                current_query = args.query
            else:
                current_query = DEFAULT_TOPICS[topic_index % len(DEFAULT_TOPICS)]
                topic_index += 1

            stream_scraped_batch(producer, scrape_fn, current_query, count=args.count)

            if not args.continuous and args.query:
                print("\n[*] Batch complete.")
                break

            print(f"[*] Waiting 5s before next scrape batch...\n")
            time.sleep(5.0)

    except KeyboardInterrupt:
        print("\n[!] Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
