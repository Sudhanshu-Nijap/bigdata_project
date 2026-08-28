import os
import sys
import re
import json
import time
from flask import Flask, render_template, request, jsonify, Response
from pymongo import MongoClient
from dotenv import load_dotenv

# Ensure module path resolution works across all environments
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from tweet_scraper import scrape_tweets
except ImportError:
    from webapp.tweet_scraper import scrape_tweets

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'sentiment-secret')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Configuration
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'bigdata_project')
MONGO_COLLECTION_NAME = os.environ.get('MONGO_COLLECTION_NAME', 'tweets')
KAFKA_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'numtest')

MODEL_PATH = os.environ.get(
    'MODEL_PATH',
    os.path.join(os.path.dirname(__file__), "..", "ML PySpark Model", "logistic_regression_model.pkl")
)
SENTIMENT_MAP = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}

_spark = None
_pipeline = None
_mongo_client = None

def get_mongo_collection():
    """Return MongoDB collection singleton."""
    global _mongo_client
    try:
        if _mongo_client is None:
            _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        return _mongo_client, _mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
    except Exception:
        return None, None

def get_spark_and_model():
    """Lazily load PySpark session and ML pipeline model."""
    global _spark, _pipeline
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = SparkSession.builder.appName("TwitterSentimentWeb").master("local[*]").getOrCreate()

    if _pipeline is None:
        from pyspark.ml import PipelineModel
        _pipeline = PipelineModel.load(MODEL_PATH)

    return _spark, _pipeline

POSITIVE_LEXICON = {
    'good', 'great', 'awesome', 'excellent', 'amazing', 'love', 'loved', 'loving',
    'bullish', 'soaring', 'soar', 'win', 'winning', 'winner', 'profit', 'profits', 'gains',
    'gain', 'gaining', 'moon', 'innovative', 'innovation', 'best', 'fantastic', 'super',
    'happy', 'success', 'successful', 'breakthrough', 'clean', 'legend', 'positive',
    'excited', 'exciting', 'growth', 'grow', 'growing', 'surging', 'surge', 'gem',
    'strong', 'solid', 'top', 'buy', 'pump', 'beautiful', 'perfect', 'nice', 'delight',
    'wonderful', 'reward', 'benefit', 'up', 'all-time-high', 'ath', 'optimistic',
    'impressive', 'promising', 'boost', 'boosting', 'rally', 'rallying', 'leading', 'leader',
    'favorite', 'speed', 'fast', 'smooth', 'recommend', 'recommended', 'brilliant',
    'opportunity', 'upgrade', 'upgraded', 'impressed', 'impress', 'outperform', 'beat',
    'high', 'higher', 'highest', 'record', 'prosper', 'shine', 'valuable', 'advance', 'advanced'
}

NEGATIVE_LEXICON = {
    'bad', 'terrible', 'horrible', 'worst', 'awful', 'hate', 'hated', 'dump',
    'bearish', 'loss', 'losses', 'losing', 'loser', 'scam', 'fraud', 'crash',
    'crashing', 'crashed', 'drop', 'dropping', 'dropped', 'down', 'fail', 'failed',
    'failure', 'broken', 'error', 'errors', 'bug', 'bugs', 'angry', 'poor', 'sad',
    'disappointed', 'disappointing', 'sell', 'selling', 'sucks', 'suck', 'trash',
    'waste', 'ugly', 'rug', 'rugpull', 'scammer', 'hacked', 'hack', 'exploit',
    'tanking', 'plummet', 'garbage', 'fud', 'liquidation', 'lawsuit', 'sue', 'sued',
    'sec', 'fine', 'penalty', 'warning', 'warn', 'concern', 'risk', 'risky', 'danger',
    'dead', 'regret', 'regretted', 'regretting', 'complaint', 'complain', 'slow',
    'freeze', 'trouble', 'flaw', 'flaws', 'recall', 'protest', 'protesting', 'struggle',
    'shed', 'sheds', 'cut', 'cutting', 'decline', 'declining', 'crisis', 'threat', 'delay'
}

NEGATION_WORDS = {'not', 'no', 'never', 'none', 'neither', 'hardly', 'barely', 'scarcely', 'isnt', 'arent', 'wasnt', 'werent', 'dont', 'doesnt', 'didnt', 'wont'}

def classify_tweet_text(text: str) -> str:
    """Classify tweet using PySpark ML Pipeline or resilient NLP engine."""
    if not text or not text.strip():
        return "Neutral"

    raw = text.strip()
    cleaned = re.sub(r"https?://\S+|www\.\S+|(@|#)\w+|[^a-zA-Z\s]", "", raw.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # 1. Attempt PySpark Pipeline Model if active
    try:
        spark, pipeline = get_spark_and_model()
        if spark and pipeline:
            df = spark.createDataFrame([(cleaned,)], ["Text"])
            predictions = pipeline.transform(df).collect()
            if predictions and len(predictions[0]) > 6:
                return SENTIMENT_MAP.get(int(predictions[0][6]), "Neutral")
    except Exception:
        pass

    # 2. NLP Classification (Lexicon + Negations)
    words = re.findall(r'[a-zA-Z]+', raw.lower())
    if len(words) < 3 and ('http' in raw.lower() or '@' in raw):
        return 'Irrelevant'

    pos_score, neg_score = 0.0, 0.0
    for i, w in enumerate(words):
        is_negated = (i > 0 and words[i-1] in NEGATION_WORDS) or (i > 1 and words[i-2] in NEGATION_WORDS)
        if w in POSITIVE_LEXICON:
            if is_negated:
                neg_score += 1.5
            else:
                pos_score += 1.0
        elif w in NEGATIVE_LEXICON:
            if is_negated:
                pos_score += 1.0
            else:
                neg_score += 1.5

    if pos_score > neg_score and pos_score >= 1.0:
        return 'Positive'
    elif neg_score > pos_score and neg_score >= 1.0:
        return 'Negative'
    elif pos_score == 0 and neg_score == 0:
        if 'http' in raw.lower() or 't.co' in raw.lower() or len(words) < 4:
            return 'Irrelevant'
        return 'Neutral'
    return 'Neutral'

def publish_to_kafka(payloads):
    """Publish tweet batch to Kafka broker."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[s.strip() for s in KAFKA_SERVERS.split(",")],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            request_timeout_ms=1500
        )
        for p in payloads:
            producer.send(KAFKA_TOPIC, value=p)
        producer.flush(timeout=1)
        producer.close()
    except Exception:
        pass

def fetch_tweets_and_stats(limit=500):
    """Retrieve tweets from MongoDB and compute sentiment metrics."""
    _, collection = get_mongo_collection()
    tweets = []
    if collection is not None:
        try:
            raw = list(collection.find().sort('_id', -1).limit(limit))
            tweets = [{'tweet': item.get('tweet', ''), 'prediction': item.get('prediction', 'Neutral')} for item in raw]
        except Exception:
            pass

    total = len(tweets)
    counts = {'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Irrelevant': 0}
    for item in tweets:
        pred = item.get('prediction')
        if pred in counts:
            counts[pred] += 1

    rates = {k: round((v / total) * 100, 2) if total > 0 else 0.0 for k, v in counts.items()}
    return total, counts, rates, tweets[:50]

@app.route('/')
def dashboard():
    return render_template(
        'index.html',
        len_data=0,
        sentiment_counts={'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Irrelevant': 0},
        sentiment_rates={'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Irrelevant': 0},
        data=[]
    )

@app.route('/api/stats')
def api_stats():
    total, counts, rates, recent_tweets = fetch_tweets_and_stats()
    return jsonify({
        'len_data': total,
        'sentiment_counts': counts,
        'sentiment_rates': rates,
        'tweets': recent_tweets
    })

@app.route('/classify', methods=['GET', 'POST'])
def classify():
    error, error_text, prediction, text = False, "", "", ""
    if request.method == 'POST':
        text = request.form.get('text', '').strip()
        if text:
            try:
                prediction = classify_tweet_text(text)
            except Exception as e:
                error, error_text = True, f"Model error: {e}"
        else:
            error, error_text = True, "Please enter tweet text."

    return render_template(
        'classify.html',
        error=error,
        error_text=error_text,
        prediction=prediction,
        text=text,
        text_len=len(text)
    )

@app.route('/scrape')
def scrape_page():
    from flask import redirect, url_for
    return redirect(url_for('dashboard'))

@app.route('/api/stream-tweets')
def api_stream_tweets():
    """Server-Sent Events (SSE) real-time streaming endpoint."""
    query = request.args.get('query', '#AI').strip() or '#AI'

    def generate_live_stream():
        scraped = scrape_tweets(query, count=30)
        if not scraped:
            scraped = scrape_tweets("tech", count=15)

        for idx, item in enumerate(scraped):
            tweet_text = item.get('tweet', '').strip()
            if not tweet_text:
                continue

            prediction = classify_tweet_text(tweet_text)
            publish_to_kafka([[str(idx + 1), query, "Unlabeled", tweet_text]])

            _, collection = get_mongo_collection()
            if collection is not None:
                try:
                    collection.insert_one({
                        'tweet': tweet_text,
                        'prediction': prediction,
                        'query': query,
                        'timestamp': time.time()
                    })
                except Exception:
                    pass

            event_data = {
                'index': idx + 1,
                'query': query,
                'tweet': tweet_text,
                'user': item.get('user', 'twitter_user'),
                'date': item.get('date', 'Live'),
                'prediction': prediction
            }

            yield f"data: {json.dumps(event_data)}\n\n"
            time.sleep(2.0)

    return Response(generate_live_stream(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })

@app.route('/api/scrape-analyze', methods=['POST'])
def api_scrape_analyze():
    """Scrape, classify, and persist batch of tweets."""
    data = request.get_json() or {}
    query = data.get('query', '').strip()
    count = min(int(data.get('count', 20)), 50)
    save_to_db = data.get('save_to_db', True)

    if not query:
        return jsonify({"error": "Please provide a search keyword or hashtag."}), 400

    scraped = scrape_tweets(query, count=count)
    classified_results = []
    counts = {'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Irrelevant': 0}
    db_documents = []
    kafka_payloads = []

    for idx, item in enumerate(scraped):
        tweet_text = item.get('tweet', '')
        prediction = classify_tweet_text(tweet_text)
        if prediction in counts:
            counts[prediction] += 1

        record = {
            'tweet': tweet_text,
            'prediction': prediction,
            'user': item.get('user', 'twitter_user'),
            'date': item.get('date', 'Recently')
        }
        classified_results.append(record)
        db_documents.append({'tweet': tweet_text, 'prediction': prediction})
        kafka_payloads.append([str(idx + 1), query, "Unlabeled", tweet_text])

    if save_to_db and db_documents:
        publish_to_kafka(kafka_payloads)
        _, collection = get_mongo_collection()
        if collection is not None:
            try:
                collection.insert_many(db_documents)
            except Exception:
                pass

    total = len(classified_results)
    rates = {k: round((v / total) * 100, 2) if total > 0 else 0.0 for k, v in counts.items()}

    return jsonify({
        'query': query,
        'total': total,
        'sentiment_counts': counts,
        'sentiment_rates': rates,
        'results': classified_results
    })

@app.route('/api/clear-db', methods=['POST'])
def api_clear_db():
    """Clear all tweets from MongoDB collection on demand."""
    _, collection = get_mongo_collection()
    if collection is not None:
        try:
            res = collection.delete_many({})
            return jsonify({"status": "cleared", "deleted_count": res.deleted_count})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"status": "no_db"}), 200

@app.route('/api/health')
def health_check():
    client, collection = get_mongo_collection()
    connected, count = False, 0
    if client:
        try:
            client.admin.command('ping')
            connected = True
            if collection is not None:
                count = collection.estimated_document_count()
        except Exception:
            connected = False

    return jsonify({
        "status": "healthy" if connected else "degraded",
        "database": "connected" if connected else "disconnected",
        "total_tweets_indexed": count
    }), (200 if connected else 503)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8000)), debug=True, use_reloader=False)
