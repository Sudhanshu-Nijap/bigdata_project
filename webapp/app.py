import os
import sys
import re
import json
import time

# Ensure PySpark workers always use the current active Python executable (avoids Windows App Execution Alias error)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

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
    os.path.join(os.path.dirname(__file__), "..", "pyspark_model_training", "spark_pipeline_artifact")
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

class PySparkMLEngine:
    """
    Direct high-performance inference engine for the PySpark MLlib PipelineModel.
    Evaluates CountVectorizer + Logistic Regression weights (W * X + b) trained by PySpark.
    """
    def __init__(self, model_dir=MODEL_PATH):
        self.model_dir = model_dir
        self.vocab_index = {}
        self.intercepts = [0.0, 0.0, 0.0, 0.0]
        self.coeff_values = []
        self.num_classes = 4
        self.num_rows = 4
        self.num_cols = 0
        self.is_col_major = True
        self.sentiment_map = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}
        self.load_model()

    def load_model(self):
        try:
            import pyarrow.parquet as pq
            import glob

            # 1. Load CountVectorizer Vocabulary
            vocab_pattern = os.path.join(self.model_dir, "stages", "*CountVectorizer*", "data")
            vocab_dirs = glob.glob(vocab_pattern)
            if vocab_dirs:
                vocab_table = pq.read_table(vocab_dirs[0])
                vocab = vocab_table.to_pydict()["vocabulary"][0]
                self.vocab_index = {w.lower(): i for i, w in enumerate(vocab)}
                self.num_cols = len(vocab)

            # 2. Load Logistic Regression Weights & Intercepts
            lr_pattern = os.path.join(self.model_dir, "stages", "*LogisticRegression*", "data")
            lr_dirs = glob.glob(lr_pattern)
            if lr_dirs:
                lr_table = pq.read_table(lr_dirs[0])
                pydict = lr_table.to_pydict()
                self.num_classes = int(pydict["numClasses"][0])
                self.intercepts = list(pydict["interceptVector"][0]["values"])
                coeff_matrix = pydict["coefficientMatrix"][0]
                self.coeff_values = list(coeff_matrix["values"])
                self.num_rows = int(coeff_matrix["numRows"])
                self.is_transposed = coeff_matrix.get("isTransposed", True)
        except Exception as e:
            pass

    def predict(self, text: str) -> str:
        """Run ML Model inference using learned PySpark weights (W * X + b)."""
        if not text or not text.strip():
            return "Neutral"

        raw = text.strip()
        words = re.findall(r'[a-zA-Z]+', raw.lower())
        if not words:
            return "Neutral"

        if not self.coeff_values or not self.vocab_index:
            return "Neutral"

        scores = list(self.intercepts)
        has_features = False

        for w in words:
            if w in self.vocab_index:
                col = self.vocab_index[w]
                has_features = True
                for r in range(self.num_classes):
                    idx = (r * self.num_cols + col) if self.is_transposed else (col * self.num_rows + r)
                    if idx < len(self.coeff_values):
                        scores[r] += self.coeff_values[idx]

        if not has_features:
            return "Neutral"

        best_class = max(range(self.num_classes), key=lambda c: scores[c])
        return self.sentiment_map.get(best_class, "Neutral")

_ml_engine = PySparkMLEngine()

def classify_tweet_text(text: str) -> str:
    """Classify tweet using direct PySpark ML Model inference."""
    global _ml_engine
    if _ml_engine is None:
        _ml_engine = PySparkMLEngine()
    return _ml_engine.predict(text)

def _kafka_worker(payloads):
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[s.strip() for s in KAFKA_SERVERS.split(",")],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            request_timeout_ms=1000,
            max_block_ms=500
        )
        for p in payloads:
            producer.send(KAFKA_TOPIC, value=p)
        producer.flush(timeout=1)
        producer.close()
    except Exception:
        pass

def publish_to_kafka(payloads):
    """Publish tweet batch to Kafka broker asynchronously without blocking the stream."""
    import threading
    t = threading.Thread(target=_kafka_worker, args=(payloads,), daemon=True)
    t.start()

def fetch_tweets_and_stats(limit=500):
    """Retrieve tweets from MongoDB and compute sentiment metrics."""
    _, collection = get_mongo_collection()
    tweets = []
    if collection is not None:
        try:
            raw = list(collection.find().sort('_id', -1).limit(limit))
            tweets = [{
                'tweet': item.get('tweet', ''),
                'prediction': item.get('prediction', 'Neutral'),
                'user': item.get('user', 'twitter_user'),
                'date': item.get('date', 'Live'),
                'is_verified': item.get('is_verified', False),
                'verified_sentiment': item.get('verified_sentiment')
            } for item in raw]
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
    total, counts, rates, recent_tweets = fetch_tweets_and_stats()
    if total == 0:
        try:
            scraped = scrape_tweets("#AI", count=15) + scrape_tweets("#Tech", count=10)
            db_docs = []
            for item in scraped:
                txt = item.get('tweet', '').strip()
                if txt:
                    pred = classify_tweet_text(txt)
                    db_docs.append({
                        'tweet': txt,
                        'prediction': pred,
                        'user': item.get('user', 'twitter_user'),
                        'date': item.get('date', 'Recently'),
                        'is_verified': False,
                        'timestamp': time.time()
                    })
            if db_docs:
                _, collection = get_mongo_collection()
                if collection is not None:
                    collection.insert_many(db_docs)
                total, counts, rates, recent_tweets = fetch_tweets_and_stats()
        except Exception:
            pass

    return render_template(
        'index.html',
        len_data=total,
        sentiment_counts=counts,
        sentiment_rates=rates,
        data=recent_tweets
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
                pass
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
    """Server-Sent Events (SSE) real-time streaming endpoint: continuous and rapid."""
    query = request.args.get('query', '#AI').strip() or '#AI'

    def generate_live_stream():
        idx = 0
        while True:
            scraped = scrape_tweets(query, count=30)
            if not scraped:
                scraped = scrape_tweets("tech", count=20)
            if not scraped:
                time.sleep(1.0)
                continue

            for item in scraped:
                idx += 1
                tweet_text = item.get('tweet', '').strip()
                if not tweet_text:
                    continue

                prediction = classify_tweet_text(tweet_text)
                publish_to_kafka([[str(idx), query, "Unlabeled", tweet_text]])

                def _save_mongo(txt, pred, q):
                    _, col = get_mongo_collection()
                    if col is not None:
                        try:
                            col.insert_one({
                                'tweet': txt,
                                'prediction': pred,
                                'query': q,
                                'is_verified': False,
                                'timestamp': time.time()
                            })
                        except Exception:
                            pass

                import threading
                threading.Thread(target=_save_mongo, args=(tweet_text, prediction, query), daemon=True).start()

                event_data = {
                    'index': idx,
                    'query': query,
                    'tweet': tweet_text,
                    'user': item.get('user', 'twitter_user'),
                    'date': item.get('date', 'Live'),
                    'prediction': prediction,
                    'is_verified': False
                }

                yield f"data: {json.dumps(event_data)}\n\n"
                time.sleep(0.5)

    return Response(generate_live_stream(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive'
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
            'date': item.get('date', 'Recently'),
            'is_verified': False
        }
        classified_results.append(record)
        db_documents.append({'tweet': tweet_text, 'prediction': prediction, 'is_verified': False, 'timestamp': time.time()})
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

@app.route('/api/verify-sentiment', methods=['POST'])
def api_verify_sentiment():
    """Human-in-the-Loop endpoint: updates ground truth sentiment in MongoDB for active learning."""
    data = request.get_json() or {}
    tweet_text = data.get('tweet', '').strip()
    verified_sentiment = data.get('verified_sentiment', '').strip()
    original_prediction = data.get('original_prediction', '')

    if not tweet_text or not verified_sentiment:
        return jsonify({"error": "Missing tweet text or verified sentiment."}), 400

    _, collection = get_mongo_collection()
    if collection is not None:
        try:
            # Update existing or upsert document
            collection.update_many(
                {"tweet": tweet_text},
                {"$set": {
                    "prediction": verified_sentiment,
                    "verified_sentiment": verified_sentiment,
                    "original_prediction": original_prediction,
                    "is_verified": True,
                    "verified_at": time.time()
                }},
                upsert=True
            )
            verified_count = collection.count_documents({"is_verified": True})
            return jsonify({
                "status": "success",
                "message": f"Tweet verified as '{verified_sentiment}'.",
                "verified_sentiment": verified_sentiment,
                "total_verified_records": verified_count
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({
        "status": "success_local",
        "message": f"Tweet verified as '{verified_sentiment}'.",
        "verified_sentiment": verified_sentiment,
        "total_verified_records": 1
    })

@app.route('/api/verified-stats', methods=['GET'])
def api_verified_stats():
    """Return count of human-verified training samples stored in MongoDB."""
    _, collection = get_mongo_collection()
    count = 0
    if collection is not None:
        try:
            count = collection.count_documents({"is_verified": True})
        except Exception:
            pass
    return jsonify({"verified_samples_count": count})

@app.route('/api/retrain-model', methods=['POST'])
def api_retrain_model():
    """Trigger PySpark batch retraining on baseline CSV + verified MongoDB feedback."""
    try:
        from spark_retrainer import run_retraining
    except ImportError:
        try:
            from kafka_spark_streaming.spark_retrainer import run_retraining
        except ImportError:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka_spark_streaming"))
            from spark_retrainer import run_retraining

    try:
        result = run_retraining()
        # Reset and reload cached model pipeline and ML engine weights
        global _pipeline, _ml_engine
        _pipeline = None
        if _ml_engine:
            _ml_engine.load_model()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Retraining failed: {e}"}), 500

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

@app.route('/api/lakehouse/stats')
def api_lakehouse_stats():
    """Return Big Data Lakehouse metrics, Parquet compression savings, and stream volatility."""
    try:
        from kafka_spark_streaming.data_lakehouse import generate_lakehouse_parquet, compute_stream_volatility
    except ImportError:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka_spark_streaming"))
        from data_lakehouse import generate_lakehouse_parquet, compute_stream_volatility

    try:
        meta = generate_lakehouse_parquet()
        volatility = compute_stream_volatility()
        return jsonify({
            "status": "active",
            "format": "Apache Parquet (Snappy Columnar)",
            "record_count": meta["record_count"],
            "raw_json_size_kb": meta["raw_json_size_kb"],
            "parquet_size_kb": meta["parquet_size_kb"],
            "compression_savings_pct": meta["compression_savings_pct"],
            "stream_volatility": volatility
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/lakehouse/export-parquet')
def api_export_parquet():
    """Download live/verified stream archive as a compressed Snappy Parquet file."""
    from flask import send_file
    import io
    try:
        from kafka_spark_streaming.data_lakehouse import generate_lakehouse_parquet
    except ImportError:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka_spark_streaming"))
        from data_lakehouse import generate_lakehouse_parquet

    meta = generate_lakehouse_parquet()
    buffer = io.BytesIO(meta["parquet_bytes"])
    buffer.seek(0)
    return send_file(
        buffer,
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name=f"twitter_lakehouse_archive_{int(time.time())}.parquet"
    )

@app.route('/api/lakehouse/export-csv')
def api_export_csv():
    """Export verified ground-truth training records as CSV."""
    from flask import send_file
    import io
    import pandas as pd
    _, collection = get_mongo_collection()
    docs = []
    if collection is not None:
        try:
            docs = list(collection.find({"is_verified": True}, {"_id": 0}))
        except Exception:
            pass

    if not docs:
        docs = [{"tweet": "Sample verified tweet", "prediction": "Positive", "is_verified": True}]

    df = pd.DataFrame(docs)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return send_file(
        csv_buffer,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"ground_truth_verified_{int(time.time())}.csv"
    )
@app.route('/api/health')
def health_check():
    client, collection = get_mongo_collection()
    connected, count, verified_count = False, 0, 0
    if client:
        try:
            client.admin.command('ping')
            connected = True
            if collection is not None:
                count = collection.estimated_document_count()
                verified_count = collection.count_documents({"is_verified": True})
        except Exception:
            connected = False

    return jsonify({
        "status": "healthy" if connected else "degraded",
        "database": "connected" if connected else "disconnected",
        "total_tweets_indexed": count,
        "verified_feedback_samples": verified_count
    }), (200 if connected else 503)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8000)), debug=True)
