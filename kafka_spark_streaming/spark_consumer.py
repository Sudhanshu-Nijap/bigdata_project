import os
import re
import sys
import json
import time
from pymongo import MongoClient
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Configuration
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "numtest")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB_NAME", "bigdata_project")
MONGO_COLL = os.getenv("MONGO_COLLECTION_NAME", "tweets")

# Model path resolution
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "pyspark_model_training", "spark_pipeline_artifact")
if not os.path.exists(MODEL_PATH):
    MODEL_PATH = os.path.join(os.path.dirname(__file__), "spark_pipeline_artifact")

# Sentiment mapping
SENTIMENT_MAP = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+", "", text)
    text = re.sub(r"(@|#)\w+", "", text)
    text = re.sub(r"[^a-zA-Z\s]", "", text.lower())
    return re.sub(r"\s+", " ", text).strip()

def main():
    print(f"[*] Connecting to MongoDB at {MONGO_URI}...")
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLL]

    print("[*] Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("TwitterSentimentAnalysis") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    print(f"[*] Loading ML PipelineModel from {MODEL_PATH}...")
    pipeline = PipelineModel.load(MODEL_PATH)

    print(f"[*] Connecting Kafka Consumer to {KAFKA_SERVERS} on topic '{KAFKA_TOPIC}'...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[s.strip() for s in KAFKA_SERVERS.split(",")],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version=(2, 5, 0)
    )

    print(f"[*] Listening for incoming scraped tweets on topic '{KAFKA_TOPIC}'...\n")

    try:
        for message in consumer:
            raw_data = message.value
            if isinstance(raw_data, list) and len(raw_data) > 0:
                tweet_text = str(raw_data[-1])
            else:
                tweet_text = str(raw_data)

            if not tweet_text.strip():
                continue

            cleaned = clean_text(tweet_text)
            if not cleaned:
                cleaned = "tweet"

            sentiment = "Neutral"
            try:
                # PySpark ML Pipeline Inference
                df = spark.createDataFrame([(cleaned,)], ["Text"])
                transformed = pipeline.transform(df).collect()
                if transformed and len(transformed[0]) > 6:
                    prediction_val = int(transformed[0][6])
                    sentiment = SENTIMENT_MAP.get(prediction_val, "Neutral")
            except Exception as ml_err:
                print(f"[!] PySpark inference warning: {ml_err}")
                sentiment = "Neutral"

            # Store in MongoDB
            try:
                collection.insert_one({
                    "tweet": tweet_text,
                    "cleaned_tweet": cleaned,
                    "prediction": sentiment,
                    "timestamp": time.time()
                })
                print(f"[>] PySpark Classified: '{tweet_text[:50]}...' -> {sentiment}")
            except Exception as db_err:
                print(f"[!] MongoDB insert error: {db_err}")

    except KeyboardInterrupt:
        print("\n[!] Consumer stopped by user.")
    finally:
        consumer.close()
        mongo_client.close()
        try:
            spark.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
