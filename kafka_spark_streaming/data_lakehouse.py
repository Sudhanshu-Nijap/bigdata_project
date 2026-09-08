"""
Big Data Lakehouse & Columnar Storage Engine
Manages storage tiering (Hot MongoDB -> Cold Columnar Snappy Parquet),
compression metrics, and sliding-window stream anomaly detection.
"""

import os
import io
import time
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "bigdata_project")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "tweets")

LAKEHOUSE_DIR = os.path.join(os.path.dirname(__file__), "..", "pyspark_model_training", "data_lakehouse")
os.makedirs(LAKEHOUSE_DIR, exist_ok=True)

def get_mongo_data():
    """Retrieve all streaming and verified documents from MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        collection = client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
        docs = list(collection.find({}, {"_id": 0}))
        client.close()
        return docs
    except Exception as e:
        print(f"[!] Warning: Could not connect to MongoDB: {e}")
        return []

def generate_lakehouse_parquet():
    """Convert uncompressed MongoDB documents into a Snappy-compressed Columnar Parquet archive."""
    docs = get_mongo_data()
    if not docs:
        # Fallback dummy record for initial state
        docs = [{
            "tweet": "Initial streaming pipeline heartbeat dispatch.",
            "prediction": "Neutral",
            "query": "#Tech",
            "is_verified": False,
            "timestamp": time.time()
        }]

    df = pd.DataFrame(docs)
    if "timestamp" not in df.columns:
        df["timestamp"] = time.time()
    if "tweet" not in df.columns:
        df["tweet"] = ""
    if "prediction" not in df.columns:
        df["prediction"] = "Neutral"
    if "query" not in df.columns:
        df["query"] = "General"
    if "is_verified" not in df.columns:
        df["is_verified"] = False

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    df["tweet"] = df["tweet"].astype(str)
    df["prediction"] = df["prediction"].astype(str)
    df["query"] = df["query"].astype(str)
    df["is_verified"] = df["is_verified"].astype(bool)

    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_buffer, compression="snappy")
    parquet_bytes = parquet_buffer.getvalue()

    # Calculate raw JSON footprint vs Columnar Parquet footprint
    raw_json_str = json.dumps(docs)
    raw_json_bytes = len(raw_json_str.encode("utf-8"))
    parquet_size_bytes = len(parquet_bytes)

    compression_ratio = 0.0
    if raw_json_bytes > 0:
        compression_ratio = round((1.0 - (parquet_size_bytes / max(raw_json_bytes, 1))) * 100, 1)

    return {
        "record_count": len(df),
        "raw_json_size_kb": round(raw_json_bytes / 1024, 2),
        "parquet_size_kb": round(parquet_size_bytes / 1024, 2),
        "compression_savings_pct": max(compression_ratio, 0.0),
        "parquet_bytes": parquet_bytes,
        "dataframe": df
    }

def compute_stream_volatility(window_seconds=60):
    """Compute real-time sliding-window sentiment volatility and alert threshold."""
    docs = get_mongo_data()
    if not docs:
        return {"status": "STABLE", "volatility_score": 12.5, "negative_ratio": 0.0, "alert": False}

    current_time = time.time()
    recent = [d for d in docs if (current_time - d.get("timestamp", current_time)) <= window_seconds]
    sample_pool = recent if len(recent) >= 5 else docs[-30:]

    total = len(sample_pool)
    neg_count = sum(1 for d in sample_pool if d.get("prediction") == "Negative")
    pos_count = sum(1 for d in sample_pool if d.get("prediction") == "Positive")

    neg_ratio = round((neg_count / max(total, 1)) * 100, 1)
    pos_ratio = round((pos_count / max(total, 1)) * 100, 1)

    # Volatility formula based on sentiment polarization
    volatility = round(abs(neg_ratio - pos_ratio) * 0.85 + (neg_ratio * 0.4), 1)
    is_anomaly = neg_ratio >= 60.0 and total >= 5

    return {
        "status": "🚨 HIGH VOLATILITY CRITICAL" if is_anomaly else "🟢 STABLE FLOW",
        "volatility_score": min(volatility, 100.0),
        "negative_ratio": neg_ratio,
        "positive_ratio": pos_ratio,
        "alert": is_anomaly,
        "window_records": total
    }
