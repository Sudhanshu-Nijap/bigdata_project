"""
PySpark Batch Retraining & Active Learning Pipeline
Orchestrates model retraining by combining baseline CSV datasets with
Human-in-the-Loop verified feedback tweets from MongoDB.
"""

import os
import re
import sys
import time

# Ensure PySpark workers always use the current active Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pymongo import MongoClient

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "bigdata_project")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "tweets")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "..", "pyspark_model_training")
TRAIN_CSV = os.path.join(MODEL_DIR, "training_dataset.csv")
VAL_CSV = os.path.join(MODEL_DIR, "validation_dataset.csv")
OUTPUT_MODEL_PATH = os.path.join(MODEL_DIR, "spark_pipeline_artifact")

SENTIMENT_TO_INDEX = {"Negative": 0.0, "Positive": 1.0, "Neutral": 2.0, "Irrelevant": 3.0}
INDEX_TO_SENTIMENT = {0.0: "Negative", 1.0: "Positive", 2.0: "Neutral", 3.0: "Irrelevant"}

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+", "", str(text))
    text = re.sub(r"(@|#)\w+", "", text)
    text = re.sub(r"[^a-zA-Z\s]", "", text.lower())
    return re.sub(r"\s+", " ", text).strip()

def fetch_verified_mongodb_tweets():
    """Fetch all human-verified training samples from MongoDB."""
    verified_data = []
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        collection = client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
        cursor = collection.find({"is_verified": True})
        for doc in cursor:
            raw_text = doc.get("tweet", "")
            sentiment = doc.get("verified_sentiment") or doc.get("prediction", "Neutral")
            cleaned = clean_text(raw_text)
            if cleaned and sentiment in SENTIMENT_TO_INDEX:
                verified_data.append((cleaned, SENTIMENT_TO_INDEX[sentiment]))
        client.close()
    except Exception as e:
        print(f"[!] Warning: Could not fetch MongoDB verified records: {e}")
    return verified_data

def run_retraining():
    """Execute complete PySpark retraining and evaluation."""
    print("==================================================================")
    print("[*] STARTING ACTIVE LEARNING PYSPARK RETRAINING PIPELINE")
    print("==================================================================")

    # 1. Fetch Human-in-the-loop MongoDB verified samples
    verified_samples = fetch_verified_mongodb_tweets()
    print(f"[*] Fetched {len(verified_samples)} human-verified samples from MongoDB.")

    # 2. Check baseline dataset
    if not os.path.exists(TRAIN_CSV):
        print(f"[!] Error: Training CSV not found at {TRAIN_CSV}")
        return {"status": "error", "message": "Baseline training dataset not found."}

    # 3. Initialize PySpark
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    print("[*] Initializing PySpark Session...")
    spark = SparkSession.builder \
        .appName("TwitterSentimentRetraining") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    try:
        # Load Baseline Training Data
        print(f"[*] Loading baseline training data from {TRAIN_CSV}...")
        raw_train_df = spark.read.csv(TRAIN_CSV, inferSchema=True, header=False)
        # Columns: _c0=ID, _c1=Entity, _c2=Sentiment, _c3=Text
        train_df = raw_train_df.select(
            raw_train_df["_c3"].alias("raw_text"),
            raw_train_df["_c2"].alias("sentiment_str")
        ).dropna()

        # Map sentiment strings to doubles
        from pyspark.sql.functions import udf
        clean_udf = udf(clean_text, StringType())
        label_udf = udf(lambda s: SENTIMENT_TO_INDEX.get(s, 2.0), DoubleType())

        train_cleaned_df = train_df \
            .withColumn("Text", clean_udf(train_df["raw_text"])) \
            .withColumn("label", label_udf(train_df["sentiment_str"])) \
            .select("Text", "label") \
            .filter("length(Text) > 2")

        # Merge with Verified MongoDB Data
        if verified_samples:
            schema = StructType([
                StructField("Text", StringType(), False),
                StructField("label", DoubleType(), False)
            ])
            verified_df = spark.createDataFrame(verified_samples, schema)
            # Combine baseline + verified active learning stream
            combined_train_df = train_cleaned_df.union(verified_df)
            print(f"[*] Merged dataset: {train_cleaned_df.count()} baseline + {len(verified_samples)} active learning samples.")
        else:
            combined_train_df = train_cleaned_df
            print(f"[*] Training on baseline dataset ({train_cleaned_df.count()} records).")

        # Load Validation Data
        print(f"[*] Loading validation dataset from {VAL_CSV}...")
        raw_val_df = spark.read.csv(VAL_CSV, inferSchema=True, header=False)
        val_df = raw_val_df.select(
            raw_val_df["_c3"].alias("raw_text"),
            raw_val_df["_c2"].alias("sentiment_str")
        ).dropna()

        val_cleaned_df = val_df \
            .withColumn("Text", clean_udf(val_df["raw_text"])) \
            .withColumn("label", label_udf(val_df["sentiment_str"])) \
            .select("Text", "label") \
            .filter("length(Text) > 2")

        # Build PySpark MLlib Pipeline
        print("[*] Assembling MLlib Pipeline: Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression...")
        tokenizer = Tokenizer(inputCol="Text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashing_tf = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])

        # Train Model
        start_time = time.time()
        print("[*] Fitting Logistic Regression model on combined distributed data...")
        model = pipeline.fit(combined_train_df)
        train_duration = round(time.time() - start_time, 2)
        print(f"[*] Model fitting completed in {train_duration} seconds.")

        # Evaluate Model
        print("[*] Evaluating model performance on validation set...")
        predictions = model.transform(val_cleaned_df)
        evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

        accuracy = round(float(evaluator_acc.evaluate(predictions)) * 100, 2)
        f1_score = round(float(evaluator_f1.evaluate(predictions)), 4)
        print(f"[>] Validation Accuracy: {accuracy}% | F1-Score: {f1_score}")

        # Save Updated Model
        print(f"[*] Exporting updated model to {OUTPUT_MODEL_PATH}...")
        model.write().overwrite().save(OUTPUT_MODEL_PATH)
        print("[✓] MODEL SUCCESSFULLY UPDATED AND SAVED TO PRODUCTION.")

        return {
            "status": "success",
            "accuracy": accuracy,
            "f1_score": f1_score,
            "verified_samples_used": len(verified_samples),
            "training_duration_seconds": train_duration,
            "timestamp": time.time()
        }

    except Exception as err:
        print(f"[!] Retraining Pipeline Error: {err}")
        return {"status": "error", "message": str(err)}
    finally:
        spark.stop()

if __name__ == "__main__":
    result = run_retraining()
    print(f"\nResult: {result}")
