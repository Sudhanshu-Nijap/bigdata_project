# Real-Time Twitter Sentiment Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark%20ML-orange.svg)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-NoSQL%20Database-green.svg)](https://www.mongodb.com/)
[![Flask](https://img.shields.io/badge/Flask-Web%20Dashboard-lightgrey.svg)](https://palletsprojects.com/p/flask/)
[![Docker](https://img.shields.io/badge/Docker-Docker%20Compose-2496ED.svg)](https://www.docker.com/)

A scalable, end-to-end Big Data pipeline designed for **real-time ingestion, distributed stream processing, machine learning-driven sentiment classification, and live visual monitoring** of Twitter / X data.

---

## 📌 System Architecture & Pipeline Diagrams

### 1. End-to-End Big Data Pipeline Architecture (Lambda Architecture)

```mermaid
flowchart TD
    subgraph INGESTION["1. Ingestion Layer (Producers)"]
        A["Twitter / X Live Stream<br/>(Hashtags, Keywords, Real-time Feeds)"] --> B["Kafka Producer<br/>(kafka_producer.py)"]
        Synthetic["High-Throughput Synthetic Stream<br/>(Benchmark Generator)"] --> B
    end

    subgraph BROKER["2. Distributed Message Broker"]
        B --> C[("Apache Kafka Cluster<br/>Topic: numtest | Replication & Partitioning")]
    end

    subgraph STREAMING["3. Distributed Stream Processing (Speed Layer)"]
        C --> D["PySpark Structured Streaming Consumer<br/>(spark_consumer.py)"]
        D --> E["PySpark MLlib Pipeline<br/>(spark_pipeline_artifact)"]
        D --> V["Sliding-Window Volatility Engine<br/>(Anomaly & Shift Detection)"]
    end

    subgraph STORAGE["4. Storage Tiering (Hot vs. Cold)"]
        E --> F[("MongoDB NoSQL<br/>(Hot Operational Store - Sub-second Reads)")]
        E --> G[("Apache Parquet Data Lakehouse<br/>(Cold Analytical Store - Snappy Compressed ~80% Saved)")]
    end

    subgraph ACTIVE_LEARNING["5. Active Learning & Batch Retraining (Batch Layer)"]
        F -- "Human Verified Feedback<br/>(is_verified: true)" --> H["PySpark Batch Retrainer<br/>(spark_retrainer.py)"]
        I["Baseline Labeled Datasets<br/>(training_dataset.csv)"] --> H
        H -- "Export Updated Model Artifact" --> E
    end

    subgraph PRESENTATION["6. Serving & Visualization Layer"]
        F --> J["Flask Web Application<br/>(app.py - Port 8000)"]
        V --> J
        J --> K["Real-Time KPI Counters & Volatility Radar"]
        J --> L["Chart.js Dynamic Sentiment Donut & Bar Charts"]
        J --> M["Interactive Model Testing Desk (/classify)"]
        J --> N["Apache Parquet / CSV Lakehouse Data Exporter"]
    end

    classDef ing fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0f172a;
    classDef brk fill:#f1f5f9,stroke:#475569,stroke-width:2px,color:#0f172a;
    classDef stm fill:#ffedd5,stroke:#ea580c,stroke-width:2px,color:#0f172a;
    classDef str fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#0f172a;
    classDef ml fill:#f3e8ff,stroke:#9333ea,stroke-width:2px,color:#0f172a;
    classDef ui fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#0f172a;

    class A,B,Synthetic ing;
    class C brk;
    class D,E,V stm;
    class F,G str;
    class H,I ml;
    class J,K,L,M,N ui;
```

---

### 2. Distributed Streaming & Partitioning Topology

```mermaid
flowchart LR
    subgraph KAFKA["Apache Kafka Broker"]
        P0["Partition 0"]
        P1["Partition 1"]
        P2["Partition 2"]
    end

    subgraph SPARK["PySpark Distributed Worker Nodes"]
        W1["Spark Executor 1<br/>(Task Partition 0)"]
        W2["Spark Executor 2<br/>(Task Partition 1)"]
        W3["Spark Executor 3<br/>(Task Partition 2)"]
    end

    subgraph SINKS["Dual Persistence Sinks"]
        M[("MongoDB Operational Sink<br/>(High-throughput Writes)")]
        P[("Snappy Parquet Sink<br/>(Columnar Lakehouse Storage)")]
    end

    P0 --> W1
    P1 --> W2
    P2 --> W3

    W1 --> M & P
    W2 --> M & P
    W3 --> M & P
```

---

### 3. PySpark MLlib NLP Feature Extraction Pipeline

```mermaid
flowchart LR
    A["Raw Tweet Text"] --> B["Regex Sanitizer<br/>(Remove URLs, @mentions, #hashtags, punctuation)"]
    B --> C["Tokenizer<br/>(Splits text into tokenized words)"]
    C --> D["StopWordsRemover<br/>(Filters English stopwords: 'the', 'is', 'at')"]
    D --> E["HashingTF<br/>(Term Frequency Feature Vector - 10,000 bins)"]
    E --> F["IDF<br/>(Inverse Document Frequency scaling)"]
    F --> G["Multiclass Logistic Regression<br/>(spark_pipeline_artifact)"]
    G --> H{"Predicted Sentiment"}
    H --> I["Positive [1]"]
    H --> J["Negative [0]"]
    H --> K["Neutral [2]"]
    H --> L["Irrelevant [3]"]

    classDef text fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0f172a;
    classDef nlp fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#0f172a;
    classDef model fill:#f3e8ff,stroke:#9333ea,stroke-width:2px,color:#0f172a;
    classDef pred fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#0f172a;

    class A,B text;
    class C,D,E,F nlp;
    class G,H model;
    class I,J,K,L pred;
```

---

### 4. Human-in-the-Loop Active Learning Feedback Loop

```mermaid
sequenceDiagram
    autonumber
    actor User as User / Data Analyst
    participant Dashboard as Flask Dashboard (UI)
    participant Mongo as MongoDB NoSQL Store
    participant Spark as PySpark MLlib Retrainer
    participant Model as Production ML Pipeline Artifact

    User->>Dashboard: Views live streaming tweet in teleprinter feed
    User->>Dashboard: Clicks "+Pos / -Neg / Neu / Irr" to correct or confirm label
    Dashboard->>Mongo: POST /api/verify-sentiment (sets is_verified: true)
    User->>Dashboard: Clicks "Retrain PySpark Model"
    Dashboard->>Spark: POST /api/retrain-model
    Spark->>Mongo: Fetch all verified ground-truth records
    Spark->>Spark: Merge verified feedback with baseline training_dataset.csv
    Spark->>Spark: Fit PipelineModel & Evaluate on validation_dataset.csv
    Spark->>Model: Overwrite spark_pipeline_artifact with newly trained weights
    Spark-->>Dashboard: Return retrained accuracy & F1-score telemetry
    Dashboard-->>User: Displays "Model Retrained Successfully" notification banner
```

---

### 5. Storage Tiering (Hot vs. Cold Lakehouse Architecture)

```mermaid
graph LR
    subgraph INGEST["Live Streaming Ingestion"]
        Kafka["Apache Kafka Broker"] --> Spark["PySpark Stream Processor"]
    end

    subgraph HOT["Hot Tier (Operational)"]
        Spark --> Mongo[("MongoDB NoSQL<br/>- Raw JSON Documents<br/>- Sub-second UI query latency<br/>- Retains active stream window")]
        Mongo --> UI["Flask Live Dashboard & SSE Stream"]
    end

    subgraph COLD["Cold Tier (Data Lakehouse)"]
        Spark --> Parquet[("Apache Parquet Storage<br/>- Snappy Columnar compression<br/>- ~82% Storage Space Saved<br/>- Column pruning & predicate pushdown")]
        Parquet --> Retrain["PySpark Batch Retraining & Historical OLAP Queries"]
    end
```

---

### 6. Sliding-Window Volatility & Anomaly Radar Pipeline

```mermaid
flowchart TD
    Stream["Continuous Tweet Stream"] --> Window["Sliding Time Window (e.g., 20 items / 60s)"]
    Window --> Calc["Compute Sentiment Probabilities & Rolling Variance"]
    Calc --> StdDev["Calculate Volatility Index & Moving Z-Score"]
    StdDev --> Check{"Z-Score > Threshold?"}
    Check -- Yes --> Anomaly["🚨 Sentiment Shock / Anomaly Detected<br/>Trigger Live UI Alert"]
    Check -- No --> Normal["🟢 Normal Sentiment Flow"]

    classDef s fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0f172a;
    classDef c fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#0f172a;
    classDef a fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#0f172a;
    classDef n fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#0f172a;

    class Stream,Window s;
    class Calc,StdDev,Check c;
    class Anomaly a;
    class Normal n;
```

---

### 7. Historical Architecture Reference

![End-to-End System Pipeline](project_diagrams/flow.png)
*Figure 1: High-level architectural pipeline flow demonstrating live streaming from Twitter through Kafka brokers and Apache Spark MLlib classification into MongoDB persistence and the serving web tier.*

---

## 🚀 Key Features

- **Real-Time Streaming Ingestion**: Kafka producers dynamically scrape live tweets across configurable keywords (`#AI`, `#Crypto`, `#Tesla`, `#Bitcoin`, etc.) or custom topics and stream them into Kafka topics.
- **Distributed Stream Inference**: PySpark consumers process incoming message streams, perform NLP text sanitization (removing URLs, mentions, and special characters), and execute inference using a pre-trained PySpark MLlib Pipeline.
- **Trained PySpark ML Model**: Multiclass Logistic Regression model with Tokenizer, StopWordsRemover, and HashingTF/IDF classifying tweets into 4 categories:
  - `Positive`
  - `Negative`
  - `Neutral`
  - `Irrelevant`
- **Data Persistence**: Stores real-time classified tweets in MongoDB (supports local MongoDB or cloud MongoDB Atlas).
- **Interactive Flask Dashboard**:
  - **Live Analytics (`/`)**: Real-time KPI summary counters, dynamic sentiment distribution charts (Pie and Bar plots via Chart.js), and searchable/filterable tweet tables.
  - **Tweet Scraper & Sentiment Studio (`/scrape`)**: Instant on-demand scraping by keyword or hashtag with immediate sentiment scoring and database persistence.
  - **Model Classifier Playground (`/classify`)**: Test custom text or sentences against the PySpark ML model in real time.
- **Full Dockerization**: Instant deployment of ZooKeeper, Kafka, MongoDB, Flask Web UI, Kafka Producer, and PySpark Consumer using Docker Compose.

---

## 📂 Repository Structure

```
Real-Time-Twitter-Sentiment-Analysis/
├── kafka_spark_streaming/        # Ingestion & Streaming Layer
│   ├── kafka_producer.py         # Live tweet scraper & Kafka stream publisher
│   ├── spark_consumer.py         # Spark streaming consumer & ML inference engine
│   └── spark_retrainer.py        # Active learning PySpark batch retrainer
├── pyspark_model_training/       # Machine Learning Layer
│   ├── spark_notebook.ipynb      # Jupyter notebook for model training & evaluation
│   ├── spark_pipeline_artifact/  # Exported PySpark PipelineModel
│   ├── training_dataset.csv      # Baseline training dataset (~74,682 tweets)
│   └── validation_dataset.csv    # Validation dataset (~998 tweets)
├── webapp/                       # Web & Active Learning Layer
│   ├── app.py                    # Flask server, REST APIs & SSE stream
│   ├── tweet_scraper.py          # Tweet scraping utilities (Nitter / Syndication)
│   ├── templates/                # Jinja2 templates (index, classify, base)
│   │   ├── base.html             # Global vintage masthead layout
│   │   ├── index.html            # Real-time ledger & active learning desk
│   │   └── classify.html         # Single text test interface
│   └── static/                   # Static assets
│       ├── css/style.css         # Vintage parchment & stamp stylesheet
│       └── js/dashboard.js       # Interactive Chart.js & teleprinter logic
├── project_diagrams/             # Architecture flowcharts and project images
├── Dockerfile                    # Container definition for web and worker services
├── docker-compose.yml            # Multi-container orchestration config
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
└── README.md                     # Project documentation
```

---

## 📊 Dataset Description

The machine learning model was trained and evaluated on the [Twitter Entity Sentiment Analysis Dataset](https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis):
- **Features**: `Tweet ID`, `Entity`, `Sentiment` (Target: Positive, Negative, Neutral, Irrelevant), and `Tweet Content`.
- **Training Set (`training_dataset.csv`)**: 74,682 labeled tweets.
- **Validation Set (`validation_dataset.csv`)**: 998 labeled tweets.

---

## 🛠️ Prerequisites

- **Python 3.9+**
- **Java OpenJDK 8 or 11** (Required for Apache Spark & PySpark)
- **Docker & Docker Compose** (Recommended for easiest setup)
- **MongoDB** (Local instance or MongoDB Atlas URI)

---

## ⚙️ Configuration (.env)

Create a `.env` file in the root directory (or copy from `.env.example`):

```env
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=bigdata_project
MONGO_COLLECTION_NAME=tweets

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=numtest
STREAM_INTERVAL_SECONDS=3.0

# Flask Configuration
PORT=8000
SECRET_KEY=sentiment-analysis-secret-key
```

---

## 🐳 Quickstart with Docker Compose (Recommended)

To launch the complete infrastructure (Zookeeper, Kafka, MongoDB, Producer, Spark Consumer, and Web UI) in one command:

```bash
# Build and start all services in detached mode
docker-compose up --build -d
```

Once running, access the web interface at **`http://localhost:8000`**.

To stop the services:
```bash
docker-compose down
```

---

## 💻 Manual Setup & Execution

If you prefer to run services locally outside of Docker:

### 1. Install Dependencies
```bash
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Start Kafka & MongoDB
Ensure you have Apache Kafka running on `localhost:9092` and MongoDB on `localhost:27017` (or provide a cloud MongoDB Atlas connection string in your `.env`).

### 3. Run the PySpark Consumer & Classifier
```bash
cd kafka_spark_streaming
python spark_consumer.py
```

### 4. Run the Kafka Producer (Live Tweet Streamer)
```bash
cd kafka_spark_streaming

# Continuous streaming across trending topics:
python kafka_producer.py --continuous

# Or stream tweets for a specific topic / hashtag:
python kafka_producer.py --query "#Bitcoin" --count 50
```

### 5. Launch the Flask Web Dashboard
```bash
# From the project root:
python webapp/app.py
```




Open your browser and navigate to:
- **Live Dashboard**: [http://127.0.0.1:8000](http://127.0.0.1:8000)
- **Live Scraper & Studio**: [http://127.0.0.1:8000/scrape](http://127.0.0.1:8000/scrape)
- **Interactive Classifier**: [http://127.0.0.1:8000/classify](http://127.0.0.1:8000/classify)

---

## 🌐 Web Application & API Endpoints

| Route | Method | Description |
|---|---|---|
| `/` | `GET` | Main real-time sentiment analytics dashboard with KPI cards and Chart.js graphs |
| `/classify` | `GET`, `POST` | Interactive model testing desk for single-sentence or custom text inference |
| `/api/stats` | `GET` | Returns aggregated sentiment count metrics & real-time proportions |
| `/api/tweets` | `GET` | Returns the latest classified tweets from MongoDB |
| `/api/stream-tweets` | `GET` | Server-Sent Events (SSE) live real-time tweet feed stream |
| `/api/scrape-analyze` | `POST` | On-demand keyword/hashtag scraping & sentiment scoring batch endpoint |
| `/api/classify` | `POST` | Programmatic JSON sentiment classification endpoint |
| `/api/verify-sentiment` | `POST` | Human-in-the-Loop label feedback endpoint (marks `is_verified: true`) |
| `/api/verified-stats` | `GET` | Returns telemetry on human-verified ground-truth sample count in MongoDB |
| `/api/retrain-model` | `POST` | Triggers PySpark MLlib batch retraining and updates pipeline model weights |
| `/api/lakehouse/stats` | `GET` | Returns Apache Parquet Lakehouse compression metrics & stream volatility index |
| `/api/lakehouse/export-parquet` | `GET` | Downloads current streaming records as Snappy-compressed Apache Parquet |
| `/api/lakehouse/export-csv` | `GET` | Downloads current streaming records as structured CSV dataset |
| `/api/clear-db` | `POST` | Clears operational MongoDB collection on demand |
| `/api/health` | `GET` | Cluster connectivity and service health check probe |

---

## 👥 Contributors & Acknowledgements

- **Driss Khattabi** ([@drisskhattabi6](https://github.com/drisskhattabi6))
- **Ayman Boufarhi** ([@aymanboufarhi](https://github.com/aymanboufarhi))
- **Abdelali Ibn Tabet** ([@abd-ibn](https://github.com/abd-ibn))

**Supervised By**: Prof. **Yasyn El Yusufi**  
*Faculty of Sciences and Technology of Tangier — Abdelmalek Essaadi University*  
*Master: Artificial Intelligence and Data Science (Module: Big Data)*
