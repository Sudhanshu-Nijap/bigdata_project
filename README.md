# Big Data Project: Real-Time Twitter Sentiment Analysis Using Kafka, Spark (MLLib & Streaming), MongoDB and Flask.

## Overview

This repository contains a Big Data project focused on real-time sentiment analysis of Twitter data (classification of tweets). The project leverages various technologies to collect, process, analyze, and visualize sentiment data from tweets in real-time.

## Project Architecture

The project is built using the following components:

- **Apache Kafka**: Used for real-time data ingestion from Twitter DataSet.
- **Spark Streaming**: Processes the streaming data from Kafka to perform sentiment analysis.
- **MongoDB**: Stores the processed sentiment data.
- **Flask**: Lightweight web framework providing a real-time analytics dashboard and ML classifier interface.
- **chart.js**: For interactive data visualization (pie and bar charts).

- This is the project plan :
   ![project img](imgs/flow.png)

## Features

- **Real-time Data Ingestion**: Collects live tweets using Kafka from the Twitter DataSet.
- **Stream Processing**: Utilizes Spark Streaming to process and analyze the data in real-time.
- **Sentiment Analysis**: Classifies tweets into different sentiment categories (positive, negative, neutral, irrelevant) using natural language processing (NLP) and PySpark MLLib.
- **Data Storage**: Stores the sentiment analysis results in MongoDB for persistence.
- **Visualization**: Provides a sleek real-time dashboard built with Flask and Chart.js with live auto-refreshing stats and interactive model classifier.

## Data description:

In This Project I'm using a Dataset (twitter_training.csv and twitter_validation.csv) to create pyspark Model and for create live tweets using Kafka. Each line of the "twitter_training.csv" learning database represents a Tweet, it contains over 74682 lines;

The data types of Features are:
- Tweet ID: int
- Entity: string
- Sentiment: string (Target)
- Tweet content: string

The validation database “twitter_validation.csv” contains 998 lines (Tweets) with the same features of “twitter_training.csv”.

This is the Data Source:
https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis

### Use a reviewed TweetClaw or X export

The Kafka producer reads `Kafka-PySpark/twitter_validation.csv`, which expects
four fields: tweet ID, entity, sentiment, and text. To test newer reviewed
tweet exports without hand-editing the CSV, convert them first:

```bash
python tools/normalize_tweet_export.py examples/tweetclaw-reviewed-export.csv \
  --output Kafka-PySpark/twitter_validation.csv
```

The converter accepts common Twitter/X and TweetClaw CSV headers such as
`tweet_id`, `account`, `sentiment`, `label`, `text`, `tweet`, `full_text`, and
`content`. Sentiment values must map to `Negative`, `Positive`, `Neutral`, or
`Irrelevant`. TweetClaw exports from https://github.com/Xquik-dev/tweetclaw are
best used after a review step adds one of those sentiment labels.

## Repository Structure

- **webapp** : Flask dashboard and ML classifier application (`app.py`, templates, static assets).
- **Kafka-PySpark** : kafka producer and pyspark streaming (kafka consumer).
- **ML PySpark Model** : trained PySpark model with jupyter notebook and datasets.
- **docker-compose.yml** : Docker compose configuration for Kafka, Zookeeper, MongoDB, Producer, Spark Consumer, and Web UI.
- **bigdataproject rapport** : a brief report about the project (in french).

## Getting Started

### Prerequisites

To run this project, you will need the following installed on your system:

- Docker (for running Kafka and services)
- Python 3.x
- Apache Kafka
- Apache Spark (PySpark for python)
- MongoDB
- Flask

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/drisskhattabi6/Real-Time-Twitter-Sentiment-Analysis.git
   cd Real-Time-Twitter-Sentiment-Analysis
   ```
   
2. **Installing Docker Desktop**

3. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Project

Note: you will need MongoDB for Running the Kafka and Spark Streaming application and for Running the Web Dashboard application.

#### Running the Kafka and Spark Streaming application:

1. **Change the directory to the application**:
   ```bash
   cd Kafka-PySpark
   ```

2. **Start Kafka in docker**:
   ```bash
   docker exec -it <kafka-container-id> /bin/bash
   ```

#### Running the Kafka Producer (CSV or Live Scraping):

- **Option A: Stream from Dataset CSV**
  ```bash
  python kafka_producer.py --mode csv
  ```

- **Option B: Scrape Live Tweets & Stream directly to Kafka**
  ```bash
  python kafka_producer.py --mode scrape --query "#Bitcoin" --count 50
  ```

4. **Run pyspark streaming (kafka consumer) app**:
   ```bash
   python pyspark_consumer.py
   ```

#### Running the Flask Web Dashboard & Scraper Studio:

1. **Run the Flask server**:
   ```bash
   python webapp/app.py
   ```
   *(or in production: `gunicorn --bind 0.0.0.0:8000 webapp.app:app`)*

2. **Access the Dashboard & Scraper**:
   - Live Dashboard: `http://127.0.0.1:8000`
   - Live Tweet Scraper & Sentiment Studio: `http://127.0.0.1:8000/scrape`
   - Model Classifier: `http://127.0.0.1:8000/classify`

## Features & Endpoints:

- **Live Scrape & Analyze Studio (`/scrape`)**: Search any keyword/hashtag (`#AI`, `Tesla`, `#Crypto`), scrape live tweets, infer sentiment via PySpark ML, and sync results into MongoDB.
- **Real-Time Live Dashboard (`/`)**: Continuous monitoring of MongoDB stream with 4-quadrant charts, KPI counters, and live search filters.
- **Interactive Classifier (`/classify`)**: Test custom text against the trained PySpark Logistic Regression model.
- in the Dashboard, There is a table contains tweets with labels.
- in the Dashboard, There is 3 statistics or plots : labels rates - pie plot - bar plot.


## Team :

- [Khattabi Idriss](https://github.com/drisskhattabi6) 
- [Boufarhi Ayman](https://github.com/aymanboufarhi) 
- [Abdelali IBN TABET](https://github.com/abd-ibn)

## Supervised By : 

- Prof. **Yasyn El Yusufi**

---

Abdelmalek Essaadi University - Faculty of Sciences and Technology of Tangier

- Master: Artificial Intelligence and Data Science
- Module: Big Data

---

- By following the above instructions, you should be able to set up and run the real-time Twitter sentiment analysis project on your local machine. Happy coding!

- Feel free to explore the project and customize it according to your requirements. If you encounter any issues or have any questions, don't hesitate to reach out!
