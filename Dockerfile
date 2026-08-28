FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/default-java

# Install Java & system utilities for Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download NLTK resources
RUN python -c "import nltk; nltk.download('stopwords', quiet=True); nltk.download('punkt', quiet=True)"

# Copy application source code and models
COPY . /app/

EXPOSE 8000

# Default entrypoint runs Flask web server (can be overridden by docker-compose)
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120", "webapp.app:app"]
