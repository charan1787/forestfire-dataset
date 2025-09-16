# Use lightweight Python base image
FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy source code, models, templates, and data
COPY src /app/src
COPY models /app/models
COPY templates /app/templates
COPY data /app/data

# Expose port for Flask
EXPOSE 5000

# Use SERVICE env var to decide which service to run
CMD if [ "$SERVICE" = "flask" ]; then \
        gunicorn -b 0.0.0.0:5000 src.weather.application:app; \
    elif [ "$SERVICE" = "producer" ]; then \
        python src/streaming/producer.py; \
    elif [ "$SERVICE" = "consumer" ]; then \
        python src/streaming/consumer.py; \
    else \
        echo "Unknown service: $SERVICE" && exit 1; \
    fi
