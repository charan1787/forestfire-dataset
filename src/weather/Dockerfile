FROM python:3.10-slim

WORKDIR /app

# install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# copy app code + models + templates
COPY src/weather /app
COPY models /app/models
COPY templates /app/templates

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "application:app", "--workers", "2", "--threads", "4"]
