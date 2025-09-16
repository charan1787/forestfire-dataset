import os, json, time, random, sys
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("TOPIC", "weather_stream")
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.0"))
DATA_FILE = os.getenv("DATA_FILE", "/app/data/master_data.csv")

def make_event(row=None):
    if row is not None:
        d = row.to_dict()
    else:
        d = {
            "Temperature": round(random.uniform(5, 45), 2),
            "RH": round(random.uniform(5, 95), 2),
            "Ws": round(random.uniform(0, 15), 2),
            "Rain": round(random.uniform(0, 5), 2),
            "FFMC": round(random.uniform(50, 100), 2),
            "DMC": round(random.uniform(0, 300), 2),
            "ISI": round(random.uniform(0, 30), 2),
            "Classes": random.randint(0, 1),
            "Region": random.randint(0, 1),
        }
    d["ts"] = datetime.utcnow().isoformat()
    return d

def main():
    print(f"[producer] connecting to {BOOTSTRAP}, topic={TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=3,
    )

    df = None
    if os.path.exists(DATA_FILE):
        try:
            df = pd.read_csv(DATA_FILE)
            # Keep only the columns we expect if present
            keep = ["Temperature","RH","Ws","Rain","FFMC","DMC","ISI","Classes","Region"]
            df = df[[c for c in keep if c in df.columns]]
            print(f"[producer] streaming from {DATA_FILE} with {len(df)} rows")
        except Exception as e:
            print(f"[producer] failed reading {DATA_FILE}: {e}")

    i = 0
    while True:
        if df is not None and i < len(df):
            event = make_event(df.iloc[i])
            i += 1
        else:
            event = make_event()

        producer.send(TOPIC, event)
        print("[producer] ->", event)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
