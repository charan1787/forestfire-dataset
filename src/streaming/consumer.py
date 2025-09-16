import os, json, csv, sys, pathlib
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("TOPIC", "weather_stream")
GROUP_ID  = os.getenv("GROUP_ID", "weather-group")
OUT_FILE  = os.getenv("OUT_FILE", "/app/data/stream/stream_data.csv")

COLUMNS = ["Temperature","RH","Ws","Rain","FFMC","DMC","ISI","Classes","Region"]

def ensure_header(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)

def main():
    out_path = pathlib.Path(OUT_FILE)
    ensure_header(out_path)

    print(f"[consumer] connecting to {BOOTSTRAP}, topic={TOPIC}, group={GROUP_ID}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    with out_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        for msg in consumer:
            data = msg.value
            # Keep only known columns; fill missing
            row = {k: data.get(k, "") for k in COLUMNS}
            writer.writerow(row)
            f.flush()
            print("[consumer] <-", row)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
