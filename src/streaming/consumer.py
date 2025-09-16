from kafka import KafkaConsumer
import json
import pandas as pd
import os

# Connect from host -> must use localhost:9093
consumer = KafkaConsumer(
    "forest_fire_weather",
    bootstrap_servers="localhost:9093",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",   # start from earliest if no committed offset
    enable_auto_commit=True,
    group_id="fire-consumer-group"
)

# Ensure the folder exists
csv_path = "data/stream/stream_data.csv"
os.makedirs(os.path.dirname(csv_path), exist_ok=True)

print("🚀 Consumer started, listening to 'forest_fire_weather'...")

for message in consumer:
    event = message.value
    print("✅ Received:", event)

    # Append to CSV
    df = pd.DataFrame([event])
    df.to_csv(
        csv_path,
        mode="a",
        header=not os.path.exists(csv_path),  # write header only if file doesn’t exist
        index=False
    )
