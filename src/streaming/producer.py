from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",   # host access port
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version = (3,1,0)
)

TOPIC = "forest_fire_weather"

def generate_weather():
    return {
        "Temperature": round(random.uniform(15, 40), 2),
        "RH": round(random.uniform(20, 90), 2),
        "Ws": round(random.uniform(0, 15), 2),
        "Rain": round(random.uniform(0, 5), 2),
        "FFMC": round(random.uniform(50, 90), 2),
        "DMC": round(random.uniform(1, 100), 2),
      
        "ISI": round(random.uniform(0, 10), 2),
        
        "Classes": random.choice([0, 1]),
        "Region": random.choice([0, 1])
    }

if __name__ == "__main__":
    while True:
        event = generate_weather()
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(2)
