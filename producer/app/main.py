import json
import random
import time
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC = 'incoming-911-calls'
INCIDENT_TYPES = ['structure_fire', 'cardiac_arrest', 'motor_vehicle_collision', 'hazmat_leak']
SECTORS = ['Sector-1', 'Sector-2', 'Sector-3', 'Sector-4']

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Event delivered to {msg.topic()} [Partition {msg.partition()}]")

print("Starting 911 Call Generator...")
while True:
    call_id = f"CALL-{random.randint(10000, 99999)}"
    idempotency_key = f"IDEM-{random.randint(50000, 59999)}" # Reused for duplicates

    event = {
        "call_id": call_id,
        "caller_timestamp": fake.iso8601(),
        "idempotency_key": idempotency_key,
        "incident_type_raw": random.choice(INCIDENT_TYPES),
        "sector_id": random.choice(SECTORS),
        "dispatcher_notes": fake.sentence()
    }

    payload = json.dumps(event)
    producer.produce(TOPIC, key=idempotency_key, value=payload, callback=delivery_report)

    # Simulate 15% cellular retry duplication
    if random.random() < 0.15:
        print(f"--> Simulating cellular retry duplicate for {idempotency_key}")
        producer.produce(TOPIC, key=idempotency_key, value=payload, callback=delivery_report)

    producer.poll(0)
    time.sleep(2) # 1 event every 2 seconds