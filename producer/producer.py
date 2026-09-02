import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer


# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "checkout-events"


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_checkout_event(event_number):
    """Generate one checkout event following Schema v1."""

    event = {
        "event_id": f"evt_{event_number:03d}",
        "event_type": "checkout",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "order_id": f"ORD_{event_number:03d}",
        "customer_id": f"CUS_{random.randint(1, 100):03d}",
        "product_id": f"PROD_{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": "INR"
    }

    return event


def main():
    event_number = 1

    print("Starting Ice-Stream Kafka Producer...")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print("Sending one checkout event every second...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_checkout_event(event_number)

            producer.send(TOPIC, value=event)
            producer.flush()

            print(f"Sent event: {event['event_id']}")
            print(json.dumps(event, indent=2))
            print("-" * 50)

            event_number += 1
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
