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
    """Generate a checkout event that is valid or intentionally invalid."""

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

    # Approximately 10% of events are intentionally invalid.
    if event_number % 10 == 0:
        invalid_type = random.choice([
            "missing_customer",
            "zero_quantity",
            "negative_amount",
            "invalid_currency"
        ])

        if invalid_type == "missing_customer":
            del event["customer_id"]

        elif invalid_type == "zero_quantity":
            event["quantity"] = 0

        elif invalid_type == "negative_amount":
            event["amount"] = -100.00

        elif invalid_type == "invalid_currency":
            event["currency"] = "XYZ"

        return event, False, invalid_type

    return event, True, None


def main():
    event_number = 1

    print("Starting Ice-Stream Kafka Producer...")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print("Target distribution: approximately 90% valid / 10% invalid")
    print("Sending one checkout event every second...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event, is_valid, invalid_type = generate_checkout_event(event_number)

            producer.send(TOPIC, value=event)
            producer.flush()

            if is_valid:
                print(f"[VALID] Sent event: {event['event_id']}")
            else:
                print(
                    f"[INVALID] Sent event: {event['event_id']} "
                    f"(reason: {invalid_type})"
                )

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