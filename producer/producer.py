import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "checkout-events")


# Day 8 simulation configuration
EVENT_INTERVAL = float(os.getenv("EVENT_INTERVAL", "1"))
INVALID_EVENT_RATE = float(os.getenv("INVALID_EVENT_RATE", "0.10"))


# Validate configuration
if EVENT_INTERVAL <= 0:
    raise ValueError("EVENT_INTERVAL must be greater than 0")

if not 0 <= INVALID_EVENT_RATE <= 1:
    raise ValueError("INVALID_EVENT_RATE must be between 0 and 1")


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_checkout_event(event_number):
    """Generate a valid or intentionally invalid checkout event."""

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

    # Randomly create an invalid event according to the configured rate.
    if random.random() < INVALID_EVENT_RATE:

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

    invalid_percentage = INVALID_EVENT_RATE * 100
    valid_percentage = 100 - invalid_percentage

    print("=" * 60)
    print("Ice-Stream Day 8 Kafka Producer")
    print("=" * 60)
    print(f"[INFO] Kafka broker: {KAFKA_BROKER}")
    print(f"[INFO] Kafka topic: {TOPIC}")
    print(f"[INFO] Event interval: {EVENT_INTERVAL} second(s)")
    print(f"[INFO] Invalid event rate: {invalid_percentage:.1f}%")
    print(
        f"[INFO] Expected distribution: "
        f"{valid_percentage:.1f}% valid / "
        f"{invalid_percentage:.1f}% invalid"
    )
    print("[INFO] Producer mode: CONTINUOUS")
    print("[INFO] Press Ctrl+C to stop.")
    print("=" * 60)

    try:

        while True:

            # Generate event
            event, is_valid, invalid_type = generate_checkout_event(
                event_number
            )

            # Send both valid and invalid events to checkout-events.
            # Flink will validate them and route invalid events to the DLQ.
            future = producer.send(
                TOPIC,
                value=event
            )

            # Wait for Kafka acknowledgement.
            future.get(timeout=10)

            # Log event status.
            if is_valid:
                print(
                    f"[INFO] Event sent successfully: "
                    f"{event['event_id']} | VALID"
                )
            else:
                print(
                    f"[WARN] Event sent: "
                    f"{event['event_id']} | "
                    f"INVALID | Reason: {invalid_type}"
                )

            event_number += 1

            # Wait before generating the next event.
            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:

        print("\n[INFO] Producer stopped by user.")

    except Exception as error:

        print(f"[ERROR] Producer stopped unexpectedly: {error}")
        raise

    finally:

        producer.flush()
        producer.close()

        print("[INFO] Kafka producer closed.")


if __name__ == "__main__":
    main()