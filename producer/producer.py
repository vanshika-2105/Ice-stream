import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, KafkaError


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "checkout-events")


# ============================================================
# Day 8 simulation configuration
# ============================================================

EVENT_INTERVAL = float(os.getenv("EVENT_INTERVAL", "1"))

# Kept for backward compatibility with the existing producer.
# Day 12 scenarios determine the actual invalid-event rate.
INVALID_EVENT_RATE = float(os.getenv("INVALID_EVENT_RATE", "0.10"))


# ============================================================
# Day 12 anomaly scenario configuration
# ============================================================

# Change ONLY this value to test a different quality scenario.
#
# Available scenarios:
# NORMAL
# SUDDEN_DROP
# GRADUAL_DEGRADATION
# RECOVERY

QUALITY_SCENARIO = "NORMAL"


# Number of events used to represent one quality window.
# With EVENT_INTERVAL = 1 second, 60 events ≈ 1 minute.
WINDOW_SIZE = 60


# Invalid-event rates for each scenario.
#
# NORMAL:
#       ~97% quality
#
# SUDDEN_DROP:
#       96%, 96%, 96%, 96%, 82%
#
# GRADUAL_DEGRADATION:
#       98%, 97%, 95%, 93%, 91%, 89%
#
# RECOVERY:
#       82%, 87%, 91%, 95%, 97%

SCENARIO_RATES = {
    "NORMAL": [
        0.03
    ],
    "SUDDEN_DROP": [
        0.04,
        0.04,
        0.04,
        0.04,
        0.18
    ],
    "GRADUAL_DEGRADATION": [
        0.02,
        0.03,
        0.05,
        0.07,
        0.09,
        0.11
    ],
    "RECOVERY": [
        0.18,
        0.13,
        0.09,
        0.05,
        0.03
    ]
}


# ============================================================
# Day 10 reliability configuration
# ============================================================

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "3"))


# ============================================================
# Validate configuration
# ============================================================

if EVENT_INTERVAL <= 0:
    raise ValueError("EVENT_INTERVAL must be greater than 0")

if not 0 <= INVALID_EVENT_RATE <= 1:
    raise ValueError("INVALID_EVENT_RATE must be between 0 and 1")

if QUALITY_SCENARIO not in SCENARIO_RATES:
    raise ValueError(
        f"Invalid QUALITY_SCENARIO: {QUALITY_SCENARIO}. "
        f"Choose from: {list(SCENARIO_RATES.keys())}"
    )

if WINDOW_SIZE <= 0:
    raise ValueError("WINDOW_SIZE must be greater than 0")

if MAX_RETRIES <= 0:
    raise ValueError("MAX_RETRIES must be greater than 0")

if RETRY_DELAY <= 0:
    raise ValueError("RETRY_DELAY must be greater than 0")


# ============================================================
# Create Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# Day 12 scenario logic
# ============================================================

def get_invalid_event_rate(event_number):
    """
    Return the invalid-event rate for the current
    Day 12 quality scenario and event window.
    """

    rates = SCENARIO_RATES[QUALITY_SCENARIO]

    # NORMAL uses the same quality level continuously.
    if QUALITY_SCENARIO == "NORMAL":
        return rates[0]

    # Determine which scenario window the event belongs to.
    window_number = (event_number - 1) // WINDOW_SIZE

    # Once the scenario reaches its final window,
    # keep using the final rate.
    if window_number >= len(rates):
        window_number = len(rates) - 1

    return rates[window_number]


def get_current_window(event_number):
    """Return the current one-based scenario window number."""

    return ((event_number - 1) // WINDOW_SIZE) + 1


# ============================================================
# Checkout event generation
# ============================================================

def generate_checkout_event(event_number):
    """Generate a valid or intentionally invalid checkout event."""

    event = {
        "event_id": f"evt_{event_number:03d}",
        "event_type": "checkout",
        "timestamp": datetime.now(timezone.utc).isoformat().replace(
            "+00:00",
            "Z"
        ),
        "order_id": f"ORD_{event_number:03d}",
        "customer_id": f"CUS_{random.randint(1, 100):03d}",
        "product_id": f"PROD_{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": "INR"
    }

    # Day 12:
    # Determine invalid-event probability from the selected scenario.
    invalid_event_rate = get_invalid_event_rate(event_number)

    if random.random() < invalid_event_rate:

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


# ============================================================
# Kafka send with retry
# ============================================================

def send_event_with_retry(event):
    """Send an event to Kafka with retry handling."""

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            future = producer.send(
                TOPIC,
                value=event
            )

            future.get(timeout=10)

            if attempt > 1:
                print("[INFO] Kafka connection restored")
                print("[INFO] Producer resumed")

            return True

        except (KafkaTimeoutError, KafkaError) as error:

            print(f"[ERROR] Kafka unavailable: {error}")

            if attempt < MAX_RETRIES:

                print(
                    f"[INFO] Retrying connection... "
                    f"attempt {attempt}/{MAX_RETRIES}"
                )

                time.sleep(RETRY_DELAY)

            else:

                print("[ERROR] Maximum retry attempts reached.")

    return False


# ============================================================
# Main producer
# ============================================================

def main():

    event_number = 1

    initial_invalid_rate = get_invalid_event_rate(1)

    invalid_percentage = initial_invalid_rate * 100
    valid_percentage = 100 - invalid_percentage

    print("=" * 60)
    print("Ice-Stream Day 12 Anomaly Scenario Producer")
    print("=" * 60)

    print(f"[INFO] Kafka broker: {KAFKA_BROKER}")
    print(f"[INFO] Kafka topic: {TOPIC}")
    print(f"[INFO] Event interval: {EVENT_INTERVAL} second(s)")
    print(f"[INFO] Quality scenario: {QUALITY_SCENARIO}")
    print(f"[INFO] Scenario window size: {WINDOW_SIZE} events")

    print(
        f"[INFO] Initial quality distribution: "
        f"{valid_percentage:.1f}% valid / "
        f"{invalid_percentage:.1f}% invalid"
    )

    print(f"[INFO] Maximum retries: {MAX_RETRIES}")
    print(f"[INFO] Retry delay: {RETRY_DELAY} second(s)")
    print("[INFO] Producer mode: CONTINUOUS")
    print("[INFO] Press Ctrl+C to stop.")
    print("=" * 60)

    try:

        while True:

            # Display a message when a new scenario window starts.
            if (event_number - 1) % WINDOW_SIZE == 0:

                current_window = get_current_window(event_number)
                current_invalid_rate = get_invalid_event_rate(event_number)

                current_invalid_percentage = current_invalid_rate * 100
                current_valid_percentage = 100 - current_invalid_percentage

                print(
                    f"[SCENARIO] Window {current_window} | "
                    f"Expected quality: "
                    f"{current_valid_percentage:.1f}% | "
                    f"Invalid rate: "
                    f"{current_invalid_percentage:.1f}%"
                )

            event, is_valid, invalid_type = generate_checkout_event(
                event_number
            )

            send_success = send_event_with_retry(event)

            if not send_success:

                print(
                    f"[ERROR] Failed to send event "
                    f"{event['event_id']} after retries."
                )

                time.sleep(RETRY_DELAY)
                continue

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