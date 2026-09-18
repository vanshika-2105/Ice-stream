import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


# ============================================================
# Ice-Stream Day 13 Anomaly Scenario Producer
# ============================================================

print("=" * 60)
print("Ice-Stream Day 13 Anomaly Scenario Producer")
print("=" * 60)


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "checkout-events"

EVENT_INTERVAL = 1.0

MAX_RETRIES = 5
RETRY_DELAY = 3


# ============================================================
# Day 13 anomaly scenario configuration
# ============================================================

# Change ONLY this value to test a different quality scenario.
#
# Available scenarios:
# NORMAL
# SUDDEN_DROP
# GRADUAL_DEGRADATION
# RECOVERY

QUALITY_SCENARIO = "SUDDEN_DROP"

# Number of events in each scenario window.
WINDOW_SIZE = 60


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
# Scenario validation
# ============================================================

if QUALITY_SCENARIO not in SCENARIO_RATES:
    raise ValueError(
        f"Unknown QUALITY_SCENARIO: {QUALITY_SCENARIO}"
    )


# ============================================================
# Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    retries=0
)


# ============================================================
# Scenario helper functions
# ============================================================

def get_current_window(event_number):
    """
    Return the 1-based scenario window number.
    """

    return ((event_number - 1) // WINDOW_SIZE) + 1


def get_invalid_event_rate(event_number):
    """
    Return the invalid-event rate for the current scenario window.
    """

    rates = SCENARIO_RATES[QUALITY_SCENARIO]

    window_number = (event_number - 1) // WINDOW_SIZE

    if window_number >= len(rates):
        window_number = len(rates) - 1

    return rates[window_number]


def get_expected_quality(event_number):
    """
    Return expected quality percentage.
    """

    invalid_rate = get_invalid_event_rate(event_number)

    return (1 - invalid_rate) * 100


# ============================================================
# Checkout event generation
# ============================================================

def generate_checkout_event(event_number):
    """
    Generate one checkout event.

    Returns:
        event
        is_valid
        invalid_type
    """

    invalid_rate = get_invalid_event_rate(event_number)

    event_id = f"evt_{event_number:03d}"

    event = {
        "event_id": event_id,
        "event_type": "checkout",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ord_{event_number:04d}",
        "customer_id": f"cust_{random.randint(1, 300):03d}",
        "product_id": f"prod_{random.randint(1, 100):03d}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": random.choice(
            ["INR", "USD", "EUR", "GBP"]
        )
    }

    is_invalid = random.random() < invalid_rate

    if not is_invalid:
        return event, True, None

    invalid_type = random.choice([
        "missing_customer",
        "zero_quantity",
        "negative_amount",
        "invalid_currency"
    ])

    if invalid_type == "missing_customer":
        event["customer_id"] = None

    elif invalid_type == "zero_quantity":
        event["quantity"] = 0

    elif invalid_type == "negative_amount":
        event["amount"] = -abs(event["amount"])

    elif invalid_type == "invalid_currency":
        event["currency"] = "XYZ"

    return event, False, invalid_type


# ============================================================
# Kafka send with retry
# ============================================================

def send_event_with_retry(event):
    """
    Send one event to Kafka with retry handling.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            future = producer.send(
                KAFKA_TOPIC,
                value=event
            )

            future.get(timeout=10)

            return True

        except (KafkaTimeoutError, KafkaError) as error:

            print(
                f"[WARN] Kafka send failed "
                f"(attempt {attempt}/{MAX_RETRIES}): {error}"
            )

            if attempt < MAX_RETRIES:

                print(
                    f"[INFO] Retrying in {RETRY_DELAY} second(s)..."
                )

                time.sleep(RETRY_DELAY)

            else:

                print(
                    "[ERROR] Maximum retries reached. "
                    "Event was not sent."
                )

    return False


# ============================================================
# Main producer loop
# ============================================================

def main():

    # Continue from event 269 because events 001-268
    # were already generated during the previous run.
    event_number = 269

    initial_invalid_rate = get_invalid_event_rate(event_number)

    print(f"[INFO] Kafka broker: {KAFKA_BROKER}")
    print(f"[INFO] Kafka topic: {KAFKA_TOPIC}")
    print(f"[INFO] Event interval: {EVENT_INTERVAL} second(s)")
    print(f"[INFO] Quality scenario: {QUALITY_SCENARIO}")
    print(f"[INFO] Window size: {WINDOW_SIZE} events")
    print(
        f"[INFO] Starting event number: {event_number}"
    )
    print(
        f"[INFO] Starting expected quality: "
        f"{(1 - initial_invalid_rate) * 100:.0f}%"
    )
    print(
        f"[INFO] Starting expected invalid rate: "
        f"{initial_invalid_rate * 100:.0f}%"
    )
    print("[INFO] Continuing SUDDEN_DROP anomaly window...")
    print("-" * 60)

    current_window = None

    try:

        while True:

            # Stop after event 300.
            if event_number > 300:

                print("-" * 60)
                print(
                    "[INFO] Completed events 269-300."
                )
                print(
                    "[INFO] SUDDEN_DROP anomaly window 5 "
                    "is now complete across both runs "
                    "(events 241-300)."
                )
                break

            window_number = get_current_window(event_number)

            if window_number != current_window:

                current_window = window_number

                invalid_rate = get_invalid_event_rate(
                    event_number
                )

                expected_quality = get_expected_quality(
                    event_number
                )

                window_start = (
                    (window_number - 1) * WINDOW_SIZE
                ) + 1

                window_end = (
                    window_number * WINDOW_SIZE
                )

                print(
                    f"[INFO] Window {window_number}: "
                    f"events {window_start}-{window_end}"
                )

                print(
                    f"[INFO] Expected quality: "
                    f"{expected_quality:.0f}%"
                )

                print(
                    f"[INFO] Expected invalid rate: "
                    f"{invalid_rate * 100:.0f}%"
                )

            event, is_valid, invalid_type = (
                generate_checkout_event(event_number)
            )

            send_success = send_event_with_retry(event)

            if send_success:

                if is_valid:

                    print(
                        f"[OK] {event['event_id']} "
                        f"valid"
                    )

                else:

                    print(
                        f"[INVALID] {event['event_id']} "
                        f"{invalid_type}"
                    )

            else:

                print(
                    f"[ERROR] Failed to send "
                    f"{event['event_id']}"
                )

            event_number += 1

            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:

        print()
        print("[INFO] Producer stopped by user.")

    finally:

        producer.flush()
        producer.close()

        print("[INFO] Kafka producer closed.")


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()