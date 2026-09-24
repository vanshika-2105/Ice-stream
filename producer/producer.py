import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


# ============================================================
# Ice-Stream Continuous Streaming Producer
# Day 16
# ============================================================

print("=" * 60)
print("Ice-Stream Continuous Streaming Producer")
print("=" * 60)


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "checkout-events"


# ============================================================
# Event rate configuration
#
# NORMAL  : 0.1  -> approximately 10 events/sec
# MEDIUM  : 0.05 -> approximately 20 events/sec
# STRESS  : 0.02 -> approximately 50 events/sec
#
# Start with NORMAL. Do not immediately use stress rate.
# ============================================================

STREAM_RATE = "HUNDRED"

RATE_INTERVALS = {
    "FIVE": 0.2,
    "NORMAL": 0.1,
    "MEDIUM": 0.05,
    "TWENTY_FIVE": 0.04,
    "STRESS": 0.02,
    "HUNDRED": 0.01,
}

if STREAM_RATE not in RATE_INTERVALS:
    raise ValueError(
        f"Unknown STREAM_RATE: {STREAM_RATE}. "
        f"Use NORMAL, MEDIUM, or STRESS."
    )

EVENT_INTERVAL = RATE_INTERVALS[STREAM_RATE]


# ============================================================
# Kafka retry configuration
# ============================================================

MAX_RETRIES = 5
RETRY_DELAY = 3


# ============================================================
# Quality scenario configuration
# ============================================================

# Available scenarios:
#
# NORMAL
# MISSING_FIELDS
# BAD_QUANTITY
# BAD_AMOUNT
# BAD_CURRENCY
# MIXED_ERRORS

QUALITY_SCENARIO = "NORMAL"


# ============================================================
# Scenario validation
# ============================================================

VALID_SCENARIOS = {
    "NORMAL",
    "MISSING_FIELDS",
    "BAD_QUANTITY",
    "BAD_AMOUNT",
    "BAD_CURRENCY",
    "MIXED_ERRORS",
}

if QUALITY_SCENARIO not in VALID_SCENARIOS:
    raise ValueError(
        f"Unknown QUALITY_SCENARIO: {QUALITY_SCENARIO}"
    )


# ============================================================
# Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    retries=0,
)


# ============================================================
# Base checkout event
# ============================================================

def create_base_event(event_number):
    """
    Create the existing checkout business event.

    The existing schema is preserved.
    Event timestamp is generated in UTC.
    """

    return {
        "event_id": f"evt_{event_number:03d}",
        "event_type": "checkout",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ord_{event_number:04d}",
        "customer_id": f"cust_{random.randint(1, 300):03d}",
        "product_id": f"prod_{random.randint(1, 100):03d}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": random.choice(
            ["INR", "USD", "EUR", "GBP"]
        ),
    }


# ============================================================
# Missing field scenario
# ============================================================

def apply_missing_fields(event, event_number):

    fields = [
        "event_id",
        "quantity",
        "amount",
        "currency",
    ]

    field_to_remove = fields[
        (event_number - 1) % len(fields)
    ]

    del event[field_to_remove]

    return event, f"missing_{field_to_remove}"


# ============================================================
# Bad quantity scenario
# ============================================================

def apply_bad_quantity(event, event_number):

    bad_values = [
        0,
        -5,
        "ten",
    ]

    value = bad_values[
        (event_number - 1) % len(bad_values)
    ]

    event["quantity"] = value

    return event, f"quantity_{repr(value)}"


# ============================================================
# Bad amount scenario
# ============================================================

def apply_bad_amount(event, event_number):

    bad_values = [
        -10,
        "abc",
    ]

    value = bad_values[
        (event_number - 1) % len(bad_values)
    ]

    event["amount"] = value

    return event, f"amount_{repr(value)}"


# ============================================================
# Bad currency scenario
# ============================================================

def apply_bad_currency(event):

    event["currency"] = "XYZ"

    return event, "invalid_currency"


# ============================================================
# Bad timestamp scenario
# ============================================================

def apply_bad_timestamp(event):

    event["timestamp"] = "not-a-valid-timestamp"

    return event, "invalid_timestamp"


# ============================================================
# Mixed error scenario
# ============================================================

def apply_mixed_error(event, error_number):

    error_types = [
        "missing_field",
        "bad_quantity",
        "bad_amount",
        "bad_currency",
        "bad_timestamp",
    ]

    error_type = error_types[
        (error_number - 1) % len(error_types)
    ]

    if error_type == "missing_field":

        fields = [
            "event_id",
            "quantity",
            "amount",
            "currency",
        ]

        field_to_remove = fields[
            (error_number - 1) % len(fields)
        ]

        del event[field_to_remove]

        return event, f"missing_{field_to_remove}"

    if error_type == "bad_quantity":

        bad_values = [
            0,
            -5,
            "ten",
        ]

        value = bad_values[
            (error_number - 1) % len(bad_values)
        ]

        event["quantity"] = value

        return event, f"quantity_{repr(value)}"

    if error_type == "bad_amount":

        bad_values = [
            -10,
            "abc",
        ]

        value = bad_values[
            (error_number - 1) % len(bad_values)
        ]

        event["amount"] = value

        return event, f"amount_{repr(value)}"

    if error_type == "bad_currency":

        return apply_bad_currency(event)

    if error_type == "bad_timestamp":

        return apply_bad_timestamp(event)

    return event, None


# ============================================================
# Event generation
# ============================================================

def generate_checkout_event(event_number):

    event = create_base_event(event_number)

    # --------------------------------------------------------
    # NORMAL
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "NORMAL":

        if event_number % 50 == 0:

            event["currency"] = "XYZ"

            return event, False, "invalid_currency"

        return event, True, None

    # --------------------------------------------------------
    # MISSING_FIELDS
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "MISSING_FIELDS":

        event, error_type = apply_missing_fields(
            event,
            event_number,
        )

        return event, False, error_type

    # --------------------------------------------------------
    # BAD_QUANTITY
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "BAD_QUANTITY":

        event, error_type = apply_bad_quantity(
            event,
            event_number,
        )

        return event, False, error_type

    # --------------------------------------------------------
    # BAD_AMOUNT
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "BAD_AMOUNT":

        event, error_type = apply_bad_amount(
            event,
            event_number,
        )

        return event, False, error_type

    # --------------------------------------------------------
    # BAD_CURRENCY
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "BAD_CURRENCY":

        event, error_type = apply_bad_currency(event)

        return event, False, error_type

    # --------------------------------------------------------
    # MIXED_ERRORS
    # --------------------------------------------------------

    if QUALITY_SCENARIO == "MIXED_ERRORS":

        # Every 4th event is invalid.
        #
        # Expected:
        # 75% valid
        # 25% invalid

        if event_number % 4 != 0:

            return event, True, None

        error_number = event_number // 4

        event, error_type = apply_mixed_error(
            event,
            error_number,
        )

        return event, False, error_type

    raise ValueError(
        f"Unsupported scenario: {QUALITY_SCENARIO}"
    )


# ============================================================
# Kafka send with retry
# ============================================================

def send_event_with_retry(event):

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            future = producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            future.get(timeout=10)

            return True

        except (
            KafkaTimeoutError,
            KafkaError,
        ) as error:

            print(
                f"[WARN] Kafka send failed "
                f"(attempt {attempt}/{MAX_RETRIES}): {error}"
            )

            if attempt < MAX_RETRIES:

                print(
                    f"[INFO] Retrying in "
                    f"{RETRY_DELAY} second(s)..."
                )

                time.sleep(RETRY_DELAY)

            else:

                print(
                    "[ERROR] Maximum retries reached. "
                    "Event was not sent."
                )

    return False


# ============================================================
# Main continuous producer loop
# ============================================================

def main():

    print(
        f"[INFO] Kafka broker: "
        f"{KAFKA_BROKER}"
    )

    print(
        f"[INFO] Kafka topic: "
        f"{KAFKA_TOPIC}"
    )

    print(
        f"[INFO] Streaming mode: "
        f"CONTINUOUS"
    )

    print(
        f"[INFO] Rate profile: "
        f"{STREAM_RATE}"
    )

    print(
        f"[INFO] Event interval: "
        f"{EVENT_INTERVAL} second(s)"
    )

    target_rate = 1 / EVENT_INTERVAL

    print(
        f"[INFO] Target event rate: "
        f"approximately {target_rate:.2f} events/sec"
    )

    print(
        f"[INFO] Quality scenario: "
        f"{QUALITY_SCENARIO}"
    )

    print(
        "[INFO] Press Ctrl+C to stop the producer."
    )

    print("-" * 60)

    valid_count = 0
    invalid_count = 0
    sent_count = 0
    failed_count = 0
    event_number = 1

    # --------------------------------------------------------
    # Start throughput measurement
    # --------------------------------------------------------

    start_time = time.perf_counter()

    try:

        while True:

            (
                event,
                expected_valid,
                scenario_error,
            ) = generate_checkout_event(
                event_number
            )

            send_success = send_event_with_retry(
                event
            )

            if send_success:

                sent_count += 1

                if expected_valid:

                    valid_count += 1

                    print(
                        f"[OK] "
                        f"{event.get('event_id', 'NO_EVENT_ID')} "
                        f"valid"
                    )

                else:

                    invalid_count += 1

                    print(
                        f"[INVALID] "
                        f"{event.get('event_id', 'NO_EVENT_ID')} "
                        f"{scenario_error}"
                    )

            else:

                failed_count += 1

                print(
                    f"[ERROR] Failed to send event "
                    f"{event_number}"
                )

            event_number += 1

            # ------------------------------------------------
            # Controlled streaming interval
            # ------------------------------------------------

            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:

        print()
        print(
            "[INFO] Producer stopped by user."
        )

    finally:

        # ----------------------------------------------------
        # Flush remaining Kafka messages
        # ----------------------------------------------------

        producer.flush()

        elapsed_seconds = (
            time.perf_counter() - start_time
        )

        producer.close()

        # ----------------------------------------------------
        # Final measurements
        # ----------------------------------------------------

        print("-" * 60)

        print(
            "[INFO] Producer finished."
        )

        print(
            f"[INFO] Events attempted: "
            f"{event_number - 1}"
        )

        print(
            f"[INFO] Events sent: "
            f"{sent_count}"
        )

        print(
            f"[INFO] Events failed: "
            f"{failed_count}"
        )

        print(
            f"[INFO] Expected valid: "
            f"{valid_count}"
        )

        print(
            f"[INFO] Expected invalid: "
            f"{invalid_count}"
        )

        print(
            f"[INFO] Elapsed time: "
            f"{elapsed_seconds:.2f} seconds"
        )

        # ----------------------------------------------------
        # Actual producer throughput
        # ----------------------------------------------------

        if elapsed_seconds > 0:

            throughput = (
                sent_count / elapsed_seconds
            )

            print(
                f"[INFO] Actual throughput: "
                f"{throughput:.2f} events/sec"
            )

        # ----------------------------------------------------
        # Quality measurements
        # ----------------------------------------------------

        if sent_count > 0:

            quality = (
                valid_count / sent_count
            ) * 100

            invalid_rate = (
                invalid_count / sent_count
            ) * 100

            print(
                f"[INFO] Expected quality: "
                f"{quality:.2f}%"
            )

            print(
                f"[INFO] Expected invalid rate: "
                f"{invalid_rate:.2f}%"
            )

        print(
            "[INFO] Kafka producer closed."
        )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()
