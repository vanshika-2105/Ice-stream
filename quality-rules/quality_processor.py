import json
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, ".")

from decision import make_quality_decision


KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "checkout-events"
VALID_TOPIC = "checkout-events-valid"
DLQ_TOPIC = "checkout-events-dlq"

CONSUMER_GROUP = "ice-stream-quality-engine"


# Read raw Kafka messages first.
# JSON parsing is handled manually so malformed JSON can be caught.
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: value.decode("utf-8"),
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def current_timestamp():
    """Return the current UTC timestamp in ISO-8601 format."""

    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def process_event(event):
    """Validate one checkout event and route it."""

    event_id = event.get("event_id")

    print(f"[INFO] Received event {event_id}")

    decision = make_quality_decision(event)

    if decision["quality_status"] == "VALID":

        producer.send(
            VALID_TOPIC,
            value=event
        )
        producer.flush()

        print(f"[INFO] Event {event_id} -> VALID")

    else:

        invalid_event = {
            "event_id": event_id,
            "original_event": event,
            "errors": decision["quality_errors"],
            "failed_at": current_timestamp(),
        }

        producer.send(
            DLQ_TOPIC,
            value=invalid_event
        )
        producer.flush()

        print(
            f"[ERROR] Event {event_id} -> DLQ "
            f"({len(decision['quality_errors'])} error(s))"
        )


def main():

    print("Starting Ice-Stream Quality Engine...")
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Valid topic: {VALID_TOPIC}")
    print(f"DLQ topic: {DLQ_TOPIC}")
    print("Waiting for events...\n")

    try:

        for message in consumer:

            try:

                # Parse JSON manually so malformed JSON can be handled.
                event = json.loads(message.value)

                process_event(event)

            except json.JSONDecodeError as error:

                print(
                    f"[ERROR] Failed to parse event "
                    f"at partition {message.partition}, "
                    f"offset {message.offset}: {error}"
                )

                # Preserve the malformed raw payload in the DLQ record.
                invalid_event = {
                    "event_id": None,
                    "original_event": message.value,
                    "errors": [
                        {
                            "code": "MALFORMED_JSON",
                            "message": "Event payload is not valid JSON",
                        }
                    ],
                    "failed_at": current_timestamp(),
                }

                producer.send(
                    DLQ_TOPIC,
                    value=invalid_event
                )
                producer.flush()

            except Exception as error:

                print(
                    f"[ERROR] Failed to process event: {error}"
                )

    except KeyboardInterrupt:

        print("\nQuality Engine stopped.")

    finally:

        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()