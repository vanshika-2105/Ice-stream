import json
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "checkout-events"
KAFKA_GROUP_ID = "ice-stream-checkout"


def process_event(message):
    """
    Parse and transform one Kafka message.

    Invalid JSON is logged and ignored so that one bad
    event does not stop the complete streaming job.
    """

    try:
        event = json.loads(message)

        quantity = int(event["quantity"])
        amount = float(event["amount"])

        total_value = round(quantity * amount, 2)

        processed_event = {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "timestamp": event["timestamp"],
            "order_id": event["order_id"],
            "customer_id": event["customer_id"],
            "product_id": event["product_id"],
            "quantity": quantity,
            "amount": amount,
            "currency": event["currency"],
            "total_value": total_value,
            "processed_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }

        return json.dumps(processed_event)

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"ERROR: Invalid checkout event: {message}")
        print(f"Reason: {error}")

        return None


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Checkout Kafka Source",
    )

    processed_events = (
        events
        .map(process_event)
        .filter(lambda event: event is not None)
    )

    processed_events.print()

    env.execute("Ice-Stream Checkout Processing")


if __name__ == "__main__":
    main()