from pyflink.table import EnvironmentSettings, TableEnvironment


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_SOURCE_TOPIC = "checkout-events"
KAFKA_GROUP_ID = "ice-stream-validation"
KAFKA_DLQ_TOPIC = "checkout-events-dlq"

ICEBERG_CATALOG = "iceberg_catalog"
ICEBERG_DATABASE = "checkout"
ICEBERG_TABLE = "checkout_events"
ICEBERG_METRICS_TABLE = "quality_metrics"


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    # ------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------

    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval",
        "60s"
    )

    # ------------------------------------------------------------
    # Iceberg REST Catalog
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE CATALOG {ICEBERG_CATALOG} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = 'http://iceberg-rest:8181',
            'warehouse' = 's3://warehouse/',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            's3.region' = 'us-east-1',
            's3.path-style-access' = 'true'
        )
        """
    )

    # ------------------------------------------------------------
    # Iceberg Database
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}
        """
    )

    # ------------------------------------------------------------
    # Kafka Source
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE TABLE kafka_checkout_events (
            event_id STRING,
            event_type STRING,
            event_timestamp STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            currency STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{KAFKA_GROUP_ID}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
        """
    )

    # ------------------------------------------------------------
    # Valid Events -> Iceberg
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE} (
            event_id STRING,
            event_type STRING,
            event_timestamp STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            currency STRING,
            total_value DOUBLE,
            processed_at TIMESTAMP_LTZ(3)
        )
        """
    )

    # ------------------------------------------------------------
    # Quality Metrics -> Iceberg
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_METRICS_TABLE} (
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            total_events BIGINT,
            valid_events BIGINT,
            invalid_events BIGINT,
            quality_score DOUBLE,
            invalid_event_rate DOUBLE
        )
        """
    )

    # ------------------------------------------------------------
    # Invalid Events -> Kafka DLQ
    # ------------------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE TABLE checkout_events_dlq (
            event_id STRING,
            event_type STRING,
            event_timestamp STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            currency STRING,
            validation_error STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_DLQ_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
        """
    )

    # ------------------------------------------------------------
    # Statement Set
    # ------------------------------------------------------------

    statement_set = table_env.create_statement_set()

    # ------------------------------------------------------------
    # VALID EVENTS -> ICEBERG
    # ------------------------------------------------------------

    statement_set.add_insert_sql(
        f"""
        INSERT INTO
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE}
        SELECT
            event_id,
            event_type,
            event_timestamp,
            order_id,
            customer_id,
            product_id,
            quantity,
            amount,
            currency,
            ROUND(quantity * amount, 2),
            CURRENT_TIMESTAMP
        FROM kafka_checkout_events
        WHERE
            event_id IS NOT NULL
            AND event_type = 'checkout'
            AND order_id IS NOT NULL
            AND customer_id IS NOT NULL
            AND product_id IS NOT NULL
            AND quantity IS NOT NULL
            AND quantity > 0
            AND amount IS NOT NULL
            AND amount >= 0
            AND currency IN ('INR', 'USD', 'EUR', 'GBP')
        """
    )

    # ------------------------------------------------------------
    # INVALID EVENTS -> DLQ
    # ------------------------------------------------------------

    statement_set.add_insert_sql(
        """
        INSERT INTO checkout_events_dlq
        SELECT
            event_id,
            event_type,
            event_timestamp,
            order_id,
            customer_id,
            product_id,
            quantity,
            amount,
            currency,
            CASE
                WHEN event_id IS NULL
                    THEN 'Missing event_id'
                WHEN event_type IS NULL
                    THEN 'Missing event_type'
                WHEN event_type <> 'checkout'
                    THEN 'Invalid event_type'
                WHEN order_id IS NULL
                    THEN 'Missing order_id'
                WHEN customer_id IS NULL
                    THEN 'Missing customer_id'
                WHEN product_id IS NULL
                    THEN 'Missing product_id'
                WHEN quantity IS NULL
                    THEN 'Missing quantity'
                WHEN quantity <= 0
                    THEN 'Invalid quantity'
                WHEN amount IS NULL
                    THEN 'Missing amount'
                WHEN amount < 0
                    THEN 'Invalid amount'
                WHEN currency IS NULL
                    THEN 'Missing currency'
                WHEN currency NOT IN ('INR', 'USD', 'EUR', 'GBP')
                    THEN 'Invalid currency'
                ELSE 'Unknown validation error'
            END
        FROM kafka_checkout_events
        WHERE
            event_id IS NULL
            OR event_type IS NULL
            OR event_type <> 'checkout'
            OR order_id IS NULL
            OR customer_id IS NULL
            OR product_id IS NULL
            OR quantity IS NULL
            OR quantity <= 0
            OR amount IS NULL
            OR amount < 0
            OR currency IS NULL
            OR currency NOT IN ('INR', 'USD', 'EUR', 'GBP')
        """
    )

    # ------------------------------------------------------------
    # 1-MINUTE WINDOWED QUALITY METRICS -> ICEBERG
    # ------------------------------------------------------------

    statement_set.add_insert_sql(
        f"""
        INSERT INTO
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_METRICS_TABLE}
        SELECT
            window_start,
            window_end,
            COUNT(*) AS total_events,

            SUM(
                CASE
                    WHEN
                        event_id IS NOT NULL
                        AND event_type = 'checkout'
                        AND order_id IS NOT NULL
                        AND customer_id IS NOT NULL
                        AND product_id IS NOT NULL
                        AND quantity IS NOT NULL
                        AND quantity > 0
                        AND amount IS NOT NULL
                        AND amount >= 0
                        AND currency IN ('INR', 'USD', 'EUR', 'GBP')
                    THEN 1
                    ELSE 0
                END
            ) AS valid_events,

            SUM(
                CASE
                    WHEN
                        event_id IS NULL
                        OR event_type IS NULL
                        OR event_type <> 'checkout'
                        OR order_id IS NULL
                        OR customer_id IS NULL
                        OR product_id IS NULL
                        OR quantity IS NULL
                        OR quantity <= 0
                        OR amount IS NULL
                        OR amount < 0
                        OR currency IS NULL
                        OR currency NOT IN ('INR', 'USD', 'EUR', 'GBP')
                    THEN 1
                    ELSE 0
                END
            ) AS invalid_events,

            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE
                    CAST(
                        SUM(
                            CASE
                                WHEN
                                    event_id IS NOT NULL
                                    AND event_type = 'checkout'
                                    AND order_id IS NOT NULL
                                    AND customer_id IS NOT NULL
                                    AND product_id IS NOT NULL
                                    AND quantity IS NOT NULL
                                    AND quantity > 0
                                    AND amount IS NOT NULL
                                    AND amount >= 0
                                    AND currency IN ('INR', 'USD', 'EUR', 'GBP')
                                THEN 1
                                ELSE 0
                            END
                        ) AS DOUBLE
                    ) / COUNT(*) * 100.0
            END AS quality_score,

            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE
                    CAST(
                        SUM(
                            CASE
                                WHEN
                                    event_id IS NULL
                                    OR event_type IS NULL
                                    OR event_type <> 'checkout'
                                    OR order_id IS NULL
                                    OR customer_id IS NULL
                                    OR product_id IS NULL
                                    OR quantity IS NULL
                                    OR quantity <= 0
                                    OR amount IS NULL
                                    OR amount < 0
                                    OR currency IS NULL
                                    OR currency NOT IN ('INR', 'USD', 'EUR', 'GBP')
                                THEN 1
                                ELSE 0
                            END
                        ) AS DOUBLE
                    ) / COUNT(*) * 100.0
            END AS invalid_event_rate

        FROM TABLE(
            TUMBLE(
                TABLE kafka_checkout_events,
                DESCRIPTOR(proc_time),
                INTERVAL '1' MINUTE
            )
        )
        GROUP BY window_start, window_end
        """
    )

    # ------------------------------------------------------------
    # Start Streaming Job
    # ------------------------------------------------------------

    result = statement_set.execute()
    result.wait()


if __name__ == "__main__":
    main()