from pyflink.table import EnvironmentSettings, TableEnvironment


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "checkout-events"
KAFKA_GROUP_ID = "ice-stream-checkout"

ICEBERG_CATALOG = "iceberg_catalog"
ICEBERG_DATABASE = "checkout"
ICEBERG_TABLE = "checkout_events"


def main():

    # Create a streaming Table Environment
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    # Enable Flink checkpoints so the Iceberg sink can commit files
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "10s"
    )

    # ---------------------------------------------------------
    # 1. Create Iceberg REST Catalog
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # 2. Create Iceberg namespace/database
    # ---------------------------------------------------------
    table_env.execute_sql(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}
        """
    )

    # ---------------------------------------------------------
    # 3. Create Kafka source table
    # ---------------------------------------------------------
    table_env.execute_sql(
        f"""
        CREATE TABLE kafka_checkout_events (
            event_id STRING,
            event_type STRING,
            `timestamp` STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            currency STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{KAFKA_GROUP_ID}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
        """
    )

    # ---------------------------------------------------------
    # 4. Create Iceberg sink table
    # ---------------------------------------------------------
    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE} (
            event_id STRING,
            event_type STRING,
            `timestamp` STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            amount DOUBLE,
            currency STRING,
            total_value DOUBLE,
            processed_at TIMESTAMP(3)
        )
        """
    )

    # ---------------------------------------------------------
    # 5. Kafka -> Flink transformation -> Iceberg
    # ---------------------------------------------------------
    result = table_env.execute_sql(
        f"""
        INSERT INTO
        {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE}
        SELECT
            event_id,
            event_type,
            `timestamp`,
            order_id,
            customer_id,
            product_id,
            quantity,
            amount,
            currency,
            ROUND(quantity * amount, 2) AS total_value,
            CURRENT_TIMESTAMP AS processed_at
        FROM kafka_checkout_events
        WHERE
            event_id IS NOT NULL
            AND event_type IS NOT NULL
            AND order_id IS NOT NULL
            AND customer_id IS NOT NULL
            AND product_id IS NOT NULL
            AND quantity IS NOT NULL
            AND amount IS NOT NULL
            AND currency IS NOT NULL
        """
    )

    # Keep the streaming job running
    result.wait()


if __name__ == "__main__":
    main()