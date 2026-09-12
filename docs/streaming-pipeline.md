# Ice-Stream Streaming Pipeline

## 1. Overview

Ice-Stream is a real-time streaming data pipeline that processes checkout events using **Apache Kafka, Apache Flink, and Apache Iceberg**.

The pipeline includes data-quality validation to identify invalid events and route them to a Dead Letter Queue (DLQ) for inspection and debugging.

### Pipeline Flow

```text
Producer
   ↓
Kafka: checkout-events
   ↓
Apache Flink
   ↓
Data Quality Validation
   ├── VALID → Iceberg
   │
   └── INVALID → Kafka: checkout-events-dlq
```

Valid events are transformed and written to the Iceberg table, while invalid events are preserved in the DLQ together with their original payload, validation errors, and failure timestamp.

---

## 2. Kafka Topics

| Topic                 | Purpose                            |
| --------------------- | ---------------------------------- |
| `checkout-events`     | Receives raw checkout events       |
| `checkout-events-dlq` | Stores invalid or malformed events |

The `checkout-events` topic acts as the input stream for the processing pipeline.

The `checkout-events-dlq` topic provides a separate location for events that fail validation or cannot be parsed correctly.

---

## 3. Event Schema

Checkout events follow Schema Version 1 and contain the following fields:

* `event_id`
* `event_type`
* `timestamp`
* `order_id`
* `customer_id`
* `product_id`
* `quantity`
* `amount`
* `currency`

Example valid event:

```json
{
  "event_id": "evt_001",
  "event_type": "checkout",
  "timestamp": "2026-09-03T10:30:00Z",
  "order_id": "ORD_001",
  "customer_id": "CUS_001",
  "product_id": "PROD_001",
  "quantity": 2,
  "amount": 1499.99,
  "currency": "INR"
}
```

---

## 4. Data Quality Validation

Incoming checkout events are validated by the Flink processing pipeline before being written to Iceberg.

### Validation Rules

The pipeline checks that:

* Required fields are present.
* `event_type` is valid.
* `quantity` is greater than 0.
* `amount` is not negative.
* `currency` is one of the supported currencies.
* The timestamp has a valid format.
* The event payload contains valid JSON.

Events that pass validation continue to the Iceberg storage layer.

Events that fail validation are routed to `checkout-events-dlq`.

---

## 5. Invalid Event Flow

Invalid events are routed to the `checkout-events-dlq` Kafka topic.

Each invalid event contains:

* Original event
* Validation errors
* Failure timestamp

Example:

```json
{
  "event_id": "day7_010",
  "original_event": {
    "event_id": "day7_010",
    "event_type": "checkout",
    "timestamp": "2026-09-03T10:30:10Z",
    "order_id": "ORD_day7_010",
    "customer_id": "CUS_day7_010",
    "product_id": "PROD_day7_010",
    "quantity": 0,
    "amount": 500,
    "currency": "INR"
  },
  "errors": [
    {
      "code": "NON_POSITIVE_VALUE",
      "message": "Quantity must be greater than 0"
    }
  ],
  "failed_at": "2026-09-03T10:30:10Z"
}
```

This structure allows failed events to be investigated without losing the original input data.

---

## 6. Error Handling

The pipeline handles different types of invalid input without stopping the overall stream processing.

### Invalid Quantity

An event with `quantity = 0` or a negative quantity is rejected.

Example validation error:

```text
NON_POSITIVE_VALUE
Quantity must be greater than 0
```

### Missing Required Field

An event missing a required field such as `customer_id` is rejected.

Example:

```text
REQUIRED_FIELD
Required field is missing
```

### Invalid Currency

An unsupported currency such as `XYZ` is rejected.

Example:

```text
INVALID_CURRENCY
Unsupported currency
```

### Malformed JSON

Malformed JSON cannot be parsed as a normal checkout event.

The pipeline captures the malformed payload and routes it to the DLQ with a `MALFORMED_JSON` error.

Example:

```json
{
  "event_id": null,
  "original_event": "{\"event_id\":\"evt_malformed_002\",",
  "errors": [
    {
      "code": "MALFORMED_JSON",
      "message": "Event payload is not valid JSON"
    }
  ],
  "failed_at": "2026-09-03T10:30:00Z"
}
```

The malformed event is isolated instead of causing the entire streaming pipeline to stop.

---

## 7. Flink Processing

Apache Flink consumes events from the Kafka input topic and performs validation and transformation.

### Valid Event Processing

For valid events, Flink:

1. Reads the event from Kafka.
2. Parses the JSON payload.
3. Validates the event fields.
4. Performs the required transformation.
5. Calculates `total_value`.
6. Adds `processed_at`.
7. Writes the processed event to Iceberg.

The processed Iceberg records contain:

```text
event_id
event_type
timestamp
order_id
customer_id
product_id
quantity
amount
currency
total_value
processed_at
```

### Invalid Event Processing

For invalid events, Flink:

1. Detects the validation failure.
2. Creates an error record.
3. Preserves the original event.
4. Adds validation error details.
5. Adds the failure timestamp.
6. Sends the record to `checkout-events-dlq`.

---

## 8. Iceberg Storage

Valid processed events are written to the Iceberg table:

```text
iceberg_catalog.checkout.checkout_events
```

The Iceberg table provides persistent lakehouse storage for successfully validated checkout events.

The stored records include both the original event information and processing fields such as `total_value` and `processed_at`.

---

## 9. Day 7 Testing Results

The Day 7 invalid-event and Dead Letter Queue flow was tested using **100 checkout events**.

### Test Summary

| Test                                     | Result |
| ---------------------------------------- | ------ |
| Valid checkout events                    | PASS   |
| Invalid quantity                         | PASS   |
| Missing `customer_id`                    | PASS   |
| Invalid currency                         | PASS   |
| Malformed JSON                           | PASS   |
| Invalid event routing                    | PASS   |
| Original event preservation              | PASS   |
| Validation error preservation            | PASS   |
| Failure timestamp preservation           | PASS   |
| Valid events written to Iceberg          | PASS   |
| Invalid events written to DLQ            | PASS   |
| Processing continues after invalid input | PASS   |

### Day 7 Batch Result

```text
Total events tested     : 100
Valid events            : 90
Invalid events          : 10
Iceberg records         : 90
DLQ records             : 10
```

The 10 invalid Day 7 events were:

```text
day7_010
day7_020
day7_030
day7_040
day7_050
day7_060
day7_070
day7_080
day7_090
day7_100
```

The remaining 90 Day 7 events successfully reached Iceberg.

---

## 10. Invalid Event Examples

The Day 7 tests covered multiple failure scenarios.

### Non-positive quantity

```text
day7_010 → INVALID
Reason: NON_POSITIVE_VALUE
```

### Missing customer ID

```text
day7_040 → INVALID
Reason: REQUIRED_FIELD
```

### Invalid currency

```text
day7_070 → INVALID
Reason: INVALID_CURRENCY
```

### Malformed JSON

```text
evt_malformed_002 → INVALID
Reason: MALFORMED_JSON
```

These tests confirm that different types of data-quality failures are correctly identified and routed to the DLQ.

---

## 11. Dead Letter Queue

The Dead Letter Queue provides a safe location for events that cannot be processed successfully.

The DLQ preserves:

```text
event_id
original_event
errors
failed_at
```

This allows invalid events to be:

* Investigated
* Debugged
* Audited
* Reprocessed later if required

The DLQ prevents bad input from blocking the processing of valid events.

---

## 12. Day 7 Outcome

The Day 7 implementation successfully established and verified the invalid-event handling flow:

```text
Kafka
  ↓
checkout-events
  ↓
Flink
  ↓
Data Quality Validation
  ├── VALID ───────→ Iceberg
  │
  └── INVALID ─────→ checkout-events-dlq
```

The pipeline successfully processed a 100-event test batch containing both valid and invalid records.

**90 valid events** were written to Iceberg, while **10 invalid events** were routed to the DLQ.

Invalid quantity, missing `customer_id`, invalid currency, and malformed JSON scenarios were successfully tested.

The original event, validation errors, and failure timestamp were preserved for invalid records.

This confirms that Ice-Stream can isolate bad data while allowing valid streaming events to continue through the Flink and Iceberg pipeline.
# Day 8 – Data Validation and Dead-Letter Queue

## Objective

Implement data validation in the Flink streaming pipeline and route invalid checkout events to a Dead-Letter Queue (DLQ), while valid events are written to Apache Iceberg.

## Pipeline

Kafka `checkout-events`
→ Flink
→ Data Validation
→ Valid Events → Iceberg
→ Invalid Events → Kafka `checkout-events-dlq`

## Validation Rules

The following fields are validated:

* `event_id` must not be NULL
* `event_type` must not be NULL
* `order_id` must not be NULL
* `customer_id` must not be NULL
* `product_id` must not be NULL
* `quantity` must not be NULL and must be greater than 0
* `amount` must not be NULL and must be greater than or equal to 0
* `currency` must not be NULL

## Valid Event Processing

A valid test event was sent to the Kafka `checkout-events` topic:

* Event ID: `test-valid-001`
* Quantity: `2`
* Amount: `100.50`
* Currency: `INR`

The Flink job was running successfully and the valid-event branch was connected to the Iceberg writer.

A new Iceberg data object was created in MinIO, confirming that streaming data was being written to Iceberg.

## Invalid Event Processing

An invalid test event was sent with:

* Event ID: `test-invalid-001`
* Quantity: `-1`

The event was rejected by the validation rules and successfully written to the Kafka DLQ topic:

`checkout-events-dlq`

The DLQ record contained the validation error:

`Invalid quantity`

## Flink Job Validation

The Flink streaming job was verified through the Flink REST API.

Status:

`RUNNING`

The job plan confirmed two processing branches:

1. Valid records → `IcebergStreamWriter`
2. Invalid records → `checkout_events_dlq`

## Storage Validation

Iceberg data files were verified in MinIO under:

`/data/warehouse/checkout/checkout_events/data`

A newly created Iceberg data object was observed after processing the valid test event.

## Result

Day 8 successfully implemented:

* Kafka event ingestion
* Flink streaming validation
* Valid event processing
* Iceberg storage
* Invalid event detection
* Dead-Letter Queue processing
* Validation error reporting
* MinIO/Iceberg data storage verification

The streaming pipeline is operational and the invalid-event DLQ flow has been successfully validated.
# Day 9 – Windowed Data Quality Metrics

## Objective

Extend the Flink streaming pipeline to calculate continuous data quality metrics using one-minute tumbling windows and persist the results to Apache Iceberg.

## Pipeline

Kafka `checkout-events`
→ Flink
→ Data Validation
→ Valid Events → Iceberg `checkout_events`
→ Invalid Events → Kafka `checkout-events-dlq`
→ One-Minute Quality Metrics → Iceberg `quality_metrics`

## Quality Metrics

For each one-minute processing-time window, the Flink job calculates:

* `window_start` – beginning of the one-minute window
* `window_end` – end of the one-minute window
* `total_events` – total events received in the window
* `valid_events` – events that pass all validation rules
* `invalid_events` – events that fail validation
* `quality_score` – percentage of valid events
* `invalid_event_rate` – percentage of invalid events

The calculations are:

```text
quality_score = valid_events / total_events × 100

invalid_event_rate = invalid_events / total_events × 100
```

For example:

| Total Events | Valid Events | Invalid Events | Quality Score | Invalid Event Rate |
| -----------: | -----------: | -------------: | ------------: | -----------------: |
|          100 |           98 |              2 |           98% |                 2% |
|          100 |           93 |              7 |           93% |                 7% |
|          100 |           85 |             15 |           85% |                15% |

## Windowing

The quality metrics use a one-minute tumbling window based on Flink processing time.

Each event belongs to exactly one one-minute window. This provides continuous monitoring of the quality of the incoming checkout stream without requiring event-time watermarks.

## Metrics Storage

Quality metrics are stored in the Apache Iceberg table:

```text
iceberg_catalog.checkout.quality_metrics
```

The table contains:

```text
window_start
window_end
total_events
valid_events
invalid_events
quality_score
invalid_event_rate
```

This allows downstream monitoring and dashboard components to query historical data quality trends.

## Validation and DLQ Preservation

Invalid events continue to be preserved in the Kafka topic:

```text
checkout-events-dlq
```

The DLQ retains the original event fields together with a `validation_error` describing the validation failure.

Valid events continue to be written to the Iceberg `checkout_events` table.

## Day 9 Verification

The Day 9 pipeline was verified by:

1. Starting the Kafka, MinIO, Iceberg REST, and Flink services.
2. Running the continuous checkout event producer.
3. Sending both valid and intentionally invalid events.
4. Confirming that the Flink validation job remained in the `RUNNING` state.
5. Confirming that invalid events reached `checkout-events-dlq`.
6. Confirming that the `checkout_events` Iceberg table continued receiving Parquet data.
7. Confirming that the `quality_metrics` Iceberg table produced windowed Parquet data.
8. Running the project test suite successfully.

The project test suite completed with:

```text
63 passed, 1 warning
```

The warning was a dependency deprecation warning and did not cause any test failures.
# Day 10 – Kafka and Flink Reliability

## Objective

Improve the reliability of the Ice-Stream streaming pipeline by testing failure and recovery scenarios involving Apache Kafka, Apache Flink, and Apache Iceberg.

The main reliability scenarios include:

* Kafka failure and recovery
* Flink failure and restart
* Producer retry and reconnection
* Invalid event handling through the Dead Letter Queue
* Valid event processing through Iceberg
* Storage failure and recovery
* Duplicate behavior after restart
* Pipeline component health monitoring

---

## 1. Reliability Architecture

The Day 10 reliability flow is:

```text
                    Producer
                       |
                       v
              Kafka: checkout-events
                       |
                       v
                Apache Flink
                       |
              Data Quality Validation
                 /           \
                /             \
               v               v
           VALID             INVALID
             |                  |
             v                  v
         Iceberg          checkout-events-dlq
             |
             v
       quality_metrics
```

The reliability design allows individual components to fail and recover without permanently stopping the entire streaming pipeline.

---

## 2. Infrastructure

The Ice-Stream infrastructure consists of:

| Component            | Purpose                           |
| -------------------- | --------------------------------- |
| Apache Kafka         | Event ingestion and messaging     |
| Apache Flink         | Stream processing and validation  |
| Apache Iceberg       | Lakehouse storage                 |
| Iceberg REST Catalog | Iceberg table metadata management |
| MinIO                | Object storage for Iceberg data   |

The infrastructure is started using:

```powershell
docker compose -f infra/docker-compose.yml up -d
```

The current infrastructure can be checked using:

```powershell
docker compose -f infra/docker-compose.yml ps
```

---

## 3. Kafka Failure and Recovery

Kafka failure recovery was tested as part of the Day 10 reliability testing.

### Test Procedure

Kafka was stopped while the Flink streaming job was running.

```powershell
docker compose -f infra/docker-compose.yml stop kafka
```

After Kafka became unavailable, the Flink job detected the Kafka connection failure.

The Flink job entered the:

```text
RESTARTING
```

state.

Kafka was then started again:

```powershell
docker compose -f infra/docker-compose.yml start kafka
```

After Kafka recovered, the Flink job eventually returned to:

```text
RUNNING
```

### Recovery Flow

```text
Kafka
  |
  | Failure
  v
Kafka unavailable
  |
  v
Flink detects failure
  |
  v
Flink RESTARTING
  |
  v
Kafka restarted
  |
  v
Kafka available
  |
  v
Flink RUNNING
```

### Result

**PASS**

The Flink pipeline detected the Kafka failure and recovered after Kafka was restarted.

---

## 4. Kafka Topic Persistence

After Kafka was restarted, the Kafka topics were verified.

The following topics were available:

```text
checkout-events
checkout-events-dlq
checkout-events-invalid
checkout-events-valid
__consumer_offsets
```

The two primary pipeline topics remained available:

```text
checkout-events
checkout-events-dlq
```

This confirmed that the required Kafka topics were available after the Kafka restart performed during the reliability test.

### Result

**PASS**

---

## 5. Flink Restart and Recovery

Flink restart and recovery behavior was tested as part of the reliability work.

The Flink job was monitored using:

```powershell
docker exec ice-stream-flink-jobmanager flink list
```

The streaming job was observed transitioning between:

```text
RUNNING
```

and:

```text
RESTARTING
```

during failure recovery.

After the dependent services recovered, the existing Flink job returned to:

```text
RUNNING
```

The existing Flink job ID used during the reliability testing was:

```text
90fa0e7b2445a12c359c891019f38fad
```

The job processes:

```text
checkout-events
        |
        v
Flink validation
        |
        +----> checkout_events
        |
        +----> checkout_events_dlq
        |
        +----> quality_metrics
```

### Result

**PASS**

The Flink job was able to recover and return to a running state.

---

## 6. Producer Retry and Reconnection

The checkout event producer already contains retry-related configuration to prevent immediate termination when Kafka is temporarily unavailable.

The producer uses:

* Kafka broker configuration
* Configurable topic
* Maximum retry attempts
* Retry delay
* Continuous event generation

The retry mechanism follows the general flow:

```text
Send event
    |
    v
Success?
  /   \
YES    NO
 |      |
 v      v
Done   Retry
        |
        v
   Retry delay
        |
        v
   Try again
```

The retry mechanism is intentionally bounded so that a Kafka failure does not result in an infinite tight retry loop.

### Producer Reliability

The producer should remain active during temporary Kafka unavailability and attempt to reconnect when Kafka becomes available again.

Detailed producer reconnect verification and logging improvements are part of the remaining Day 10 verification work.

---

## 7. Producer Logging

Reliable logging is important when diagnosing Kafka connectivity problems.

The desired producer behavior is:

```text
[ERROR] Kafka unavailable
[INFO] Retrying connection...
```

After Kafka becomes available:

```text
[INFO] Kafka connection restored
[INFO] Producer resumed
```

These messages make Kafka connectivity and producer recovery easier to monitor.

The producer retry configuration is already present. Logging behavior should be verified during the producer reconnect test.

---

## 8. Invalid Event and DLQ Behavior

The pipeline continues to use:

```text
checkout-events-dlq
```

as the Dead Letter Queue for invalid checkout events.

The validation flow is:

```text
checkout-events
      |
      v
    Flink
      |
      v
Validation
   /     \
  /       \
VALID    INVALID
  |         |
  v         v
Iceberg   DLQ
```

Invalid events can contain validation information such as:

```text
event_id
validation_error
```

The existing validation rules include checks for:

* Required fields
* Valid event type
* Positive quantity
* Non-negative amount
* Valid currency
* Valid identifiers

The DLQ prevents invalid events from stopping the processing of valid events.

Further post-Flink-restart DLQ verification is part of the remaining Day 10 testing.

---

## 9. Valid Event Processing and Iceberg

Valid events continue to be written to:

```text
iceberg_catalog.checkout.checkout_events
```

Quality metrics are stored in:

```text
iceberg_catalog.checkout.quality_metrics
```

The valid-event processing flow is:

```text
Valid checkout event
        |
        v
      Flink
        |
        v
   Validation PASS
        |
        v
     Iceberg
```

Further verification of valid events before and after a Flink restart is part of the remaining Day 10 testing.

---

## 10. Iceberg Storage Failure

During the Kafka/Flink reliability testing, an Iceberg REST catalog storage failure was observed.

The Iceberg REST service reported:

```text
org.sqlite.SQLiteException:
[SQLITE_BUSY] The database file is locked
```

This caused an Iceberg commit operation to fail.

Flink consequently reported:

```text
CommitStateUnknownException
```

for the:

```text
quality_metrics
```

Iceberg sink.

This demonstrated an additional storage-layer failure scenario.

---

## 11. Iceberg Recovery

The Iceberg REST service was restarted without deleting the Iceberg catalog data or recreating the tables.

The service was restarted using:

```powershell
docker restart ice-stream-iceberg-rest
```

After the restart, the Iceberg REST logs showed:

```text
Successfully committed to table checkout.quality_metrics
```

The service also successfully loaded the Iceberg table metadata.

The quality metrics table was refreshed to a new metadata version:

```text
quality_metrics/metadata/00001-d1dc4c29-45e2-49bc-a11b-c58ba16d4010.metadata.json
```

and subsequently:

```text
quality_metrics/metadata/00002-34c07e37-8f6d-4ab1-96b0-579345ee7057.metadata.json
```

After Iceberg REST recovered, the existing Flink job returned to:

```text
RUNNING
```

### Recovery Flow

```text
Iceberg REST
     |
     v
SQLite database lock
     |
     v
Commit failure
     |
     v
Flink restart/recovery
     |
     v
Iceberg REST restarted
     |
     v
Catalog available
     |
     v
quality_metrics commit succeeds
     |
     v
Flink RUNNING
```

### Result

**PASS — Recovery verified**

This test demonstrated that the Iceberg REST service could be restarted without deleting the catalog or recreating the streaming job.

---

## 12. Duplicate Behavior

Streaming systems can potentially process an event again when a failure occurs between processing and committing a result.

The Day 10 reliability testing therefore includes checking whether Flink recovery creates obvious duplicate records.

The current architecture uses Flink checkpoints and Iceberg commits for recovery.

Full end-to-end exactly-once behavior has not been formally established as part of this task.

Therefore, duplicate behavior should be monitored and documented based on the results of the final restart test.

---

## 13. Pipeline Health

A simple component health model is used to represent the state of the streaming pipeline.

Example:

```json
{
  "kafka": "UP",
  "flink": "UP",
  "iceberg": "UP",
  "producer": "UP"
}
```

The main pipeline components are:

```text
Kafka       → UP
Flink       → UP
Iceberg     → UP
Producer    → UP
```

This health concept can later be consumed by the monitoring and dashboard components developed by other team members.

---

## 14. Current Recovery Behavior

The Day 10 testing demonstrated the following recovery behavior:

### Kafka failure

```text
Kafka fails
   ↓
Flink detects failure
   ↓
Flink enters RESTARTING
   ↓
Kafka recovers
   ↓
Flink returns to RUNNING
```

### Iceberg REST failure

```text
Iceberg catalog failure
   ↓
Commit failure
   ↓
Flink enters recovery
   ↓
Iceberg REST restarted
   ↓
Catalog becomes available
   ↓
Commit succeeds
   ↓
Flink returns to RUNNING
```

This demonstrates that the pipeline has recovery mechanisms at multiple infrastructure layers.

---

## 15. Day 10 Testing Results

| Test                                     | Result      |
| ---------------------------------------- | ----------- |
| Infrastructure startup                   | PASS        |
| Kafka failure detection                  | PASS        |
| Kafka recovery                           | PASS        |
| Kafka topics available after restart     | PASS        |
| Flink recovery after Kafka failure       | PASS        |
| Flink restart/recovery                   | PASS        |
| Iceberg REST failure detected            | PASS        |
| Iceberg REST recovery                    | PASS        |
| Quality metrics commit after recovery    | PASS        |
| Producer retry configuration             | IMPLEMENTED |
| Producer reconnect verification          | PENDING     |
| Producer recovery logging verification   | PENDING     |
| DLQ verification after Flink restart     | PENDING     |
| Iceberg verification after Flink restart | PENDING     |
| Duplicate behavior check                 | PENDING     |
| Pipeline health concept                  | DOCUMENTED  |

---

## 16. Day 10 Outcome

Day 10 improved the reliability and recovery behavior of the Ice-Stream streaming pipeline.

The Kafka failure test demonstrated that Flink detects Kafka unavailability and enters a recovery state. After Kafka was restored, the Flink job returned to the `RUNNING` state.

Kafka topics including `checkout-events` and `checkout-events-dlq` were available after Kafka recovery.

An additional Iceberg REST failure was observed during the testing process. The Iceberg catalog reported a SQLite database lock, which caused an `Iceberg` commit to enter an unknown state.

The Iceberg REST service was restarted without deleting the catalog or recreating the Iceberg tables. After recovery, the `quality_metrics` table successfully committed new metadata and the existing Flink job returned to the `RUNNING` state.

The Day 10 work therefore demonstrates recovery from both **Kafka availability failures** and an **Iceberg catalog/storage failure** while preserving the existing streaming pipeline state.

Remaining producer reconnect, post-restart DLQ/Iceberg verification, and duplicate-behavior checks should be completed before marking the entire Day 10 reliability checklist as fully complete.
