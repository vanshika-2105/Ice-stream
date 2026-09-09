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
