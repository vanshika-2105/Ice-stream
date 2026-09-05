# Ice-Stream Streaming Pipeline

## 1. Overview

Ice-Stream is a real-time streaming data pipeline that processes checkout events using **Apache Kafka, a Data Quality Engine, Apache Flink, and Apache Iceberg**.

### Pipeline Flow

```text
Producer
   ↓
Kafka: checkout-events
   ↓
Quality Engine
   ├── VALID → checkout-events-valid → Flink → Iceberg
   └── INVALID → checkout-events-invalid
```

---

## 2. Kafka Topics

| Topic                     | Purpose                                    |
| ------------------------- | ------------------------------------------ |
| `checkout-events`         | Receives raw checkout events               |
| `checkout-events-valid`   | Stores events that pass quality validation |
| `checkout-events-invalid` | Stores invalid or malformed events         |

---

## 3. Event Schema

Checkout events contain the following fields:

* `event_id`
* `event_type`
* `timestamp`
* `order_id`
* `customer_id`
* `product_id`
* `quantity`
* `amount`
* `currency`

---

## 4. Data Quality Validation

The Quality Engine validates incoming events before they reach Flink.

### Validation Rules

* Required fields must be present.
* `event_type` must be `checkout`.
* `quantity` must be greater than 0.
* `amount` cannot be negative.
* Currency must be `INR`, `USD`, `EUR`, or `GBP`.
* Timestamp must be valid ISO-8601 format.
* Malformed JSON is rejected.

Valid events are sent to `checkout-events-valid`.

Invalid events are sent to `checkout-events-invalid` with:

* `quality_status`
* `quality_errors`

---

## 5. Error Handling

Malformed JSON is handled using exception handling in the Quality Engine.

Example log:

```text
[ERROR] Failed to parse event at partition 0, offset 1073
```

The malformed event is routed to the invalid topic with:

```json
{
  "event_id": null,
  "quality_status": "INVALID",
  "quality_errors": [
    {
      "field": "event",
      "code": "MALFORMED_JSON",
      "message": "Event payload is not valid JSON"
    }
  ]
}
```

The Quality Engine continues processing after the error instead of crashing.

---

## 6. Testing Results

The following tests were successfully completed:

* Valid checkout events → **PASS**
* Missing `customer_id` → **PASS**
* Negative `amount` → **PASS**
* Invalid currency → **PASS**
* Malformed JSON → **PASS**
* Invalid event routing → **PASS**
* Error logging → **PASS**
* Processing continues after malformed event → **PASS**
* Valid event after malformed event → **PASS**
* Valid event reached `checkout-events-valid` → **PASS**

Example:

```text
[ERROR] Failed to parse event ...
[INFO] Received event evt_after_malformed
[INFO] Event evt_after_malformed → VALID
```

This confirms that the pipeline can recover from malformed input and continue processing subsequent valid events.

---

## 7. Flink and Iceberg

Flink consumes events from:

```text
checkout-events-valid
```

The processed events are written to the Iceberg table:

```text
iceberg_catalog.checkout.checkout_events
```

The Flink job was successfully submitted and verified as **RUNNING**.

---

## 8. Day 6 Outcome

The Day 6 implementation successfully establishes a quality-controlled streaming pipeline:

**Kafka → Quality Engine → Flink → Iceberg**

Invalid and malformed events are isolated in a separate Kafka topic, while valid events continue through Flink into Iceberg storage.
