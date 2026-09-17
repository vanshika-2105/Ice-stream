# Day 14 — Data Profiling & Quality Scenario Testing

## Objective

The objective of Day 14 was to test the Ice-Stream real-time data quality pipeline using multiple controlled data-quality scenarios.

The producer was extended to generate different types of invalid checkout events while keeping the existing Kafka → Flink → Quality Decision → Iceberg/DLQ architecture unchanged.

The testing focused on:

* Missing required fields
* Invalid quantities
* Invalid amounts
* Invalid currencies
* Mixed data-quality errors
* Flink stability during invalid-event processing
* Iceberg storage and historical quality metrics

---

## Data Quality Scenarios

The producer supports the following scenarios:

### 1. NORMAL

Generates mostly valid checkout events with a small percentage of invalid events.

Expected result:

* Total events: 100
* Valid events: 98
* Invalid events: 2
* Quality score: 98%
* Invalid event rate: 2%

The invalid events were generated using an unsupported currency.

---

### 2. MISSING_FIELDS

Tests missing required fields.

The producer cycles through:

* Missing `event_id`
* Missing `quantity`
* Missing `amount`
* Missing `currency`

Results:

* Total events: 100
* Valid events: 0
* Invalid events: 100
* Quality score: 0%
* Invalid event rate: 100%

Each missing-field category occurred 25 times.

---

### 3. BAD_QUANTITY

Tests invalid quantity values.

The producer generates:

* `0`
* `-5`
* `"ten"`

Results:

* Total events: 100
* Valid events: 0
* Invalid events: 100
* Quality score: 0%
* Invalid event rate: 100%

---

### 4. BAD_AMOUNT

Tests invalid amount values.

The producer generates:

* `-10`
* `"abc"`

Results:

* Total events: 100
* Valid events: 0
* Invalid events: 100
* Quality score: 0%
* Invalid event rate: 100%

---

### 5. BAD_CURRENCY

Tests unsupported currency values.

The producer generates:

* `XYZ`

Results:

* Total events: 100
* Valid events: 0
* Invalid events: 100
* Quality score: 0%
* Invalid event rate: 100%

---

### 6. MIXED_ERRORS

The final profiling scenario combines several different data-quality problems in a single stream.

The scenario was configured for 100 events.

Every fourth event was intentionally made invalid while the remaining events were valid.

Results:

| Metric             | Result |
| ------------------ | -----: |
| Total events       |    100 |
| Valid events       |     75 |
| Invalid events     |     25 |
| Quality score      |    75% |
| Invalid event rate |    25% |

The invalid events included:

* Missing `event_id`
* Negative quantity
* Zero quantity
* String quantity
* Negative amount
* String amount
* Missing `quantity`
* Missing `amount`
* Missing `currency`
* Invalid currency
* Invalid timestamp

This scenario demonstrated that the validation layer can identify different error types within the same streaming workload.

---

## Pipeline Verification

The existing architecture was retained:

```text
Kafka
  |
  v
checkout-events
  |
  v
Flink Validation
  |
  +--------------------+
  |                    |
  v                    v
Valid Events          Invalid Events
  |                    |
  v                    v
Iceberg              DLQ
  |
  v
Quality Metrics
```

No changes were made to the DLQ behavior. Invalid events continue to be separated from valid events by the Flink quality-processing pipeline.

---

## Flink Stability Issue and Resolution

During Day 14 testing, the Flink job entered a `RESTARTING` state.

The root cause was identified in the Iceberg REST catalog logs as a SQLite database locking error:

```text
SQLITE_BUSY: The database file is locked
```

The Iceberg REST catalog was originally using an in-memory SQLite catalog.

To improve catalog persistence and reduce the locking/restart issue, the catalog configuration was changed to use a persistent SQLite database:

```text
CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
CATALOG_URI=jdbc:sqlite:/tmp/iceberg_rest.db
CATALOG_JDBC_USER=user
CATALOG_JDBC_PASSWORD=password
```

The Flink job was then cancelled and resubmitted.

New Flink job:

```text
2b64a014c373329fccceca3c54d54d83
```

The new job reached and remained in:

```text
RUNNING
```

---

## Iceberg Catalog Verification

After restarting the Iceberg REST service, the catalog was initially empty because the previous catalog used an in-memory SQLite database.

The Flink job contains `CREATE DATABASE IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` statements, so it recreated the required Iceberg objects.

The following namespace was successfully recreated:

```text
checkout
```

The following tables were successfully recreated:

```text
checkout_events
quality_metrics
```

The Iceberg REST API returned HTTP 200 responses when checking the namespace and tables.

---

## Final Verification Status

| Component               | Status    |
| ----------------------- | --------- |
| Kafka                   | Working   |
| Producer scenarios      | Tested    |
| NORMAL                  | Completed |
| MISSING_FIELDS          | Completed |
| BAD_QUANTITY            | Completed |
| BAD_AMOUNT              | Completed |
| BAD_CURRENCY            | Completed |
| MIXED_ERRORS            | Completed |
| Flink validation        | Working   |
| DLQ processing          | Preserved |
| Iceberg REST            | Working   |
| `checkout` namespace    | Recreated |
| `checkout_events` table | Recreated |
| `quality_metrics` table | Recreated |
| Flink job               | RUNNING   |

The final MIXED_ERRORS scenario successfully produced **75 valid and 25 invalid events**, demonstrating the intended data-quality profiling behavior.

---

## Day 14 Outcome

Day 14 successfully added controlled data-quality profiling scenarios to the streaming producer.

The pipeline was tested against both individual and mixed error conditions. The final mixed scenario produced the expected 75% quality score, and the Flink pipeline remained operational after resolving the Iceberg REST catalog database-locking issue.

The implementation is ready for the next stage of the Ice-Stream project.
