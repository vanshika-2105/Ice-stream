# Ice-Stream Controlled Failure Scenarios

## Purpose

This document defines controlled and repeatable failure scenarios for testing the Ice-Stream streaming pipeline.

The scenarios covered are:

- NORMAL_STREAM
- PRODUCER_STOP
- KAFKA_FAILURE
- FLINK_FAILURE

---

## 1. NORMAL_STREAM

### Description

The complete streaming pipeline is running normally.

### Expected behavior

- Kafka is running.
- Flink JobManager and TaskManager are running.
- Producer sends checkout events.
- Events are processed by Flink.
- Valid and invalid events are handled according to the quality rules.

### Test status

**Completed successfully.**

---

## 2. PRODUCER_STOP

### Description

The producer is intentionally stopped while the streaming infrastructure remains running.

### Test procedure

1. Start the Ice-Stream infrastructure.
2. Start the producer.
3. Confirm events are being sent.
4. Stop the producer with `Ctrl+C`.
5. Confirm that the producer has stopped sending events.

### Observed result

The producer was stopped successfully.

First test:

- Events attempted: 102
- Events sent: 102
- Events failed: 0
- Actual throughput: 20.79 events/sec
- Expected valid: 77
- Expected invalid: 25
- Expected quality: 75.49%
- Expected invalid rate: 24.51%

Second test:

- Events attempted: 47
- Events sent: 47
- Events failed: 0
- Actual throughput: 35.46 events/sec
- Events failed: 0
- Expected valid: 36
- Expected invalid: 11
- Expected quality: 76.60%
- Expected invalid rate: 23.40%

The producer was stopped using `Ctrl+C`.

### Test status

**Completed successfully.**

---

## 3. KAFKA_FAILURE

### Description

Kafka is intentionally stopped while the other infrastructure components remain running.

### Test procedure

1. Stop Kafka:

`docker stop ice-stream-kafka`

2. Check the running containers:

`docker ps`

3. Inspect Kafka logs:

`docker logs --tail 100 ice-stream-kafka`

4. Restart Kafka:

`docker start ice-stream-kafka`

5. Check the running containers again:

`docker ps`

### Observed result

Kafka was intentionally stopped.

After the failure:

- Kafka was absent from `docker ps`.
- Flink JobManager remained running.
- Flink TaskManager remained running.
- Iceberg REST remained running.
- MinIO remained running.

Kafka logs showed a controlled shutdown, including:

- ReplicaManager shutdown
- BrokerServer shutdown
- Broker transition to SHUTDOWN

Kafka was then restarted successfully.

After restart, all five infrastructure containers were running again.

### Test status

**Completed successfully.**

---

## 4. FLINK_FAILURE

### Description

The Flink TaskManager is intentionally stopped while the other infrastructure components remain running.

### Test procedure

1. Stop the Flink TaskManager:

`docker stop ice-stream-flink-taskmanager`

2. Check the running containers:

`docker ps`

3. Inspect TaskManager logs:

`docker logs --tail 100 ice-stream-flink-taskmanager`

4. Restart the TaskManager:

`docker start ice-stream-flink-taskmanager`

5. Check the running containers again:

`docker ps`

### Observed result

The Flink TaskManager was intentionally stopped.

After the failure:

- Flink TaskManager was absent from `docker ps`.
- Flink JobManager remained running.
- Kafka remained running.
- Iceberg REST remained running.
- MinIO remained running.

The TaskManager logs showed a controlled shutdown:

`RECEIVED SIGNAL 15: SIGTERM. Shutting down as requested.`

`Stopping TaskExecutor`

The TaskManager was then restarted successfully.

### Post-recovery verification

After the Flink TaskManager was restarted, the existing producer successfully sent events:

- Events attempted: 48
- Events sent: 48
- Events failed: 0
- Actual throughput: 35.86 events/sec
- Expected valid: 36
- Expected invalid: 12
- Expected quality: 75%
- Expected invalid rate: 25%

This confirmed that event production could continue after Flink TaskManager recovery.

### Test status

**Completed successfully.**

---

## Summary

| Scenario | Tested | Recovery/Continuation Verified |
|---|---|---|
| NORMAL_STREAM | Yes | Yes |
| PRODUCER_STOP | Yes | Yes |
| KAFKA_FAILURE | Yes | Yes |
| FLINK_FAILURE | Yes | Yes |

---

## Conclusion

The Ice-Stream controlled failure scenarios were tested using repeatable manual failure procedures.

The producer was intentionally stopped, Kafka was intentionally stopped and restarted, and the Flink TaskManager was intentionally stopped and restarted.

After infrastructure recovery, the existing producer successfully sent events again, confirming that the streaming pipeline could continue accepting events after component recovery.
