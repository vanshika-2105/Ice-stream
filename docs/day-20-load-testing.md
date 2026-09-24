# Day 20 — Controlled Load Testing & Recovery Verification

## Goal

Verify the Ice-Stream pipeline under controlled producer load and confirm that Kafka and Flink remain available and that the pipeline recovers successfully after load testing.

---

## 1. Load Test Results

The producer was tested at controlled target rates of 5, 10, 25, 50, and 100 events per second.

| Target Rate | Events Sent | Failed | Duration | Actual Throughput | Expected Quality |
| ----------- | ----------: | -----: | -------: | ----------------: | ---------------: |
| 5 eps       |         420 |      0 |  96.64 s |          4.35 eps |           98.10% |
| 10 eps      |         509 |      0 |  53.41 s |          9.53 eps |           98.04% |
| 25 eps      |       1,372 |      0 |  60.74 s |         22.59 eps |           98.03% |
| 50 eps      |       2,378 |      0 |  57.85 s |         41.11 eps |           98.02% |
| 100 eps     |       4,490 |      0 |  62.39 s |         71.96 eps |           98.02% |

All recorded load tests completed with **0 producer failures**.

---

## 2. Kafka and Flink Behavior

After the load tests, the infrastructure was checked using Docker Compose.

The following services were running:

* Kafka — running
* Flink JobManager — running
* Flink TaskManager — running
* Iceberg REST — running
* MinIO — running

The Kafka and Flink services remained available after the controlled load tests.

---

## 3. Docker Resource Monitoring

Docker resource usage was recorded after the load testing.

| Container         |   CPU |    Memory |
| ----------------- | ----: | --------: |
| Kafka             | 1.73% | 868.9 MiB |
| Flink TaskManager | 1.60% | 379.7 MiB |
| Flink JobManager  | 0.97% | 315.8 MiB |
| Iceberg REST      | 0.13% | 87.39 MiB |
| MinIO             | 0.03% | 79.98 MiB |

---

## 4. Recovery Verification

After the load tests, the producer was started again using the 100 eps configuration.

Recovery test results:

* Events attempted: 566
* Events sent: 566
* Events failed: 0
* Actual throughput: 71.42 events/sec
* Expected quality: 98.06%
* Expected invalid rate: 1.94%

The producer successfully resumed sending events after the load testing, with **566/566 events sent successfully**.

---

## 5. Completion Checklist

* [x] Establish baseline
* [x] Test 5 eps
* [x] Test 10 eps
* [x] Test 25 eps
* [x] Test 50 eps
* [x] Test 100 eps
* [x] Record Kafka/Flink behavior
* [x] Record Docker resource usage
* [x] Verify recovery after load

## Conclusion

Day 20 controlled load testing and recovery verification were completed successfully. The producer completed all controlled load tests without producer-side failures, Kafka and Flink remained running, Docker resource usage was recorded, and the producer successfully resumed event transmission during the recovery verification.
