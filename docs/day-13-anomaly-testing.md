\# Day 13 — Anomaly Scenario Testing



\## Objective



Day 13 focuses on testing the Ice-Stream data-quality pipeline against controlled anomaly scenarios and verifying that changes in event quality can be observed through the streaming pipeline.



The producer supports four scenarios:



\* `NORMAL`

\* `SUDDEN\_DROP`

\* `GRADUAL\_DEGRADATION`

\* `RECOVERY`



The scenario window size is 60 events.



\## Scenario Configuration



\### NORMAL



Expected invalid-event rate: approximately 3%.



\### SUDDEN\_DROP



Expected quality pattern:



| Window | Expected Quality | Expected Invalid Rate |

| ------ | ---------------: | --------------------: |

| 1      |              96% |                    4% |

| 2      |              96% |                    4% |

| 3      |              96% |                    4% |

| 4      |              96% |                    4% |

| 5      |              82% |                   18% |



\### GRADUAL\_DEGRADATION



Expected quality decreases progressively:



98% → 97% → 95% → 93% → 91% → 89%.



\### RECOVERY



Expected quality improves progressively:



82% → 87% → 91% → 95% → 97%.



\## SUDDEN\_DROP Test Execution



The `SUDDEN\_DROP` scenario was executed using 300 events.



Windows 1–4 were generated normally.



Window 5 covered events 241–300. Because the producer was stopped during the initial run after event 268, the remaining events 269–300 were generated in a continuation run.



Therefore:



\* First part of Window 5: events 241–268

\* Continuation: events 269–300

\* Complete Window 5: events 241–300

\* Total events in Window 5: 60



\## Observed Window 5 Results



The first 28 events of Window 5 produced:



\* 5 invalid events

\* 23 valid events



The continuation from events 269–300 produced:



\* 9 invalid events

\* 23 valid events



Combined Window 5:



| Metric                | Result |

| --------------------- | -----: |

| Total events          |     60 |

| Valid events          |     46 |

| Invalid events        |     14 |

| Observed quality      | 76.67% |

| Observed invalid rate | 23.33% |



The observed invalid rate differs from the configured 18% target because invalid events are randomly generated according to the scenario rate. The complete window therefore demonstrates the intended sudden quality degradation while allowing normal random variation.



\## Invalid Event Types Observed



During the Window 5 execution, invalid events included:



\* `negative\_amount`

\* `missing\_customer`

\* `invalid\_currency`

\* `zero\_quantity`



This confirms that the anomaly producer continues to generate the same validation failure categories used by the quality rules.



\## Flink Processing



The Flink job was submitted using:



```powershell

docker exec ice-stream-flink-jobmanager flink run -py /opt/flink/jobs/checkout\_job.py

```



A fresh job was submitted after the earlier failed Flink execution.



Job ID:



```text

9b9b195d747e4ac39b1877ba1de87da5

```



The job initially reported:



```text

RUNNING

```



The pipeline targets:



\* `iceberg\_catalog.checkout.checkout\_events`

\* `default\_catalog.default\_database.checkout\_events\_dlq`

\* `iceberg\_catalog.checkout.quality\_metrics`



\## Infrastructure Issue Encountered



During processing, Iceberg REST returned HTTP 500 errors caused by SQLite database locking.



The underlying exception was:



```text

org.sqlite.SQLiteException:

\[SQLITE\_BUSY] The database file is locked

```



This caused:



```text

org.apache.iceberg.exceptions.CommitStateUnknownException:

Service failed: 500: Unknown failure

```



The failure occurred while Iceberg was committing data to the catalog.



\## Recovery Attempt



The Iceberg REST container was restarted:



```powershell

docker restart ice-stream-iceberg-rest

```



The REST service successfully restarted and began listening on port 8181.



The failed Flink job was then cancelled and a fresh Flink job was submitted.



The new job initially reached:



```text

RUNNING

```



However, subsequent TaskManager logs showed another Iceberg commit failure involving:



```text

iceberg\_catalog.checkout.checkout\_events

```



and cancellation of the corresponding:



```text

iceberg\_catalog.checkout.quality\_metrics

```



writer.



Therefore, the SQLite/Iceberg catalog locking issue remains an unresolved infrastructure issue.



\## Current Status



\### Completed



\* Anomaly scenario configuration implemented.

\* `SUDDEN\_DROP` scenario executed.

\* Events 001–300 generated.

\* Window 5 completed across events 241–300.

\* Random invalid-event types observed.

\* Flink anomaly-processing job submitted.

\* Iceberg REST locking failure identified from logs.

\* Recovery attempt performed by restarting Iceberg REST.

\* Failed Flink execution cancelled and fresh execution submitted.



\### Outstanding



\* Reliable Iceberg commits for `checkout\_events` and `quality\_metrics`.

\* Final verification of persisted historical quality metrics.

\* Dashboard verification using the persisted anomaly metrics.



\## Conclusion



Day 13 successfully exercised the anomaly-generation portion of the Ice-Stream pipeline and produced a complete sudden-drop window.



The observed Window 5 quality was 76.67%, compared with the configured target quality of approximately 82%. The difference is attributable to random event generation.



The remaining blocker is Iceberg REST catalog persistence: SQLite database locking is causing HTTP 500 commit failures and subsequent Flink task failures. This issue should be resolved before considering the complete Day 13 pipeline verification finished.



