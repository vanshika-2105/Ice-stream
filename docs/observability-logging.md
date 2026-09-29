\# Backend Observability Logging



\## Overview



Ice-Stream uses centralized backend logging to track API activity,

data-quality processing, pipeline performance, anomaly detection,

alerts, recovery events, WebSocket activity, and observability operations.



The logging flow is:



API Request

&#x20;   ↓

Correlation ID

&#x20;   ↓

Backend Processing

&#x20;   ↓

Quality / Pipeline / Anomaly / Alerts

&#x20;   ↓

Structured Observability Logs

&#x20;   ↓

Observability Events

&#x20;   ↓

Dashboard



\---



\## Central Logging



Centralized logging is configured in:



`alert\_server/logging\_config.py`



The application logger is:



`ice\_stream`



The logger writes events to:



1\. Console output

2\. An in-memory bounded observability event store



The event store keeps a maximum of 500 events.



Older events are automatically removed when the limit is reached.



\---



\## Log Levels



The backend supports the standard logging levels:



| Level | Purpose |

|---|---|

| DEBUG | Detailed diagnostic information |

| INFO | Normal application activity |

| WARNING | Degraded or potentially problematic conditions |

| ERROR | Application errors |

| CRITICAL | Severe system conditions |



Examples:



\- Normal API processing → `INFO`

\- Quality degradation → `WARNING`

\- Pipeline degradation → `WARNING`

\- Unexpected exceptions → `ERROR`

\- Severe infrastructure conditions → `CRITICAL`



\---



\## Correlation IDs



Every HTTP request receives a correlation ID.



The client can provide:



`X-Request-ID`



Example:



```text

X-Request-ID: day27-observability-test

