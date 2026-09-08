\# Ice-Stream Alert Server API



\## Overview



FastAPI backend for data-quality monitoring, system health monitoring,

alert history, and real-time WebSocket alerts.



\## Base URL



http://127.0.0.1:8000



\## WebSocket



ws://127.0.0.1:8000/ws/alerts



\---



\## Health



\### GET /health



Liveness check for the alert server.



\### GET /health/ready



Readiness check for the backend.



\---



\## Quality Status



\### GET /quality/status



Returns the current quality state and metrics.



Quality thresholds:



| Quality Score | Status |

|---|---|

| >= 95 | HEALTHY |

| >= 90 | WARNING |

| < 90 | CRITICAL |



\---



\## Alert History



\### GET /alerts



Returns recent alert history.



\---



\## System Status



\### GET /system/status



Returns the current infrastructure component status.



Example:



```json

{

&#x20; "system\_status": "DEGRADED",

&#x20; "components": {

&#x20;   "kafka": "DOWN",

&#x20;   "flink": "UP",

&#x20;   "iceberg": "UP",

&#x20;   "alert\_server": "UP"

&#x20; }

}
