\# ICE-STREAM Backend



The ICE-STREAM backend provides REST APIs and WebSocket

communication for real-time observability.



The backend is responsible for:



\- Data quality metrics

\- Data validation

\- Data profiling

\- Anomaly detection

\- Pipeline metrics

\- System health

\- Alert generation

\- Retry and circuit-breaker resilience

\- Observability overview

\- Live metrics

\- WebSocket events



\## Architecture



The backend is implemented using FastAPI.



The main backend entry point is:



```text

alert\_server/main.py

