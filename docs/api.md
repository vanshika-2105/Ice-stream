# Ice-Stream Alert Server API

## Overview

FastAPI backend for data-quality monitoring, system health monitoring,
historical quality metrics, alert history, and real-time WebSocket alerts.

## Base URL

http://127.0.0.1:8000

## WebSocket

ws://127.0.0.1:8000/ws/alerts

WebSocket clients can receive the following message types:

- `QUALITY_METRICS`
- `QUALITY_ALERT`
- `QUALITY_RECOVERY`
- `SYSTEM_ALERT`
- `SYSTEM_RECOVERY`

---

## Health

### GET /health

Liveness check for the alert server.

Example response:

```json
{
  "status": "ok",
  "service": "alert-server",
  "timestamp": "2026-09-09T10:00:00+00:00"
}