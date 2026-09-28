# Backend Reliability

## Overview

Day 28 focuses on making the Ice-Stream Alert Server stable and safe under failures.

The backend reliability work covers API error handling, alert lifecycle management, WebSocket reliability, health checks, security behavior, observability, and regression testing.

## API Error Handling

The backend returns controlled responses for invalid requests.

Examples:

- `400 Bad Request` — invalid request parameters or invalid component status.
- `404 Not Found` — unknown endpoints or alert IDs.
- `422 Unprocessable Entity` — malformed JSON or request validation errors.
- `500 Internal Server Error` — unexpected server-side failures.

Internal stack traces should not be exposed to API clients.

## Alert Lifecycle

Alerts follow the lifecycle:

```text
ACTIVE
   ↓
ACKNOWLEDGED
   ↓
RESOLVED