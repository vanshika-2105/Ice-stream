# Backend Deployment Guide

## Overview

The Ice-Stream alert backend is a FastAPI application served by Uvicorn.

Application entry point:

    alert_server.main:app

The backend provides HTTP APIs, health checks, and a WebSocket alert stream.

## Configuration

Backend configuration is centralized in:

    alert_server/config.py

Supported environment variables:

- APP_ENV - application environment
- HOST - server bind address
- PORT - server port
- CORS_ORIGINS - comma-separated allowed frontend origins

Example configuration is provided in:

    .env.example

The real .env file is ignored by Git and must not contain committed secrets.

## Local Development

Run:

    uvicorn alert_server.main:app --host 0.0.0.0 --port 8000 --reload

Development configuration:

    APP_ENV=development
    HOST=0.0.0.0
    PORT=8000
    CORS_ORIGINS=http://localhost:5173

## Production Startup

Production startup should not use the --reload option.

Run:

    uvicorn alert_server.main:app --host 0.0.0.0 --port 8000

The application listens on all interfaces so it can be reached from a container or deployment environment.

## Health Check

The existing health endpoint is:

    GET /health

Example:

    Invoke-RestMethod http://localhost:8000/health

Expected response includes:

    status: ok
    service: alert-server

## WebSocket

The alert WebSocket endpoint is:

    /ws/alerts

Local browser or client URL:

    ws://localhost:8000/ws/alerts

When running inside Docker, the browser should normally continue using the host address and published port.

The Docker service name backend is intended for communication between containers, not for a browser running on the host machine.

## CORS

CORS origins are controlled through:

    CORS_ORIGINS

The default development origin is:

    http://localhost:5173

Production deployments should set this variable to the actual frontend origin instead of using a wildcard.

## Docker

The backend Dockerfile is:

    alert_server/Dockerfile

The Docker Compose configuration is:

    docker-compose.yml

The Compose configuration publishes:

    localhost:8000 -> backend:8000

The backend container also has a health check using:

    /health

Docker was not available in the development environment during this deployment work, so Docker image build and container runtime verification could not be performed locally.

## Verification

The backend was verified locally with:

- Full pytest suite: 117 tests passed
- Uvicorn startup: successful
- HTTP health check: successful
- WebSocket connection: successful

## Security Notes

- .env is excluded from Git.
- .env.example contains configuration examples only.
- Production CORS origins should be explicitly configured.
- Production startup must not use Uvicorn --reload.
- Secrets should be supplied through the deployment environment rather than committed to the repository.
