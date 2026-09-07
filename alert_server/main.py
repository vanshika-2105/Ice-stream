from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import sys
from pathlib import Path

# Add quality-rules to Python path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "quality-rules")
)

from alert_server.alert_manager import AlertManager
from alerts import AlertEngine
from metrics import QualityMetrics
from validator import validate_checkout_event


app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.3.0",
)

alert_manager = AlertManager()
quality_metrics = QualityMetrics()
alert_engine = AlertEngine()
# In-memory alert history.
# This stores state-transition alerts and recovery events.
alert_history = []

MAX_ALERT_HISTORY = 100

@app.get("/health")
def health_check():
    """Check whether the backend service is running."""
    return {
        "status": "healthy",
        "service": "alert-server"
    }


@app.get("/")
def root():
    return {
        "message": "Ice-Stream Alert Server is running"
    }


@app.get("/metrics")
def get_metrics():
    """Return current data-quality metrics with current status."""

    metrics = quality_metrics.get_metrics()

    return {
        **metrics,
        "status": alert_engine.get_status(metrics["quality_score"]),
    }


@app.get("/alerts")
def get_alerts():
    """Return recent quality alert history."""

    return {
        "alerts": alert_history
    }
@app.get("/quality/status")
def get_quality_status():
    """Return the current data-quality state and metrics."""

    metrics = quality_metrics.get_metrics()

    return {
        "status": alert_engine.get_status(metrics["quality_score"]),
        "quality_score": metrics["quality_score"],
        "total_events": metrics["total_events"],
        "valid_events": metrics["valid_events"],
        "invalid_events": metrics["invalid_events"],
        "invalid_event_rate": metrics["invalid_event_rate"],
    }


@app.post("/events")
async def process_event(event: dict):
    """
    Validate an event, update metrics, broadcast metrics,
    and generate state-transition alerts.
    """

    result = validate_checkout_event(event)

    if result["valid"]:
        quality_metrics.record_valid()
    else:
        quality_metrics.record_invalid(result["errors"])

    metrics = quality_metrics.get_metrics()

    status = alert_engine.get_status(metrics["quality_score"])

    # Broadcast current metrics.
    metrics_message = {
        "type": "QUALITY_METRICS",
        "total_events": metrics["total_events"],
        "valid_events": metrics["valid_events"],
        "invalid_events": metrics["invalid_events"],
        "quality_score": metrics["quality_score"],
        "invalid_event_rate": metrics["invalid_event_rate"],
        "error_counts": metrics["error_counts"],
        "status": status,
    }

    await alert_manager.broadcast(metrics_message)

    # Let AlertEngine decide whether a state-transition
    # alert/recovery event should be generated.
    alert = alert_engine.evaluate(
        quality_score=metrics["quality_score"],
        invalid_event_rate=metrics["invalid_event_rate"],
    )

    if alert is not None:
        alert_data = alert.to_dict()

        # Add a unique history ID.
        alert_data["id"] = f"alert-{len(alert_history) + 1:03d}"

        # Store newest alert.
        alert_history.append(alert_data)

        # Keep only the most recent alerts.
        if len(alert_history) > MAX_ALERT_HISTORY:
            alert_history.pop(0)

        # Broadcast the same alert to WebSocket clients.
        await alert_manager.broadcast(alert_data)  

    return {
        **result,
        "status": status,
    }


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """WebSocket endpoint for real-time data-quality messages."""

    await alert_manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)

    except Exception:
        alert_manager.disconnect(websocket)


@app.post("/alerts/test")
async def send_test_alert():
    """Send a test alert to connected WebSocket clients."""

    alert = {
        "type": "QUALITY_ALERT",
        "severity": "CRITICAL",
        "quality_score": 0.0,
        "message": "Test data-quality alert",
    }

    await alert_manager.broadcast(alert)

    return {
        "status": "sent",
        "alert": alert,
    }
