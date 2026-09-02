from datetime import datetime, timezone
import sys
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "quality-rules"))
from alert_server.alert_manager import AlertManager
from metrics import QualityMetrics
from validator import validate_checkout_event
from thresholds import get_quality_severity

app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.2.0",
)

alert_manager = AlertManager()
quality_metrics = QualityMetrics()

@app.get("/health")
def health_check():
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
    """Return current data-quality metrics."""
    return quality_metrics.get_metrics()
@app.get("/alerts")
def get_alerts():
    """Return the current quality alert status."""

    metrics = quality_metrics.get_metrics()
    severity = get_quality_severity(metrics["quality_score"])

    if severity is None:
        return {
            "alert": False,
            "severity": None,
            "quality_score": metrics["quality_score"],
        }

    return {
        "alert": True,
        "severity": severity,
        "quality_score": metrics["quality_score"],
        "message": "Data quality dropped below threshold",
    }
@app.post("/events")
async def process_event(event: dict):
    """Validate an event, update metrics, and generate quality alerts."""

    result = validate_checkout_event(event)

    if result["valid"]:
        quality_metrics.record_valid()
    else:
        quality_metrics.record_invalid(result["errors"])

    metrics = quality_metrics.get_metrics()

    # Check whether the current quality score breaches a threshold.
    severity = get_quality_severity(metrics["quality_score"])

    if severity:
        alert = {
            "type": "QUALITY_ALERT",
            "severity": severity,
            "quality_score": metrics["quality_score"],
            "message": "Data quality dropped below threshold",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await alert_manager.broadcast(alert)

    return result

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """WebSocket endpoint for real-time data quality alerts."""

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
    """Send a fake data-quality alert to connected WebSocket clients."""

    alert = {
        "type": "DATA_QUALITY_ERROR",
        "event_id": "evt_test",
        "severity": "ERROR",
        "message": "Amount cannot be negative",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    await alert_manager.broadcast(alert)

    return {
        "status": "sent",
        "alert": alert,
    }