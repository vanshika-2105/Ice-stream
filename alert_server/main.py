from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import sys
from pathlib import Path


# Add quality-rules to Python path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "quality-rules")
)


from alert_server.alert_manager import AlertManager
from alert_server.system_alerts import SystemAlertEngine
from alert_server.circuit_breaker import CircuitBreaker

from alerts import AlertEngine
from metrics import QualityMetrics
from models import QualityMetricSnapshot
from validator import validate_checkout_event
from anomaly import detect_anomaly


app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.3.0",
)


# --------------------------------------------------
# CORS
# --------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------
# Application state
# --------------------------------------------------

alert_manager = AlertManager()
quality_metrics = QualityMetrics()
alert_engine = AlertEngine()
system_alert_engine = SystemAlertEngine()
circuit_breaker = CircuitBreaker()


# --------------------------------------------------
# In-memory alert history
# --------------------------------------------------

alert_history = []

MAX_ALERT_HISTORY = 100


# --------------------------------------------------
# In-memory historical quality metric snapshots
# --------------------------------------------------

quality_history = []

MAX_QUALITY_HISTORY = 100


def calculate_quality_trend() -> str:
    """Return UP, DOWN, or STABLE based on recent quality scores."""

    if len(quality_history) < 2:
        return "STABLE"

    previous_score = quality_history[-2].quality_score
    latest_score = quality_history[-1].quality_score

    if latest_score > previous_score:
        return "UP"

    if latest_score < previous_score:
        return "DOWN"

    return "STABLE"


# --------------------------------------------------
# Health endpoints
# --------------------------------------------------

@app.get("/health")
def health_check():
    """Liveness check: verify that the alert server process is running."""

    return {
        "status": "ok",
        "service": "alert-server",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health/ready")
def readiness_check():
    """Readiness check: verify that the backend is ready to serve requests."""

    return {
        "status": "ready",
        "service": "alert-server",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------
# Root endpoint
# --------------------------------------------------

@app.get("/")
def root():
    return {
        "message": "Ice-Stream Alert Server is running"
    }


# --------------------------------------------------
# Current metrics endpoint
# --------------------------------------------------

@app.get("/metrics")
def get_metrics():
    """Return current data-quality metrics with current status."""

    metrics = quality_metrics.get_metrics()

    return {
        **metrics,
        "status": alert_engine.get_status(
            metrics["quality_score"]
        ),
    }


# --------------------------------------------------
# Alert history endpoint
# --------------------------------------------------

@app.get("/alerts")
def get_alerts():
    """Return recent quality and system alert history."""

    return {
        "alerts": alert_history
    }


# --------------------------------------------------
# Current quality status
# --------------------------------------------------

@app.get("/quality/status")
def get_quality_status():
    """Return the current data-quality state and metrics."""

    metrics = quality_metrics.get_metrics()

    return {
        "status": alert_engine.get_status(
            metrics["quality_score"]
        ),
        "quality_score": metrics["quality_score"],
        "total_events": metrics["total_events"],
        "valid_events": metrics["valid_events"],
        "invalid_events": metrics["invalid_events"],
        "invalid_event_rate": metrics["invalid_event_rate"],
        "trend": calculate_quality_trend(),
        "is_anomaly": alert_engine.anomaly_active,
        "anomaly_severity": alert_engine.anomaly_severity,
    }


# --------------------------------------------------
# Historical quality metrics
# --------------------------------------------------

@app.get("/quality/history")
def get_quality_history(limit: int = 20):
    """
    Return historical data-quality metric snapshots.

    Default:
        20 snapshots

    Maximum:
        100 snapshots
    """

    if limit < 1 or limit > MAX_QUALITY_HISTORY:
        raise HTTPException(
            status_code=400,
            detail=(
                f"limit must be between 1 and "
                f"{MAX_QUALITY_HISTORY}"
            ),
        )

    snapshots = quality_history[-limit:]

    return {
        "history": [
            snapshot.to_dict()
            for snapshot in snapshots
        ]
    }


# --------------------------------------------------
# Quality anomaly detection
# --------------------------------------------------

@app.get("/quality/anomaly")
def get_quality_anomaly():
    """
    Detect whether the current quality score is anomalous
    compared with recent historical quality scores.
    """

    metrics = quality_metrics.get_metrics()
    current_quality = metrics["quality_score"]

    # Use previous snapshots as the baseline history.
    # The current score must not be included in its own baseline.
    history = [
        snapshot.quality_score
        for snapshot in quality_history[:-1]
    ]

    result = detect_anomaly(
        current_quality=current_quality,
        history=history,
    )

    return {
        "is_anomaly": result.is_anomaly,
        "severity": result.severity,
        "current_quality": result.current_quality,
        "baseline_quality": result.baseline_quality,
        "deviation": round(result.deviation, 2),
        "reason": result.reason,
    }


# --------------------------------------------------
# System status
# --------------------------------------------------

@app.get("/system/status")
def get_system_status():
    """Return the current infrastructure system status."""

    return system_alert_engine.get_system_status()


@app.post("/system/components/{component}")
async def update_system_component(
    component: str,
    payload: dict,
):
    """Update the status of an infrastructure component."""

    status = payload.get("status")

    if status not in {"UP", "DOWN"}:
        raise HTTPException(
            status_code=400,
            detail="status must be UP or DOWN",
        )

    alert = system_alert_engine.update_component(
        component=component,
        status=status,
        message=payload.get("message"),
    )

    if alert is not None:

        alert_data = alert.to_dict()

        alert_data["id"] = (
            f"system-alert-{len(alert_history) + 1:03d}"
        )

        alert_history.append(alert_data)

        if len(alert_history) > MAX_ALERT_HISTORY:
            alert_history.pop(0)

        await alert_manager.broadcast(alert_data)

    return {
        "status": status,
        "component": component,
        "alert": alert.to_dict() if alert else None,
    }


# --------------------------------------------------
# Event processing
# --------------------------------------------------

@app.post("/events")
async def process_event(event: dict):
    """
    Validate an event, update metrics, store historical
    metrics, broadcast metrics, and generate alerts.
    """

    # --------------------------------------------------
    # Validate checkout event
    # --------------------------------------------------

    result = validate_checkout_event(event)

    # --------------------------------------------------
    # Update quality metrics
    # --------------------------------------------------

    if result["valid"]:
        quality_metrics.record_valid()
    else:
        quality_metrics.record_invalid(
            result["errors"]
        )

    # --------------------------------------------------
    # Get current metrics
    # --------------------------------------------------

    metrics = quality_metrics.get_metrics()

    # --------------------------------------------------
    # Determine current quality status
    # --------------------------------------------------

    status = alert_engine.get_status(
        metrics["quality_score"]
    )

    # --------------------------------------------------
    # Store historical quality snapshot
    # --------------------------------------------------

    snapshot = QualityMetricSnapshot(
        timestamp=datetime.now(timezone.utc),
        total_events=metrics["total_events"],
        valid_events=metrics["valid_events"],
        invalid_events=metrics["invalid_events"],
        quality_score=metrics["quality_score"],
        invalid_event_rate=metrics["invalid_event_rate"],
    )

    quality_history.append(snapshot)

    # Keep only the latest 100 snapshots
    if len(quality_history) > MAX_QUALITY_HISTORY:
        quality_history.pop(0)

    # --------------------------------------------------
    # Detect quality anomaly
    # --------------------------------------------------

    anomaly_history = [
        snapshot.quality_score
        for snapshot in quality_history[:-1]
    ]

    anomaly_result = detect_anomaly(
        current_quality=metrics["quality_score"],
        history=anomaly_history,
    )

    # --------------------------------------------------
    # Broadcast current quality metrics
    # --------------------------------------------------

    metrics_message = {
        "type": "QUALITY_METRICS",
        "total_events": metrics["total_events"],
        "valid_events": metrics["valid_events"],
        "invalid_events": metrics["invalid_events"],
        "quality_score": metrics["quality_score"],
        "invalid_event_rate": metrics["invalid_event_rate"],
        "error_counts": metrics["error_counts"],
        "status": status,
        "trend": calculate_quality_trend(),
    }

    await alert_manager.broadcast(
        metrics_message
    )

    # --------------------------------------------------
    # Evaluate intelligent anomaly alert
    # --------------------------------------------------

    anomaly_alert = alert_engine.evaluate_anomaly(
        is_anomaly=anomaly_result.is_anomaly,
        severity=anomaly_result.severity,
        current_quality=anomaly_result.current_quality,
        baseline_quality=anomaly_result.baseline_quality,
        deviation=anomaly_result.deviation,
        reason=anomaly_result.reason,
    )

    # --------------------------------------------------
    # Preserve existing anomaly notification
    # --------------------------------------------------

    if anomaly_result.is_anomaly:

        anomaly_message = {
            "type": "QUALITY_ANOMALY",
            "is_anomaly": True,
            "severity": anomaly_result.severity,
            "current_quality": anomaly_result.current_quality,
            "baseline_quality": anomaly_result.baseline_quality,
            "deviation": round(
                anomaly_result.deviation,
                2,
            ),
            "reason": anomaly_result.reason,
        }

        await alert_manager.broadcast(
            anomaly_message
        )

    # --------------------------------------------------
    # Broadcast intelligent anomaly alert/recovery
    # --------------------------------------------------

    if anomaly_alert is not None:

        await alert_manager.broadcast(
            anomaly_alert
        )

    # --------------------------------------------------
    # Evaluate quality alert
    # --------------------------------------------------

    alert = alert_engine.evaluate(
        quality_score=metrics["quality_score"],
        invalid_event_rate=metrics["invalid_event_rate"],
    )

    if alert is not None:

        alert_data = alert.to_dict()

        # Add unique history ID
        alert_data["id"] = (
            f"alert-{len(alert_history) + 1:03d}"
        )

        # Store newest alert
        alert_history.append(alert_data)

        # Keep only the latest alerts
        if len(alert_history) > MAX_ALERT_HISTORY:
            alert_history.pop(0)

        # Broadcast alert
        await alert_manager.broadcast(
            alert_data
        )

    return {
        **result,
        "status": status,
    }


# --------------------------------------------------
# WebSocket
# --------------------------------------------------

@app.websocket("/ws/alerts")
async def websocket_alerts(
    websocket: WebSocket,
):
    """WebSocket endpoint for real-time data-quality messages."""

    await alert_manager.connect(websocket)

    try:

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)

    except Exception:
        alert_manager.disconnect(websocket)


# --------------------------------------------------
# Test alert
# --------------------------------------------------

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