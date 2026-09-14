from datetime import datetime, timezone
import time
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
from profiler import (
    build_profile,
    calculate_error_percentages,
    get_top_error,
)

from pipeline_metrics import (
    build_pipeline_metrics,
    pipeline_metrics_to_dict,
)


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

# Pipeline performance state
pipeline_start_time = None
pipeline_elapsed_seconds = 0.0
pipeline_latencies_ms = []
pipeline_status = "HEALTHY"

# Data profiling state
profile_event_type_counts = {}
profile_currency_counts = {}



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
# Data quality profile
# --------------------------------------------------

@app.get("/quality/profile")
def get_quality_profile():
    """Return the current data-quality profile."""

    metrics = quality_metrics.get_metrics()

    profile = build_profile(
        total_events=metrics["total_events"],
        valid_events=metrics["valid_events"],
        invalid_events=metrics["invalid_events"],
        error_counts=metrics["error_counts"],
        event_type_counts=profile_event_type_counts,
        currency_counts=profile_currency_counts,
    )

    return {
        "total_events": profile.total_events,
        "valid_events": profile.valid_events,
        "invalid_events": profile.invalid_events,
        "error_counts": profile.error_counts,
        "error_percentages": calculate_error_percentages(
            profile.error_counts
        ),
        "event_type_counts": profile.event_type_counts,
        "currency_counts": profile.currency_counts,
        "top_error": get_top_error(profile.error_counts),
    }
    # --------------------------------------------------
# Pipeline performance metrics
# --------------------------------------------------

@app.get("/pipeline/metrics")
def get_pipeline_metrics():
    """Return current pipeline performance metrics."""

    metrics = quality_metrics.get_metrics()

    pipeline_metrics = build_pipeline_metrics(
        total_events=metrics["total_events"],
        valid_events=metrics["valid_events"],
        invalid_events=metrics["invalid_events"],
        elapsed_seconds=pipeline_elapsed_seconds,
        latencies_ms=pipeline_latencies_ms,
    )

    return pipeline_metrics_to_dict(pipeline_metrics)
# --------------------------------------------------
# Pipeline performance status
# --------------------------------------------------

@app.get("/pipeline/status")
def get_pipeline_status():
    """Return a concise pipeline performance status."""

    metrics = quality_metrics.get_metrics()

    pipeline_metrics = build_pipeline_metrics(
        total_events=metrics["total_events"],
        valid_events=metrics["valid_events"],
        invalid_events=metrics["invalid_events"],
        elapsed_seconds=pipeline_elapsed_seconds,
        latencies_ms=pipeline_latencies_ms,
    )

    pipeline_data = pipeline_metrics_to_dict(pipeline_metrics)

    return {
        "status": pipeline_data["status"],
        "throughput_eps": pipeline_data["throughput_eps"],
        "average_latency_ms": pipeline_data["average_latency_ms"],
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
    Validate an event, update quality and pipeline metrics,
    store historical metrics, broadcast metrics, and generate alerts.
    """

    global pipeline_start_time
    global pipeline_elapsed_seconds

    if pipeline_start_time is None:
        pipeline_start_time = time.perf_counter()

    event_start_time = time.perf_counter()

    # --------------------------------------------------
    # Validate checkout event
    # --------------------------------------------------

    result = validate_checkout_event(event)

    # --------------------------------------------------
    # Record pipeline latency
    # --------------------------------------------------

    pipeline_latency_ms = (
        time.perf_counter() - event_start_time
    ) * 1000

    pipeline_latencies_ms.append(
        round(pipeline_latency_ms, 2)
    )

    pipeline_elapsed_seconds = (
        time.perf_counter() - pipeline_start_time
    )

    # --------------------------------------------------
    # Update data profiling distributions
    # --------------------------------------------------

    event_type = event.get("event_type")
    currency = event.get("currency")

    if event_type:
        profile_event_type_counts[event_type] = (
            profile_event_type_counts.get(event_type, 0) + 1
        )

    if currency:
        profile_currency_counts[currency] = (
            profile_currency_counts.get(currency, 0) + 1
        )

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

        # Keep only the latest 100 alerts
        if len(alert_history) > MAX_ALERT_HISTORY:
            alert_history.pop(0)

        await alert_manager.broadcast(
            alert_data
        )

    # --------------------------------------------------
    # Broadcast pipeline performance metrics
    # --------------------------------------------------

    pipeline_metrics = build_pipeline_metrics(
        total_events=metrics["total_events"],
        valid_events=metrics["valid_events"],
        invalid_events=metrics["invalid_events"],
        elapsed_seconds=pipeline_elapsed_seconds,
        latencies_ms=pipeline_latencies_ms,
    )

    pipeline_data = pipeline_metrics_to_dict(
        pipeline_metrics
    )

    current_pipeline_status = pipeline_data["status"]

    pipeline_message = {
        "type": "PIPELINE_METRICS",
        "total_events": pipeline_metrics.total_events,
        "valid_events": pipeline_metrics.valid_events,
        "invalid_events": pipeline_metrics.invalid_events,
        "throughput_eps": pipeline_metrics.throughput_eps,
        "average_latency_ms": pipeline_metrics.average_latency_ms,
        "dlq_rate": pipeline_metrics.dlq_rate,
        "status": current_pipeline_status,
    }

    await alert_manager.broadcast(
        pipeline_message
    )

    # --------------------------------------------------
    # Evaluate pipeline status transition
    # --------------------------------------------------

    global pipeline_status

    if current_pipeline_status != pipeline_status:

        previous_pipeline_status = pipeline_status

        if current_pipeline_status == "HEALTHY":
            pipeline_transition = {
                "type": "PIPELINE_RECOVERY",
                "previous_status": previous_pipeline_status,
                "current_status": current_pipeline_status,
            }

        elif current_pipeline_status == "DEGRADED":
            pipeline_transition = {
                "type": "PIPELINE_DEGRADED",
                "previous_status": previous_pipeline_status,
                "current_status": current_pipeline_status,
            }

        elif current_pipeline_status == "CRITICAL":
            pipeline_transition = {
                "type": "PIPELINE_CRITICAL",
                "previous_status": previous_pipeline_status,
                "current_status": current_pipeline_status,
            }

        else:
            pipeline_transition = None

        pipeline_status = current_pipeline_status

        if pipeline_transition is not None:
            await alert_manager.broadcast(
                pipeline_transition
            )

    # --------------------------------------------------
    # Return event result
    # --------------------------------------------------

    return {
        **result,
        "status": status,
    }
# --------------------------------------------------
# WebSocket alerts
# --------------------------------------------------

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """Maintain a WebSocket connection for real-time alerts."""

    await alert_manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)