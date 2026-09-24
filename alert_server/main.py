from datetime import datetime, timezone
import time
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware

import sys
from pathlib import Path
from alert_server.config import (
    ALLOWED_ORIGINS,
    WEBSOCKET_UPDATE_INTERVAL_SECONDS,
    PERFORMANCE_HISTORY_SIZE,
)
ALLOWED_COMPONENTS = {
    "kafka",
    "producer",
    "flink",
    "backend",
    "websocket",
    "iceberg",
}
# Add quality-rules to Python path
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "quality-rules")
)


from alert_server.alert_manager import AlertManager
from alert_server.system_alerts import SystemAlertEngine
from alert_server.retry import retry_call
from alert_server.recovery_metrics import RecoveryMetrics
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
    build_performance_snapshot,
    calculate_latency_percentiles,
    performance_snapshot_to_dict,
    pipeline_metrics_to_dict,
)

from performance_history import PerformanceHistory
from health_checks import (
    check_kafka,
    check_producer,
    check_flink,
    check_backend,
    check_websocket,
    check_iceberg,
)
from live_aggregator import LiveAggregator
from insights import generate_insights

# Live metrics broadcast state
last_live_metrics_broadcast = 0.0
LIVE_METRICS_INTERVAL_SECONDS = (
    WEBSOCKET_UPDATE_INTERVAL_SECONDS
)
app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.3.0",
)
@app.exception_handler(Exception)
async def handle_unexpected_error(
    request: Request,
    exc: Exception,
):
    """Return a safe response for unexpected server errors."""

    return {
        "error": "Internal server error",
        "detail": "Service temporarily unavailable",
    }
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add basic security headers to HTTP responses."""

    response = await call_next(request)

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = (
        "camera=(), microphone=(), geolocation=()"
    )

    return response

def calculate_overall_status(statuses: list[str]) -> str:
    """Calculate the overall observability status."""

    normalized_statuses = {
        status.upper()
        for status in statuses
        if status
    }

    # Failed/critical components make the whole system critical.
    if (
        "CRITICAL" in normalized_statuses
        or "FAILED" in normalized_statuses
    ):
        return "CRITICAL"

    # Degraded or recovering components make the system degraded.
    if (
        "DEGRADED" in normalized_statuses
        or "WARNING" in normalized_statuses
        or "DOWN" in normalized_statuses
        or "RECOVERING" in normalized_statuses
    ):
        return "DEGRADED"

    return "HEALTHY"
# --------------------------------------------------
# CORS
# --------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
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
live_aggregator = LiveAggregator()

# Pipeline performance state
pipeline_start_time = None
pipeline_elapsed_seconds = 0.0
pipeline_latencies_ms = []
pipeline_status = "HEALTHY"

# Historical pipeline performance snapshots
performance_history = PerformanceHistory(
    max_size=PERFORMANCE_HISTORY_SIZE
)

# Data profiling state
profile_event_type_counts = {}
profile_currency_counts = {}



system_alert_engine = SystemAlertEngine()
circuit_breaker = CircuitBreaker()
recovery_metrics = RecoveryMetrics()

def execute_with_resilience(func):
    """
    Execute a backend operation using the existing retry
    and circuit-breaker mechanisms while recording recovery metrics.
    """

    if not circuit_breaker.can_execute():
        recovery_metrics.record_circuit_transition(
            circuit_breaker.state.value
        )

        raise HTTPException(
            status_code=503,
            detail="Dependency temporarily unavailable (circuit open)",
        )

    try:
        result = retry_call(func)

        previous_state = circuit_breaker.state

        circuit_breaker.record_success()

        recovery_metrics.current_circuit_state = (
            circuit_breaker.state.value
        )

        if previous_state.value == "HALF_OPEN":
            recovery_metrics.record_recovery("kafka")

            recovery_metrics.record_circuit_transition(
                circuit_breaker.state.value,
                "kafka",
            )

        return result

    except Exception:
        circuit_breaker.record_failure()

        recovery_metrics.record_failure("kafka")

        recovery_metrics.record_circuit_transition(
            circuit_breaker.state.value,
            "kafka",
        )

        raise

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

    response = pipeline_metrics_to_dict(
        pipeline_metrics
    )

    percentiles = calculate_latency_percentiles(
        pipeline_latencies_ms
    )

    response.update(percentiles)

    latest_snapshot = performance_history.get_latest()

    response["performance_history"] = [
        performance_snapshot_to_dict(snapshot)
        for snapshot in performance_history.get_history()
    ]

    response["latest_snapshot"] = (
        performance_snapshot_to_dict(latest_snapshot)
        if latest_snapshot
        else None
    )

    return response
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

def get_component_health():
    """Return health checks for all known platform components."""

    system_components = system_alert_engine.component_status

    kafka_health = check_kafka(
        system_components.get("kafka")
    )

    producer_health = check_producer(
        live_aggregator.last_event_time
    )

    flink_health = check_flink(None)

    backend_health = check_backend()

    websocket_health = check_websocket(
        alert_manager.get_connection_state()
    )

    iceberg_health = check_iceberg(
        system_components.get("iceberg")
    )

    health_results = [
        kafka_health,
        flink_health,
        producer_health,
        backend_health,
        websocket_health,
        iceberg_health,
    ]

    return {
        health.component: {
            "status": health.status,
            "message": health.message,
            "checked_at": health.checked_at.isoformat(),
        }
        for health in health_results
    }
# --------------------------------------------------
# System status
# --------------------------------------------------

@app.get("/system/status")
def get_system_status():
    """Return the current infrastructure and component health status."""

    status = system_alert_engine.get_system_status()

    status["websocket"] = {
        "status": alert_manager.get_connection_state(),
        "connected_clients": len(alert_manager.clients),
    }

    status["health"] = get_component_health()

    return status


@app.post("/system/components/{component}")
async def update_system_component(
    component: str,
    payload: dict,
):
    """Update the status of an infrastructure component."""

    component = component.strip().lower()

    if component not in ALLOWED_COMPONENTS:
        raise HTTPException(
            status_code=400,
            detail="Unknown system component",
        )

    status = payload.get("status")

    if status not in {"UP", "DOWN"}:
        raise HTTPException(
            status_code=400,
            detail="status must be UP or DOWN",
        )

    message = payload.get("message")

    if message is not None:
        if not isinstance(message, str):
            raise HTTPException(
                status_code=400,
                detail="message must be a string",
            )

        message = message.strip()

        if len(message) > 500:
            raise HTTPException(
                status_code=400,
                detail="message must not exceed 500 characters",
            )

    alert = system_alert_engine.update_component(
        component=component,
        status=status,
        message=message,
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


@app.get("/observability/live")
def get_live_observability():
    """Return the latest rolling live streaming metrics."""

    metrics = live_aggregator.get_metrics()

    return {
        "status": metrics.status,
        "total_events": metrics.total_events,
        "valid_events": metrics.valid_events,
        "invalid_events": metrics.invalid_events,
        "throughput_eps": metrics.throughput_eps,
        "average_latency_ms": metrics.average_latency_ms,
        "dlq_rate": metrics.dlq_rate,
        "quality_percentage": metrics.quality_percentage,
        "last_event_time": (
            metrics.last_event_time.isoformat()
            if metrics.last_event_time
            else None
        ),
    }
# --------------------------------------------------
# Unified observability overview
# --------------------------------------------------

@app.get("/observability/overview")
def get_observability_overview():
    """Return a unified observability view of the platform."""

    quality = get_quality_status()
    anomaly = get_quality_anomaly()
    profile = get_quality_profile()
    pipeline = get_pipeline_metrics()
    system = get_system_status()

    statuses = [
        quality["status"],
        pipeline["status"],
    ]

    components = system.get("components", {})

    component_statuses = [
        status
        for status in components.values()
        if isinstance(status, str)
    ]

    # Kafka DOWN is considered CRITICAL because Kafka is
    # the primary event-ingestion dependency.
    if components.get("kafka") == "DOWN":
        statuses.append("CRITICAL")
    else:
        statuses.extend(
            status
            for status in component_statuses
            if status
        )

    overall_status = calculate_overall_status(statuses)

    return {
        "quality": {
            "quality_score": quality["quality_score"],
            "status": quality["status"],
            "valid_events": quality["valid_events"],
            "invalid_events": quality["invalid_events"],
        },
        "anomaly": {
            "is_anomaly": anomaly["is_anomaly"],
            "severity": anomaly["severity"],
            "deviation": anomaly["deviation"],
        },
        "profile": {
            "top_error": profile["top_error"],
            "error_counts": profile["error_counts"],
        },
        "pipeline": {
            "total_events": pipeline["total_events"],
            "throughput_eps": pipeline["throughput_eps"],
            "average_latency_ms": pipeline["average_latency_ms"],
            "dlq_rate": pipeline["dlq_rate"],
            "status": pipeline["status"],
        },
        "system": system,
        "recovery": recovery_metrics.get_metrics(),
        "overall_status": overall_status,
    }
# --------------------------------------------------
# Observability insights
# --------------------------------------------------

@app.get("/observability/insights")
def get_observability_insights():
    """Return human-readable insights from current observability metrics."""

    metrics = quality_metrics.get_metrics()

    # Current quality metrics
    current_quality = metrics["quality_score"]

    # Previous quality snapshot, when available
    previous_quality = None
    if len(quality_history) >= 2:
        previous_quality = quality_history[-2].quality_score

    # Current pipeline metrics
    pipeline = get_pipeline_metrics()

    # Current error profile
    profile = get_quality_profile()

    # Current anomaly state
    anomaly = get_quality_anomaly()

    insights = generate_insights(
        previous_quality=previous_quality,
        current_quality=current_quality,
        previous_latency=None,
        current_latency=pipeline["average_latency_ms"],
        previous_throughput=None,
        current_throughput=pipeline["throughput_eps"],
        previous_dlq_rate=None,
        current_dlq_rate=pipeline["dlq_rate"],
        previous_errors=None,
        current_errors=profile["error_counts"],
        is_anomaly=anomaly["is_anomaly"],
        anomaly_severity=anomaly["severity"],
        anomaly_reason=(
            f"Quality deviation: {anomaly['deviation']}"
            if anomaly["is_anomaly"]
            else ""
        ),
    )

    return {
        "insights": [
            insight.to_dict()
            for insight in insights
        ],
        "count": len(insights),
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
        # --------------------------------------------------
    # Update live streaming metrics
    # --------------------------------------------------

    live_aggregator.record_event(
        valid=result["valid"],
        latency_ms=pipeline_latency_ms,
        event_time=datetime.now(timezone.utc),
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
    # Build pipeline performance metrics
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
        # --------------------------------------------------
    # Store performance snapshot
    # --------------------------------------------------

    performance_snapshot = build_performance_snapshot(
        timestamp=datetime.now(timezone.utc).isoformat(),
        events_received=metrics["total_events"],
        events_processed=metrics["total_events"],
        latencies_ms=pipeline_latencies_ms,
        elapsed_seconds=pipeline_elapsed_seconds,
        dlq_events=metrics["invalid_events"],
    )

    performance_history.add_snapshot(
        performance_snapshot
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
    # Broadcast unified observability overview
    # --------------------------------------------------

    overview = get_observability_overview()

    # --------------------------------------------------
    # Broadcast throttled live metrics
    # --------------------------------------------------

    global last_live_metrics_broadcast

    current_time = time.monotonic()

    if (
        current_time - last_live_metrics_broadcast
        >= LIVE_METRICS_INTERVAL_SECONDS
    ):
        live_metrics = live_aggregator.get_metrics()

        live_metrics_message = {
            "type": "LIVE_METRICS",
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "data": {
                "total_events": live_metrics.total_events,
                "valid_events": live_metrics.valid_events,
                "invalid_events": live_metrics.invalid_events,
                "throughput_eps": live_metrics.throughput_eps,
                "average_latency_ms": (
                    live_metrics.average_latency_ms
                ),
                "dlq_rate": live_metrics.dlq_rate,
                "quality_percentage": (
                    live_metrics.quality_percentage
                ),
                "stream_status": live_metrics.status,
            },
        }

        await alert_manager.broadcast(
            live_metrics_message
        )

        # Record the actual aggregated WebSocket update.
        live_aggregator.record_websocket_update(
            update_time=current_time
        )

        last_live_metrics_broadcast = current_time

    # --------------------------------------------------
    # Broadcast observability overview
    # --------------------------------------------------

    overview_message = {
        "type": "OBSERVABILITY_OVERVIEW",
        "overall_status": overview["overall_status"],
        "quality_score": overview["quality"]["quality_score"],
        "throughput_eps": overview["pipeline"]["throughput_eps"],
        "average_latency_ms": (
            overview["pipeline"]["average_latency_ms"]
        ),
        "dlq_rate": overview["pipeline"]["dlq_rate"],
        "is_anomaly": overview["anomaly"]["is_anomaly"],
    }

    await alert_manager.broadcast(
        overview_message
    )
    
    # --------------------------------------------------
    # Broadcast observability insights
    # --------------------------------------------------

    insight_response = get_observability_insights()

    for insight in insight_response["insights"]:
        await alert_manager.broadcast(
            {
                "type": "OBSERVABILITY_INSIGHT",
                "insight": insight,
            }
        )

    # --------------------------------------------------
    # Return event validation result
    # --------------------------------------------------
    # --------------------------------------------------

    return {
        "valid": result["valid"],
        "errors": result["errors"],
        "status": status,
    }
# --------------------------------------------------
# WebSocket endpoint
# --------------------------------------------------

@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    """Maintain a live WebSocket connection for alerts."""

    origin = websocket.headers.get("origin")

    if origin and origin not in ALLOWED_ORIGINS:
        await websocket.close(code=1008)
        return

    await alert_manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)

    except Exception:
        alert_manager.disconnect(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)

    except Exception:
        alert_manager.disconnect(websocket)