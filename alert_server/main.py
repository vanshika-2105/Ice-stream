from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from alert_server.alert_manager import AlertManager


app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.2.0",
)

alert_manager = AlertManager()


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