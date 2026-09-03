from fastapi import WebSocket


class AlertManager:
    """Manage connected WebSocket clients and broadcast alerts."""

    def __init__(self):
        self.clients: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Accept and register a WebSocket client."""
        await websocket.accept()
        self.clients.append(websocket)

    def disconnect(self, websocket: WebSocket):
        """Remove a disconnected WebSocket client."""
        if websocket in self.clients:
            self.clients.remove(websocket)

    async def broadcast(self, alert: dict):
        """Send an alert to all connected clients."""
        disconnected_clients = []

        for client in self.clients:
            try:
                await client.send_json(alert)
            except Exception:
                disconnected_clients.append(client)

        for client in disconnected_clients:
            self.disconnect(client)