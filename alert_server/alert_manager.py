from fastapi import WebSocket


class AlertManager:
    """Manage connected WebSocket clients and connection state."""

    def __init__(self):
        self.clients: list[WebSocket] = []
        self.connection_state = "DISCONNECTED"

    async def connect(self, websocket: WebSocket):
        """Accept and register a WebSocket client."""

        await websocket.accept()
        self.clients.append(websocket)
        self.connection_state = "CONNECTED"

    def disconnect(self, websocket: WebSocket):
        """Remove a disconnected WebSocket client."""

        if websocket in self.clients:
            self.clients.remove(websocket)

        if not self.clients:
            self.connection_state = "DISCONNECTED"

    def get_connection_state(self) -> str:
        """Return the current WebSocket connection state."""

        return self.connection_state

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
