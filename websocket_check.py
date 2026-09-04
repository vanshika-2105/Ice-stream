import asyncio
import websockets


async def test_websocket():
    uri = "ws://127.0.0.1:8000/ws/alerts"

    async with websockets.connect(uri) as websocket:
        print("Connected to Alert Server")

        for _ in range(2):
            message = await websocket.recv()

            print("Received WebSocket message:")
            print(message)


if __name__ == "__main__":
    asyncio.run(test_websocket())