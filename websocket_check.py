import asyncio
import websockets


async def test_websocket():
    uri = "ws://127.0.0.1:8000/ws/alerts"

    async with websockets.connect(uri) as websocket:
        print("Connected to Alert Server")

        message = await websocket.recv()

        print("Received alert:")
        print(message)


if __name__ == "__main__":
    asyncio.run(test_websocket())