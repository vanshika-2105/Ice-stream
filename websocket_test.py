import asyncio
import websockets

async def main():
    uri = "ws://localhost:8000/ws/alerts"

    print("Connecting...")
    
    async with websockets.connect(uri) as websocket:
        print("CONNECTED")
        print("Waiting for alert event...")

        try:
            while True:
                message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=30
                )
                print("RECEIVED:")
                print(message)

        except asyncio.TimeoutError:
            print("No event received within 30 seconds.")

asyncio.run(main())
