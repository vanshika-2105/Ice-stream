 import { useEffect, useRef, useState } from "react";

function useWebSocket(url) {
  const [status, setStatus] = useState("Disconnected");
  const [latestMessage, setLatestMessage] = useState(null);

  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const shouldReconnectRef = useRef(true);

  useEffect(() => {
    shouldReconnectRef.current = true;

    const connect = () => {
      if (!shouldReconnectRef.current) return;

      console.log("Connecting to WebSocket...");

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log("WebSocket connected");
        setStatus("Connected");
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          console.log("Received:", data);

          setLatestMessage(data);
        } catch (error) {
          console.error(
            "Invalid WebSocket message:",
            error
          );
        }
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        setStatus("Disconnected");
      };

      ws.onclose = () => {
        console.log("WebSocket disconnected");
        setStatus("Disconnected");

        if (shouldReconnectRef.current) {
          console.log(
            "Trying to reconnect in 3 seconds..."
          );

          reconnectTimerRef.current = setTimeout(() => {
            connect();
          }, 3000);
        }
      };
    };

    connect();

    return () => {
      shouldReconnectRef.current = false;

      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }

      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [url]);

  return {
    status,
    latestMessage,
  };
}

export default useWebSocket;
