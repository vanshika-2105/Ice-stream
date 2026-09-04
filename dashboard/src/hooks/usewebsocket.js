import { useEffect, useState } from "react";

function useWebSocket(url) {
  const [status, setStatus] = useState("Disconnected");
  const [message, setMessage] = useState(null);

  useEffect(() => {
    let socket;

    try {
      socket = new WebSocket(url);

      socket.onopen = () => {
        console.log("WebSocket connected");
        setStatus("Connected");
      };

      socket.onmessage = (event) => {
        console.log("WebSocket message:", event.data);

        try {
          const data = JSON.parse(event.data);
          setMessage(data);
        } catch {
          setMessage(event.data);
        }
      };

      socket.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      socket.onclose = () => {
        console.log("WebSocket disconnected");
        setStatus("Disconnected");
      };
    } catch (error) {
      console.error("WebSocket connection failed:", error);
      setStatus("Disconnected");
    }

    return () => {
      if (socket) {
        socket.close();
      }
    };
  }, [url]);

  return { status, message };
}

export default useWebSocket;
