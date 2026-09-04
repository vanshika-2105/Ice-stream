  import Header from "./components/Header";
import Pipeline from "./components/Pipeline";
import StatusCard from "./components/StatusCard";
import AlertPanel from "./components/AlertPanel";
import useWebSocket from "./hooks/useWebSocket";

function App() {
  const { status, latestMessage } = useWebSocket(
    "ws://localhost:8000/ws"
  );

  return (
    <main className="app">
      <Header />

      <Pipeline />

      <section className="dashboard-grid">
        <StatusCard />

        <section className="websocket-card">
          <h2>WebSocket</h2>

          <p>
            Connection Status:{" "}
            <strong>{status}</strong>
          </p>

          <h3>Latest Backend Message</h3>

          {latestMessage ? (
            <pre>
              {typeof latestMessage === "string"
                ? latestMessage
                : JSON.stringify(latestMessage, null, 2)}
            </pre>
          ) : (
            <p>No message received yet.</p>
          )}
        </section>
      </section>

      <AlertPanel alert={latestMessage} />
    </main>
  );
}

export default App;
