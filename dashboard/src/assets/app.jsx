  import Header from "./components/Header";
import Pipeline from "./components/Pipeline";
import StatusCard from "./components/StatusCard";
import useWebSocket from "./hooks/useWebSocket";
import "./App.css";

function App() {
  const { status, message } = useWebSocket(
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

          {message ? (
            <pre>
              {typeof message === "string"
                ? message
                : JSON.stringify(message, null, 2)}
            </pre>
          ) : (
            <p>No message received yet.</p>
          )}
        </section>
      </section>
    </main>
  );
}

export default App;
