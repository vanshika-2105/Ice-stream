function ComponentHealth({
  componentHealth = {},
  websocketStatus = "disconnected",
}) {
  const getStatus = (component) => {
    return componentHealth[component] || "HEALTHY";
  };

  const getClassName = (status) => {
    return String(status).toLowerCase();
  };

  const normalizedWebsocketStatus = String(websocketStatus).toLowerCase();

  const isConnected = normalizedWebsocketStatus === "connected";

  return (
    <section className="component-health">
      <div className="component-health-header">
        <div>
          <h2>Component Health</h2>
          <span>System component status</span>
        </div>
      </div>

      <div className="component-health-list">
        {["Kafka", "Flink", "Producer", "Backend"].map((component) => {
          const status = getStatus(component);

          return (
            <div className="component-health-row" key={component}>
              <span>{component}</span>
              <strong className={getClassName(status)}>
                ● {status}
              </strong>
            </div>
          );
        })}

        <div className="component-health-row">
          <span>WebSocket</span>
          <strong className={isConnected ? "healthy" : "critical"}>
            ● {isConnected ? "CONNECTED" : normalizedWebsocketStatus.toUpperCase()}
          </strong>
        </div>
      </div>
    </section>
  );
}

export default ComponentHealth;
