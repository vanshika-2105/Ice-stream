 function StatusCard({ invalidEvents = 0 }) {
  return (
    <section className="status-section">
      <h2>System Status</h2>

      <div className="status-grid">
        <div className="status-card">
          <h3>Kafka</h3>
          <p>● Online</p>
        </div>

        <div className="status-card">
          <h3>Flink</h3>
          <p>● Online</p>
        </div>

        <div className="status-card">
          <h3>Iceberg</h3>
          <p>● Online</p>
        </div>

        <div className="status-card">
          <h3>Backend</h3>
          <p>● Online</p>
        </div>

        <div className="status-card">
          <h3>WebSocket</h3>
          <p>● Connected</p>
        </div>

        <div className="status-card">
          <h3>DLQ</h3>
          <p>
            ● {invalidEvents > 0 ? "Active" : "No Errors"}
          </p>
        </div>
      </div>
    </section>
  );
}

export default StatusCard;
