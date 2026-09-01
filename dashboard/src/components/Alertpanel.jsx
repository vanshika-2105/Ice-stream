function AlertPanel({ alert }) {
  return (
    <section className="alert-panel">
      <h2>🚨 Latest Alert</h2>

      {!alert ? (
        <p>No alerts received yet.</p>
      ) : (
        <div className="alert-content">
          <h3>{alert.type}</h3>

          <p>
            <strong>Event:</strong> {alert.event_id}
          </p>

          <p>
            <strong>Severity:</strong> {alert.severity}
          </p>

          <p>
            <strong>Message:</strong> {alert.message}
          </p>
        </div>
      )}
    </section>
  );
}

export default AlertPanel;
