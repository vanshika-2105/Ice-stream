 function AlertPanel({ alert }) {
  return (
    <section className="alert-panel">
      <h2>🚨 Alerts</h2>

      {!alert ? (
        <p>No active critical alerts</p>
      ) : (
        <div className="alert-content">
          <h3>⚠ {alert.severity}</h3>

          <p>{alert.message}</p>

          <p>
            Quality Score: {alert.quality_score}%
          </p>
        </div>
      )}
    </section>
  );
}

export default AlertPanel;
