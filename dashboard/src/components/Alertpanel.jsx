 function AlertPanel({ alert, alerts = [] }) {
  const getIcon = (severity) => {
    if (severity === "CRITICAL") return "🚨";
    if (severity === "WARNING") return "⚠";
    if (severity === "RECOVERY") return "✓";
    return "ℹ";
  };

  return (
    <section className="alert-panel">
      <h2>Alerts</h2>

      {/* Current Alert */}
      {alert ? (
        <div className={`current-alert ${alert.severity?.toLowerCase()}`}>
          <h3>
            {getIcon(alert.severity)} {alert.severity || "ALERT"}
          </h3>

          {alert.quality_score !== undefined && (
            <p>
              <strong>Quality:</strong> {alert.quality_score}%
            </p>
          )}

          <p>
            <strong>Message:</strong>{" "}
            {alert.message || "No message"}
          </p>

          {alert.timestamp && (
            <p>
              <strong>Time:</strong> {alert.timestamp}
            </p>
          )}
        </div>
      ) : (
        <p>No active alerts</p>
      )}

      {/* Alert History */}
      <h3>Recent Alerts</h3>

      {alerts.length === 0 ? (
        <p>No alert history</p>
      ) : (
        <div className="alert-history">
          {alerts.map((item, index) => (
            <div
              key={index}
              className={`alert-history-item ${item.severity?.toLowerCase()}`}
            >
              <strong>
                {getIcon(item.severity)}{" "}
                {item.severity || "ALERT"}
              </strong>

              {item.quality_score !== undefined && (
                <p>
                  Quality: {item.quality_score}%
                </p>
              )}

              <p>
                {item.message || "No message"}
              </p>

              {item.timestamp && (
                <small>{item.timestamp}</small>
              )}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default AlertPanel;
