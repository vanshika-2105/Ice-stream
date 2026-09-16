 function AlertHistory({ alerts = [] }) {
  return (
    <section className="alert-history">
      <h2>Alert History</h2>

      {alerts.length === 0 ? (
        <p>No alert history</p>
      ) : (
        <div className="alert-history-list">
          {alerts.map((item, index) => (
            <div
              key={`${item.timestamp || "alert"}-${index}`}
              className="alert-history-item"
            >
              <div>
                <strong>
                  {item.type === "QUALITY_RECOVERY"
                    ? "✓ RECOVERY"
                    : item.severity === "CRITICAL"
                    ? "🚨 CRITICAL"
                    : "⚠ WARNING"}
                </strong>
              </div>

              <div>
                {item.timestamp
                  ? new Date(item.timestamp).toLocaleTimeString()
                  : new Date().toLocaleTimeString()}
              </div>

              {item.quality_score !== undefined && (
                <div>
                  Quality: {item.quality_score}%
                </div>
              )}

              {item.message && (
                <div>
                  {item.message}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default AlertHistory;
