 function AnomalyHistory({ anomalies = [] }) {
  return (
    <section className="anomaly-history">
      <h2>Recent Anomalies</h2>

      {anomalies.length === 0 ? (
        <p>No anomalies detected</p>
      ) : (
        <div className="anomaly-history-list">
          {anomalies.map((item, index) => (
            <div
              key={`${item.timestamp || "anomaly"}-${index}`}
              className="anomaly-history-item"
            >
              <span>
                {item.timestamp
                  ? new Date(item.timestamp).toLocaleTimeString()
                  : "N/A"}
              </span>

              <strong>
                {Number.isFinite(Number(item.current_quality))
                  ? `${Number(item.current_quality).toFixed(1)}%`
                  : "N/A"}
              </strong>

              <span>
                {Number.isFinite(Number(item.baseline_quality))
                  ? `${Number(item.baseline_quality).toFixed(1)}%`
                  : "N/A"}
              </span>

              <strong>
                {item.severity ||
                  (Number(item.deviation) >= 10
                    ? "CRITICAL"
                    : "WARNING")}
              </strong>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default AnomalyHistory;
