 function QualityAnomaly({ anomaly }) {
  // No anomaly data
  if (!anomaly) {
    return null;
  }

  // If the message says there is no anomaly,
  // don't show the warning card.
  if (anomaly.is_anomaly !== true) {
    return null;
  }

  // Safely format percentage values
  const formatPercent = (value) => {
    const number = Number(value);

    if (!Number.isFinite(number)) {
      return "N/A";
    }

    return `${number.toFixed(1)}%`;
  };

  // Determine severity
  const severity =
    anomaly.severity ||
    (Number(anomaly.deviation) >= 10
      ? "CRITICAL"
      : "WARNING");

  return (
    <section className={`quality-anomaly ${severity.toLowerCase()}`}>
      {/* Header */}
      <div className="quality-anomaly-header">
        <h2>⚠ QUALITY ANOMALY</h2>

        <span className="anomaly-severity">
          Severity: {severity}
        </span>
      </div>

      {/* Anomaly Details */}
      <div className="anomaly-details">

        <div className="anomaly-item">
          <strong>Current Quality</strong>

          <span>
            {formatPercent(anomaly.current_quality)}
          </span>
        </div>

        <div className="anomaly-item">
          <strong>Historical Baseline</strong>

          <span>
            {formatPercent(anomaly.baseline_quality)}
          </span>
        </div>

        <div className="anomaly-item">
          <strong>Deviation</strong>

          <span>
            {formatPercent(anomaly.deviation)}
          </span>
        </div>

        <div className="anomaly-item">
          <strong>Severity</strong>

          <span>
            {severity}
          </span>
        </div>

      </div>

      {/* Reason */}
      <div className="anomaly-reason">
        <strong>Reason:</strong>

        <p>
          {anomaly.reason ||
            "Quality dropped significantly below historical baseline."}
        </p>
      </div>
    </section>
  );
}

export default QualityAnomaly;
