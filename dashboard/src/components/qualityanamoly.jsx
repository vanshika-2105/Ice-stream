function QualityAnomaly({ anomaly }) {
  // No anomaly data yet
  if (!anomaly) {
    return null;
  }

  // Normal condition:
  // Do not show a scary warning when there is no anomaly.
  if (anomaly.is_anomaly !== true) {
    return null;
  }

  // Safely format numerical values
  const formatPercent = (value) => {
    const number = Number(value);

    if (!Number.isFinite(number)) {
      return "N/A";
    }

    return `${number.toFixed(1)}%`;
  };

  return (
    <section className="quality-anomaly">
      <div className="quality-anomaly-header">
        <h2>⚠ Quality Anomaly Detected</h2>
      </div>

      <div className="anomaly-details">

        <div className="anomaly-item">
          <strong>Current Quality</strong>
          <span>
            {formatPercent(anomaly.current_quality)}
          </span>
        </div>

        <div className="anomaly-item">
          <strong>Baseline Quality</strong>
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

      </div>

      <div className="anomaly-reason">
        <strong>Reason:</strong>{" "}
        {anomaly.reason || "Quality dropped below the historical baseline."}
      </div>
    </section>
  );
}

export default QualityAnomaly;
