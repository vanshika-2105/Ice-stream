 function LiveMetrics({
  metrics,
  throughputHistory = [],
  connectionStatus,
  lastUpdated,
}) {
  const safeNumber = (value) => {
    const number = Number(value);
    return Number.isFinite(number) ? number : 0;
  };

  const getConnectionLabel = () => {
    if (connectionStatus === "connected") {
      return "LIVE";
    }

    if (
      connectionStatus === "connecting" ||
      connectionStatus === "reconnecting"
    ) {
      return "RECONNECTING...";
    }

    return "DISCONNECTED";
  };

  const getConnectionClass = () => {
    if (connectionStatus === "connected") {
      return "live";
    }

    if (
      connectionStatus === "connecting" ||
      connectionStatus === "reconnecting"
    ) {
      return "reconnecting";
    }

    return "disconnected";
  };

  const getChartPoints = () => {
    if (!throughputHistory.length) {
      return "";
    }

    const values = throughputHistory.map((item) =>
      safeNumber(item.value)
    );

    const maxValue = Math.max(...values, 1);
    const minValue = Math.min(...values, 0);
    const range = maxValue - minValue || 1;

    return values
      .map((value, index) => {
        const x =
          throughputHistory.length === 1
            ? 300
            : (index / (throughputHistory.length - 1)) * 600;

        const y =
          180 - ((value - minValue) / range) * 180;

        return `${x},${y}`;
      })
      .join(" ");
  };

  const throughputPoints = getChartPoints();

  const events = safeNumber(
    metrics?.events ?? metrics?.events_processed
  );

  const throughput = safeNumber(metrics?.throughput);

  const latency = safeNumber(
    metrics?.avg_latency ?? metrics?.latency
  );

  const quality = safeNumber(
    metrics?.quality ?? metrics?.quality_score
  );

  const dlqRate = safeNumber(metrics?.dlq_rate);

  const streamStatus = metrics?.stream_status || "UNKNOWN";

  const formattedLastUpdated = lastUpdated
    ? new Date(lastUpdated).toLocaleTimeString()
    : "Never";

  return (
    <section className="live-metrics">
      <div className="live-metrics-header">
        <div>
          <h2>Live Metrics</h2>
          <span>Real-time stream monitoring</span>
        </div>

        <div className={`connection-indicator ${getConnectionClass()}`}>
          <span className="connection-dot">●</span>
          {getConnectionLabel()}
        </div>
      </div>

      {streamStatus === "STALE" && (
        <div className="live-stale-warning">
          <strong>⚠ STREAM STALE</strong>
          <span>
            Metrics are temporarily stale. Last known values are retained.
          </span>
        </div>
      )}

      <div className="live-metric-cards">
        <div className="live-metric-card">
          <span>Live Events</span>
          <strong>{events.toLocaleString()}</strong>
        </div>

        <div className="live-metric-card">
          <span>Throughput</span>
          <strong>{throughput.toFixed(1)} / sec</strong>
        </div>

        <div className="live-metric-card">
          <span>Latency</span>
          <strong>{latency.toFixed(0)} ms</strong>
        </div>

        <div className="live-metric-card">
          <span>Quality</span>
          <strong>{quality.toFixed(1)}%</strong>
        </div>

        <div className="live-metric-card">
          <span>DLQ Rate</span>
          <strong>{dlqRate.toFixed(1)}%</strong>
        </div>

        <div className="live-metric-card">
          <span>Stream Status</span>
          <strong>{streamStatus}</strong>
        </div>
      </div>

      <div className="live-throughput-card">
        <div className="live-throughput-header">
          <h3>Live Throughput</h3>
          <span>Last {throughputHistory.length} points</span>
        </div>

        {throughputHistory.length === 0 ? (
          <p className="live-chart-empty">
            Waiting for live throughput data...
          </p>
        ) : (
          <svg
            className="live-throughput-chart"
            viewBox="0 0 600 180"
            preserveAspectRatio="none"
          >
            <polyline
              points={throughputPoints}
              fill="none"
              stroke="currentColor"
              strokeWidth="3"
              vectorEffect="non-scaling-stroke"
            />

            {throughputHistory.map((item, index) => {
              const values = throughputHistory.map((point) =>
                safeNumber(point.value)
              );

              const maxValue = Math.max(...values, 1);
              const minValue = Math.min(...values, 0);
              const range = maxValue - minValue || 1;

              const x =
                throughputHistory.length === 1
                  ? 300
                  : (index / (throughputHistory.length - 1)) * 600;

              const y =
                180 -
                ((safeNumber(item.value) - minValue) / range) * 180;

              return (
                <circle
                  key={`live-throughput-${index}`}
                  cx={x}
                  cy={y}
                  r="4"
                  fill="currentColor"
                />
              );
            })}
          </svg>
        )}
      </div>

      <div className="live-last-updated">
        Last Updated: <strong>{formattedLastUpdated}</strong>
      </div>
    </section>
  );
}

export default LiveMetrics;
