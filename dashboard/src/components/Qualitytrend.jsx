  function QualityTrend({ history = [], anomalies = [] }) {
  if (history.length === 0) {
    return (
      <section className="quality-trend">
        <h2>Quality Trend</h2>
        <p>No quality history available</p>
      </section>
    );
  }

  const width = 900;
  const height = 300;
  const padding = 40;

  const points = history.map((item, index) => {
    const x =
      history.length === 1
        ? width / 2
        : padding +
          (index * (width - padding * 2)) /
            (history.length - 1);

    const score = Number(item.score) || 0;

    const y =
      height -
      padding -
      (score / 100) *
        (height - padding * 2);

    // Check whether this quality point is anomalous
    const isAnomaly = anomalies.some((anomaly) => {
      const anomalyTime = anomaly.timestamp;
      const pointTime = item.timestamp;

      return (
        anomalyTime &&
        pointTime &&
        anomalyTime === pointTime
      );
    });

    return {
      x,
      y,
      score,
      timestamp: item.timestamp,
      isAnomaly,
    };
  });

  const linePoints = points
    .map((point) => `${point.x},${point.y}`)
    .join(" ");

  // Determine trend
  let trend = "STABLE";

  if (history.length >= 2) {
    const previous =
      Number(
        history[history.length - 2].score
      ) || 0;

    const latest =
      Number(
        history[history.length - 1].score
      ) || 0;

    const difference = latest - previous;

    if (difference > 1) {
      trend = "UP";
    } else if (difference < -1) {
      trend = "DOWN";
    }
  }

  return (
    <section className="quality-trend">

      <div className="quality-trend-header">
        <h2>Quality Trend</h2>

        <span
          className={`trend-indicator ${trend.toLowerCase()}`}
        >
          {trend}
        </span>
      </div>

      <div className="quality-chart-wrapper">

        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="quality-chart"
          preserveAspectRatio="none"
        >

          {/* 100% line */}
          <line
            x1={padding}
            y1={padding}
            x2={width - padding}
            y2={padding}
            stroke="currentColor"
            opacity="0.2"
          />

          {/* 50% line */}
          <line
            x1={padding}
            y1={height / 2}
            x2={width - padding}
            y2={height / 2}
            stroke="currentColor"
            opacity="0.2"
          />

          {/* 0% line */}
          <line
            x1={padding}
            y1={height - padding}
            x2={width - padding}
            y2={height - padding}
            stroke="currentColor"
            opacity="0.2"
          />

          {/* Existing quality line */}
          <polyline
            points={linePoints}
            fill="none"
            stroke="currentColor"
            strokeWidth="4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Quality points */}
          {points.map((point, index) => (
            <circle
              key={index}
              cx={point.x}
              cy={point.y}
              r={point.isAnomaly ? 8 : 5}
              fill="currentColor"
            />
          ))}

          {/* Anomaly markers */}
          {points
            .filter((point) => point.isAnomaly)
            .map((point, index) => (
              <text
                key={`anomaly-${index}`}
                x={point.x}
                y={point.y - 12}
                textAnchor="middle"
                fontSize="18"
                fontWeight="bold"
              >
                ⚠
              </text>
            ))}

        </svg>

        <div className="chart-label top">
          100%
        </div>

        <div className="chart-label middle">
          50%
        </div>

        <div className="chart-label bottom">
          0%
        </div>

      </div>

      <div className="quality-history">
        {history.slice(-5).map((item, index) => (
          <div
            key={index}
            className="quality-point"
          >
            <strong>
              {Number(item.score) || 0}%
            </strong>

            <span>
              {item.timestamp
                ? new Date(
                    item.timestamp
                  ).toLocaleTimeString()
                : ""}
            </span>
          </div>
        ))}
      </div>

    </section>
  );
}

export default QualityTrend;
