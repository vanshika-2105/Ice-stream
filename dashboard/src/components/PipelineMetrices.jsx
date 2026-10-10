 function PipelineMetrics({
  metrics,
  throughputHistory = [],
  latencyHistory = [],
  pipelineRecovered = false,
}) {
  // =====================================================
  // NO DATA
  // =====================================================

  if (!metrics) {
    return (
      <section className="pipeline-metrics">
        <div className="pipeline-metrics-header">
          <h2>Pipeline Performance</h2>
          <span>Live Metrics</span>
        </div>

        <p className="pipeline-metrics-empty">
          No pipeline metrics available
        </p>
      </section>
    );
  }

  // =====================================================
  // SAFE NUMBER
  // =====================================================

  const safeNumber = (value) => {
    const number = Number(value);

    return Number.isFinite(number)
      ? number
      : 0;
  };

  // =====================================================
  // METRIC VALUES
  // =====================================================

  const events = safeNumber(
    metrics.events ??
      metrics.total_events
  );

  const throughput = safeNumber(
    metrics.throughput
  );

  const avgLatency = safeNumber(
    metrics.avg_latency
  );

  const dlqRate = safeNumber(
    metrics.dlq_rate
  );

  const status =
    metrics.status || "UNKNOWN";

  const statusClass =
    status.toLowerCase();

  // =====================================================
  // CHART POINTS
  // =====================================================

  const getChartData = (
    history,
    width = 600,
    height = 180
  ) => {
    if (!history.length) {
      return {
        points: "",
        circles: [],
      };
    }

    const values = history.map((item) =>
      safeNumber(item.value)
    );

    const maxValue = Math.max(
      ...values,
      1
    );

    const minValue = Math.min(
      ...values,
      0
    );

    const range =
      maxValue - minValue || 1;

    const circles = values.map(
      (value, index) => {
        const x =
          history.length === 1
            ? width / 2
            : (index /
                (history.length - 1)) *
              width;

        const y =
          height -
          ((value - minValue) /
            range) *
            height;

        return {
          x,
          y,
        };
      }
    );

    const points = circles
      .map(
        (point) =>
          `${point.x},${point.y}`
      )
      .join(" ");

    return {
      points,
      circles,
    };
  };

  const throughputChart =
    getChartData(
      throughputHistory
    );

  const latencyChart =
    getChartData(
      latencyHistory
    );

  // =====================================================
  // UI
  // =====================================================

  return (
    <section className="pipeline-metrics">

      {/* HEADER */}

      <div className="pipeline-metrics-header">
        <h2>
          Pipeline Performance
        </h2>

        <span>
          Live Metrics
        </span>
      </div>

      {/* METRIC CARDS */}

      <div className="pipeline-metric-cards">

        <div className="pipeline-metric-card">
          <span>Events</span>

          <strong>
            {events.toLocaleString()}
          </strong>
        </div>

        <div className="pipeline-metric-card">
          <span>
            Throughput
          </span>

          <strong>
            {throughput.toFixed(1)}
            {" / sec"}
          </strong>
        </div>

        <div className="pipeline-metric-card">
          <span>
            Avg Latency
          </span>

          <strong>
            {avgLatency.toFixed(0)}
            {" ms"}
          </strong>
        </div>

        <div className="pipeline-metric-card">
          <span>
            DLQ Rate
          </span>

          <strong>
            {dlqRate.toFixed(1)}
            {"%"}
          </strong>
        </div>

      </div>

      {/* PIPELINE STATUS */}

      <div
        className={`pipeline-health ${statusClass}`}
      >
        <h3>
          Pipeline Status
        </h3>

        <strong>
          {status === "HEALTHY" &&
            "● "}

          {status === "DEGRADED" &&
            "⚠ "}

          {status === "CRITICAL" &&
            "🚨 "}

          {status}
        </strong>
      </div>

      {/* PIPELINE DEGRADED */}

      {status === "DEGRADED" && (
        <div className="pipeline-degraded-alert">

          <h3>
            ⚠ PIPELINE DEGRADED
          </h3>

          <p>
            Average latency exceeded
            threshold.
          </p>

        </div>
      )}

      {/* PIPELINE RECOVERED */}

      {pipelineRecovered && (
        <div className="pipeline-recovery-alert">

          <h3>
            ✓ PIPELINE RECOVERED
          </h3>

        </div>
      )}

      {/* =================================================
          THROUGHPUT CHART
          ================================================= */}

      <div className="pipeline-chart-card">

        <h3>
          Events/sec
        </h3>

        {throughputHistory.length ===
        0 ? (
          <p className="pipeline-chart-empty">
            No throughput data
            available
          </p>
        ) : (
          <svg
            className="pipeline-chart"
            viewBox="0 0 600 180"
            preserveAspectRatio="none"
          >

            <polyline
              points={
                throughputChart.points
              }
              fill="none"
              stroke="currentColor"
              strokeWidth="3"
              vectorEffect="non-scaling-stroke"
            />

            {throughputChart.circles.map(
              (point, index) => (
                <circle
                  key={`throughput-${index}`}
                  cx={point.x}
                  cy={point.y}
                  r="4"
                  fill="currentColor"
                />
              )
            )}

          </svg>
        )}

      </div>

      {/* =================================================
          LATENCY CHART
          ================================================= */}

      <div className="pipeline-chart-card">

        <h3>
          Average Latency
        </h3>

        {latencyHistory.length ===
        0 ? (
          <p className="pipeline-chart-empty">
            No latency data
            available
          </p>
        ) : (
          <svg
            className="pipeline-chart"
            viewBox="0 0 600 180"
            preserveAspectRatio="none"
          >

            <polyline
              points={
                latencyChart.points
              }
              fill="none"
              stroke="currentColor"
              strokeWidth="3"
              vectorEffect="non-scaling-stroke"
            />

            {latencyChart.circles.map(
              (point, index) => (
                <circle
                  key={`latency-${index}`}
                  cx={point.x}
                  cy={point.y}
                  r="4"
                  fill="currentColor"
                />
              )
            )}

          </svg>
        )}

      </div>

    </section>
  );
}

export default PipelineMetrics;
