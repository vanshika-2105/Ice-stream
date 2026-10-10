function ObservabilityOverview({
  metrics,
  pipelineMetrics,
  anomaly,
  qualityProfile,
  alerts = [],
  systemHealth,
  websocketStatus,
}) {
  // =========================
  // SAFE NUMBER
  // =========================

  const safeNumber = (value, fallback = 0) => {
    const number = Number(value);

    return Number.isFinite(number)
      ? number
      : fallback;
  };

  // =========================
  // QUALITY
  // =========================

  const qualityScore = safeNumber(
    metrics?.quality_score
  );

  const validEvents = safeNumber(
    metrics?.valid_events
  );

  const invalidEvents = safeNumber(
    metrics?.invalid_events
  );

  // =========================
  // PIPELINE
  // =========================

  const pipelineEvents = safeNumber(
    pipelineMetrics?.events
  );

  const throughput = safeNumber(
    pipelineMetrics?.throughput
  );

  const avgLatency = safeNumber(
    pipelineMetrics?.avg_latency
  );

  const dlqRate = safeNumber(
    pipelineMetrics?.dlq_rate
  );

  // =========================
  // STATUS
  // =========================

  const qualityStatus =
    metrics?.status || "UNKNOWN";

  const pipelineStatus =
    pipelineMetrics?.status || "UNKNOWN";

  // =========================
  // OVERALL STATUS
  // =========================

  let overallStatus = "HEALTHY";

  if (
    qualityStatus === "CRITICAL" ||
    pipelineStatus === "CRITICAL" ||
    anomaly?.is_anomaly === true
  ) {
    overallStatus = "CRITICAL";
  } else if (
    qualityStatus === "DEGRADED" ||
    qualityStatus === "WARNING" ||
    pipelineStatus === "DEGRADED"
  ) {
    overallStatus = "DEGRADED";
  }

  const getStatusIcon = (status) => {
    if (status === "CRITICAL") {
      return "🚨";
    }

    if (
      status === "DEGRADED" ||
      status === "WARNING"
    ) {
      return "⚠";
    }

    if (status === "HEALTHY") {
      return "●";
    }

    return "●";
  };

  // =========================
  // ANOMALY
  // =========================

  const anomalyDetected =
    anomaly?.is_anomaly === true;

  const anomalyCurrent = safeNumber(
    anomaly?.current_quality
  );

  const anomalyBaseline = safeNumber(
    anomaly?.baseline_quality
  );

  const anomalyDeviation = safeNumber(
    anomaly?.deviation
  );

  const anomalySeverity =
    anomaly?.severity ||
    (anomalyDeviation >= 10
      ? "CRITICAL"
      : "WARNING");

  // =========================
  // TOP ERRORS
  // =========================

  const errorCounts =
    qualityProfile?.error_counts || {};

  const errorEntries = Object.entries(
    errorCounts
  )
    .map(([type, count]) => ({
      type,
      count: safeNumber(count),
    }))
    .filter((item) => item.count > 0)
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  // =========================
  // RECENT ALERTS
  // =========================

  const recentAlerts = alerts
    .slice(0, 5);

  // =========================
  // SYSTEM HEALTH
  // =========================

  const system =
    systemHealth || {};

  const getSystemStatus = (name) => {
    const value =
      system[name];

    if (
      value === "HEALTHY" ||
      value === "CONNECTED"
    ) {
      return "HEALTHY";
    }

    if (
      value === "DEGRADED" ||
      value === "WARNING"
    ) {
      return "DEGRADED";
    }

    if (
      value === "CRITICAL" ||
      value === "DISCONNECTED"
    ) {
      return "CRITICAL";
    }

    return "HEALTHY";
  };

  return (
    <section className="observability-overview">

      {/* =====================================
          OVERALL STATUS
          ===================================== */}

      <div
        className={`observability-banner ${overallStatus.toLowerCase()}`}
      >
        <div>
          <h1>ICE-STREAM</h1>
          <p>Unified Observability Dashboard</p>
        </div>

        <div className="overall-status">
          <span>
            {getStatusIcon(overallStatus)}
          </span>

          <strong>
            Overall Status: {overallStatus}
          </strong>
        </div>
      </div>

      {/* =====================================
          MAIN GRID
          ===================================== */}

      <div className="observability-grid">

        {/* ===================================
            PIPELINE HEALTH
            =================================== */}

        <section className="observability-card">

          <div className="observability-card-header">
            <h2>Pipeline Health</h2>

            <span>
              {getStatusIcon(
                pipelineStatus
              )}{" "}
              {pipelineStatus}
            </span>
          </div>

          <div className="overview-metrics">

            <div>
              <span>Events Processed</span>
              <strong>
                {pipelineEvents.toLocaleString()}
              </strong>
            </div>

            <div>
              <span>Throughput</span>
              <strong>
                {throughput.toFixed(1)} EPS
              </strong>
            </div>

            <div>
              <span>Avg Latency</span>
              <strong>
                {avgLatency.toFixed(0)} ms
              </strong>
            </div>

            <div>
              <span>DLQ Rate</span>
              <strong>
                {dlqRate.toFixed(1)}%
              </strong>
            </div>

          </div>
        </section>

        {/* ===================================
            DATA QUALITY
            =================================== */}

        <section className="observability-card">

          <div className="observability-card-header">
            <h2>Data Quality</h2>

            <span>
              {getStatusIcon(
                qualityStatus
              )}{" "}
              {qualityStatus}
            </span>
          </div>

          <div className="overview-metrics">

            <div>
              <span>Quality Score</span>
              <strong>
                {qualityScore.toFixed(1)}%
              </strong>
            </div>

            <div>
              <span>Valid Events</span>
              <strong>
                {validEvents.toLocaleString()}
              </strong>
            </div>

            <div>
              <span>Invalid Events</span>
              <strong>
                {invalidEvents.toLocaleString()}
              </strong>
            </div>

          </div>
        </section>

        {/* ===================================
            ANOMALY
            =================================== */}

        <section className="observability-card">

          <div className="observability-card-header">
            <h2>Anomaly</h2>

            <span>
              {anomalyDetected
                ? "🚨 ANOMALY"
                : "✓ NORMAL"}
            </span>
          </div>

          {anomalyDetected ? (
            <div className="anomaly-overview-critical">

              <strong>
                🚨 ANOMALY DETECTED
              </strong>

              <div className="overview-metrics">

                <div>
                  <span>Current</span>
                  <strong>
                    {anomalyCurrent.toFixed(1)}%
                  </strong>
                </div>

                <div>
                  <span>Baseline</span>
                  <strong>
                    {anomalyBaseline.toFixed(1)}%
                  </strong>
                </div>

                <div>
                  <span>Deviation</span>
                  <strong>
                    {anomalyDeviation.toFixed(1)}%
                  </strong>
                </div>

                <div>
                  <span>Severity</span>
                  <strong>
                    {anomalySeverity}
                  </strong>
                </div>

              </div>
            </div>
          ) : (
            <div className="anomaly-normal">

              <strong>✓ Normal</strong>

              <div className="overview-metrics">

                <div>
                  <span>Baseline</span>
                  <strong>
                    {anomalyBaseline.toFixed(1)}%
                  </strong>
                </div>

                <div>
                  <span>Deviation</span>
                  <strong>
                    {anomalyDeviation.toFixed(1)}%
                  </strong>
                </div>

              </div>
            </div>
          )}
        </section>

        {/* ===================================
            SYSTEM HEALTH
            =================================== */}

        <section className="observability-card">

          <div className="observability-card-header">
            <h2>System Health</h2>
          </div>

          <div className="system-health-list">

            {[
              ["Kafka", "kafka"],
              ["Flink", "flink"],
              ["Iceberg", "iceberg"],
              ["Backend", "backend"],
            ].map(([label, key]) => {
              const currentStatus =
                getSystemStatus(key);

              return (
                <div
                  className="system-health-row"
                  key={key}
                >
                  <span>{label}</span>

                  <strong>
                    {getStatusIcon(
                      currentStatus
                    )}{" "}
                    {currentStatus}
                  </strong>
                </div>
              );
            })}

            <div className="system-health-row">
              <span>WebSocket</span>

              <strong>
                {websocketStatus ===
                "Connected"
                  ? "● CONNECTED"
                  : "⚠ DISCONNECTED"}
              </strong>
            </div>

          </div>
        </section>

        {/* ===================================
            DATA PROFILE
            =================================== */}

        <section className="observability-card observability-profile">

          <div className="observability-card-header">
            <h2>Data Profile</h2>
          </div>

          <div className="top-error-overview">

            <span>Top Error</span>

            <strong>
              {errorEntries.length > 0
                ? errorEntries[0].type
                : "No errors detected"}
            </strong>

          </div>

          <h3>Error Distribution</h3>

          {errorEntries.length === 0 ? (
            <p>
              No profiling data available
            </p>
          ) : (
            <div className="overview-error-list">

              {errorEntries.map((item) => (
                <div
                  className="overview-error-row"
                  key={item.type}
                >
                  <span>{item.type}</span>
                  <strong>
                    {item.count}
                  </strong>
                </div>
              ))}

            </div>
          )}
        </section>

      </div>

      {/* =====================================
          RECENT ALERTS
          ===================================== */}

      <section className="observability-card recent-alerts">

        <div className="observability-card-header">
          <h2>Recent Alerts</h2>

          <span>
            Latest {recentAlerts.length}
          </span>
        </div>

        {recentAlerts.length === 0 ? (
          <p>No recent alerts</p>
        ) : (
          <div className="recent-alert-list">

            {recentAlerts.map(
              (item, index) => {

                const alertTime =
                  item.timestamp
                    ? new Date(
                        item.timestamp
                      ).toLocaleTimeString()
                    : "--:--";

                const alertType =
                  item.type ||
                  "SYSTEM";

                return (
                  <div
                    className="recent-alert-row"
                    key={`${alertTime}-${index}`}
                  >
                    <span>
                      {alertTime}
                    </span>

                    <strong>
                      {alertType}
                    </strong>

                    <span>
                      {item.reason ||
                        item.message ||
                        "Alert detected"}
                    </span>
                  </div>
                );
              }
            )}

          </div>
        )}

      </section>

    </section>
  );
}

export default ObservabilityOverview;
