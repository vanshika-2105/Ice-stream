  import { useEffect, useState } from "react";

import Header from "./components/Header";
import Pipeline from "./components/Pipeline";
import StatusCard from "./components/StatusCard";
import AlertPanel from "./components/AlertPanel";
import MetricsCard from "./components/MetricsCard";
import QualityScore from "./components/QualityScore";
import useWebSocket from "./hooks/useWebSocket";
import InvalidEventRate from "./components/InvalidEventRate";
import ErrorStatistics from "./components/ErrorStatistics";
import TopError from "./components/TopError";
import PipelineStatus from "./components/PipelineStatus";
import QualityTrend from "./components/QualityTrend";
import DLQStatus from "./components/DLQStatus";
import AlertHistory from "./components/AlertHistory";
import QualityAnomaly from "./components/QualityAnomaly";

import "./App.css";

function App() {
  // =========================
  // WEBSOCKET - LIVE UPDATES
  // =========================
  const { status, latestMessage } = useWebSocket(
    "ws://127.0.0.1:8000/ws/alerts"
  );

  // =========================
  // METRICS STATE
  // =========================
  const [metrics, setMetrics] = useState({
    total_events: 0,
    valid_events: 0,
    invalid_events: 0,
    quality_score: 0,
    invalid_event_rate: 0,
    error_counts: {},
    status: "CRITICAL",
  });

  // =========================
  // ALERT STATE
  // =========================
  const [alert, setAlert] = useState(null);
  const [alerts, setAlerts] = useState([]);

  // =========================
  // ANOMALY STATE
  // =========================
  const [anomaly, setAnomaly] = useState(null);

  // =========================
  // QUALITY HISTORY
  // =========================
  const [qualityHistory, setQualityHistory] = useState([]);

  // =========================
  // LAST UPDATED
  // =========================
  const [lastUpdated, setLastUpdated] = useState(null);

  // =====================================================
  // STEP 1: FETCH INITIAL QUALITY STATUS + ALERT HISTORY
  // =====================================================
  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        // Get current quality status
        const statusResponse = await fetch(
          "http://127.0.0.1:8000/quality/status"
        );

        if (statusResponse.ok) {
          const statusData = await statusResponse.json();

          console.log("Initial quality status:", statusData);

          setMetrics((prev) => ({
            ...prev,
            ...statusData,
          }));

          if (statusData.quality_score !== undefined) {
            setQualityHistory([
              {
                score: statusData.quality_score,
                timestamp: new Date().toISOString(),
              },
            ]);
          }

          setLastUpdated(new Date());
        }

        // Get alert history
        const alertsResponse = await fetch(
          "http://127.0.0.1:8000/alerts"
        );

        if (alertsResponse.ok) {
          const alertsData = await alertsResponse.json();

          console.log("Initial alerts:", alertsData);

          // Support both:
          // [ ...alerts ]
          // { alerts: [ ...alerts ] }
          const alertList = Array.isArray(alertsData)
            ? alertsData
            : alertsData.alerts || [];

          setAlerts(alertList.slice(0, 10));

          if (alertList.length > 0) {
            setAlert(alertList[0]);
          }
        }
      } catch (error) {
        console.error(
          "Failed to fetch initial dashboard data:",
          error
        );
      }
    };

    fetchInitialData();
  }, []);

  // =====================================================
  // STEP 2: HANDLE LIVE WEBSOCKET MESSAGES
  // =====================================================
  useEffect(() => {
    if (!latestMessage) return;

    console.log(
      "Dashboard received:",
      latestMessage
    );

    // -------------------------
    // QUALITY METRICS
    // -------------------------
    if (latestMessage.type === "QUALITY_METRICS") {
      setMetrics((prev) => ({
        ...prev,
        ...latestMessage,
      }));

      // Add quality score to history
      if (latestMessage.quality_score !== undefined) {
        setQualityHistory((prev) =>
          [
            ...prev,
            {
              score: latestMessage.quality_score,
              timestamp:
                latestMessage.timestamp ||
                new Date().toISOString(),
            },
          ].slice(-50)
        );
      }

      // Update last updated time
      setLastUpdated(new Date());
    }

    // -------------------------
    // QUALITY ANOMALY
    // -------------------------
    if (latestMessage.type === "QUALITY_ANOMALY") {
      setAnomaly(latestMessage);
    }

    // -------------------------
    // QUALITY ALERT
    // -------------------------
    if (latestMessage.type === "QUALITY_ALERT") {
      setAlert(latestMessage);

      setAlerts((prev) =>
        [latestMessage, ...prev].slice(0, 10)
      );
    }

    // -------------------------
    // QUALITY RECOVERY
    // -------------------------
    if (latestMessage.type === "QUALITY_RECOVERY") {
      setAlert(latestMessage);

      setAlerts((prev) =>
        [latestMessage, ...prev].slice(0, 10)
      );

      // If recovery contains quality score,
      // update current metrics
      if (latestMessage.quality_score !== undefined) {
        setMetrics((prev) => ({
          ...prev,
          quality_score:
            latestMessage.quality_score,
          status:
            latestMessage.status ||
            "HEALTHY",
        }));

        setQualityHistory((prev) =>
          [
            ...prev,
            {
              score:
                latestMessage.quality_score,
              timestamp:
                latestMessage.timestamp ||
                new Date().toISOString(),
            },
          ].slice(-50)
        );

        setLastUpdated(new Date());
      }
    }
  }, [latestMessage]);

  // =====================================================
  // UI
  // =====================================================
  return (
    <main className="app">

      {/* Header */}
      <Header />

      {/* Overall Pipeline Status */}
      <PipelineStatus
        status={metrics.status}
      />

      {/* Pipeline */}
      <Pipeline />

      {/* Last Updated */}
      <section className="last-updated">
        <p>
          <strong>Last updated:</strong>{" "}
          {lastUpdated
            ? lastUpdated.toLocaleTimeString()
            : "Waiting for data..."}
        </p>
      </section>

      {/* Event Metrics */}
      <MetricsCard
        total={metrics.total_events}
        valid={metrics.valid_events}
        invalid={metrics.invalid_events}
      />

      {/* DLQ */}
      <DLQStatus
        invalidEvents={metrics.invalid_events}
      />

      {/* Invalid Event Rate */}
      <InvalidEventRate
        rate={metrics.invalid_event_rate}
      />

      {/* Error Statistics */}
      <ErrorStatistics
        errors={metrics.error_counts}
      />

      {/* Top Error */}
      <TopError
        errors={metrics.error_counts}
      />

      {/* Quality Score */}
      <QualityScore
        score={metrics.quality_score}
      />

     {/* Quality Trend */}
<QualityTrend
  history={qualityHistory}
/>

{/* Quality Anomaly */}
<QualityAnomaly
  anomaly={anomaly}
/>

{/* Existing Status Card */}
<StatusCard />

      {/* WebSocket Status */}
      <section className="websocket-card">
        <h2>WebSocket</h2>

        <p>
          ● {status}
        </p>

        {status === "Disconnected" && (
          <p>
            ⚠ Unable to connect to backend
          </p>
        )}

        {!latestMessage &&
          status === "Connected" && (
            <p>Loading metrics...</p>
          )}
      </section>

      {/* Current Alert + Recent Alerts */}
      <AlertPanel
        alert={alert}
        alerts={alerts}
      />

      {/* Alert History */}
      <AlertHistory
        alerts={alerts}
      />

    </main>
  );
}

export default App;
