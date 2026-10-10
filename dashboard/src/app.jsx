  import { useEffect, useState } from "react";

import Pipeline from "./components/Pipeline";
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
import AnomalyHistory from "./components/AnomalyHistory";
import DataQualityProfile from "./components/DataQualityProfile";
import PipelineMetrics from "./components/PipelineMetrics";
import ObservabilityOverview from "./components/ObservabilityOverview";
import LiveMetrics from "./components/LiveMetrics";
import ComponentHealth from "./components/ComponentHealth";
import SystemEvents from "./components/SystemEvents";

import "./App.css";

function App() {
  // =====================================================
  // WEBSOCKET
  // =====================================================

  const { status, latestMessage } = useWebSocket(
    "ws://127.0.0.1:8000/ws/alerts"
  );

  const isConnected =
    String(status).toLowerCase() === "connected";
    
  // Fetch actual infrastructure status from the backend.
  useEffect(() => {
    let cancelled = false;

    const refreshSystemStatus = async () => {
      try {
        const response = await fetch("http://127.0.0.1:8000/system/status");

        if (!response.ok) {
          throw new Error(`System status request failed: ${response.status}`);
        }

        const data = await response.json();

        if (cancelled || !data.components) return;

        const statusMap = {};

        for (const [name, value] of Object.entries(data.components)) {
          const component = String(name).toLowerCase();
          const rawStatus =
            typeof value === "object" && value !== null
              ? value.status
              : value;

          if (!rawStatus) continue;

          const displayName =
            component === "kafka" ? "Kafka" :
            component === "flink" ? "Flink" :
            component === "producer" ? "Producer" :
            component === "backend" ? "Backend" :
            component === "pipeline" ? "Pipeline" :
            name;

          statusMap[displayName] =
            String(rawStatus).toUpperCase() === "DOWN"
              ? "FAILED"
              : String(rawStatus).toUpperCase() === "UP"
                ? "HEALTHY"
                : String(rawStatus).toUpperCase();
        }

        setComponentHealth((previous) => ({
          ...previous,
          ...statusMap,
        }));
      } catch (error) {
        console.error("Failed to fetch system status:", error);
      }
    };

    refreshSystemStatus();

    return () => {
      cancelled = true;
    };
  }, []);


  // =====================================================
  // NAVIGATION
  // =====================================================

  const [activeView, setActiveView] = useState("overview");

  // =====================================================
  // DATA QUALITY METRICS
  // =====================================================

  const [metrics, setMetrics] = useState({
    total_events: 0,
    valid_events: 0,
    invalid_events: 0,
    quality_score: 0,
    invalid_event_rate: 0,
    error_counts: {},
    status: "CRITICAL",
  });

  // =====================================================
  // COMPONENT HEALTH
  // =====================================================

  const [componentHealth, setComponentHealth] = useState({
    Kafka: "HEALTHY",
    Flink: "HEALTHY",
    Producer: "HEALTHY",
    Backend: "HEALTHY",
    WebSocket: "HEALTHY",
    Pipeline: "HEALTHY",
  });

  // Keep track of currently failed components.
  // This prevents recovery of one component from hiding
  // another component's active failure.

  const [failedComponents, setFailedComponents] = useState({});

  // =====================================================
  // SYSTEM EVENTS AND NOTIFICATIONS
  // =====================================================

  const [systemEvents, setSystemEvents] = useState([]);
  
  c
const failureCount = systemEvents.filter(
  (event) =>
    event.type === "FAILED" ||
    event.type === "SYSTEM_ALERT"
).length;

const recoveryCount = systemEvents.filter(
  (event) =>
    event.type === "RECOVERED" ||
    event.type === "SYSTEM_RECOVERY"
).length;

const lastRecovery = systemEvents.find(
  (event) =>
    event.type === "RECOVERED" ||
    event.type === "SYSTEM_RECOVERY"
);const [systemAlert, setSystemAlert] = useState(null);
  const [systemRecovery, setSystemRecovery] = useState(null);
   
  // =====================================================
  // PIPELINE PERFORMANCE
  // =====================================================

  const [pipelineMetrics, setPipelineMetrics] = useState(null);
  const [throughputHistory, setThroughputHistory] = useState([]);
  const [latencyHistory, setLatencyHistory] = useState([]);
  const [pipelineRecovered, setPipelineRecovered] = useState(false);

  // =====================================================
  // ALERTS
  // =====================================================

  const [alert, setAlert] = useState(null);
  const [alerts, setAlerts] = useState([]);

  // =====================================================
  // ANOMALIES
  // =====================================================

  const [anomaly, setAnomaly] = useState(null);
  const [anomalyHistory, setAnomalyHistory] = useState([]);
  const [anomalyCount, setAnomalyCount] = useState(0);
  const [anomalyRecovered, setAnomalyRecovered] = useState(false);

  // =====================================================
  // DATA QUALITY PROFILE
  // =====================================================

  const [qualityProfile, setQualityProfile] = useState(null);

  // =====================================================
  // QUALITY HISTORY
  // =====================================================

  const [qualityHistory, setQualityHistory] = useState([]);

  // =====================================================
  // LIVE METRICS
  // =====================================================

  const [liveMetrics, setLiveMetrics] = useState(null);
  const [liveThroughputHistory, setLiveThroughputHistory] = useState([]);
  const [liveLastUpdated, setLiveLastUpdated] = useState(null);
  
const [liveDataResumed, setLiveDataResumed] = useState(false);
const [hasBeenDisconnected, setHasBeenDisconnected] = useState(false);


  // =====================================================
  // OBSERVABILITY OVERVIEW
  // =====================================================

  const [observabilityOverview, setObservabilityOverview] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  // =====================================================
  // HELPERS
  // =====================================================

  const normalizeComponent = (message) => {
    const rawComponent =
      message?.component ||
      message?.service ||
      message?.source ||
      "Backend";

    const componentMap = {
      kafka: "Kafka",
      flink: "Flink",
      producer: "Producer",
      backend: "Backend",
      websocket: "WebSocket",
      pipeline: "Pipeline",
    };

    const key = String(rawComponent).trim().toLowerCase();

    return componentMap[key] || rawComponent;
  };

  const addAlert = (message) => {
    setAlert(message);
    setAlerts((previous) => [message, ...previous].slice(0, 10));
  };

  const addSystemEvent = (event) => {
    setSystemEvents((previous) => [event, ...previous].slice(0, 20));
  };

  const getTimestamp = (message) =>
    message?.timestamp || new Date().toISOString();

  // =====================================================
  // INITIAL DATA FETCH
  // =====================================================

  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const pipelineResponse = await fetch(
          "http://127.0.0.1:8000/pipeline/status"
        );

        if (pipelineResponse.ok) {
          const data = await pipelineResponse.json();

          setPipelineMetrics(data);

          if (data.throughput !== undefined) {
            setThroughputHistory([
              {
                value: Number(data.throughput),
                timestamp: data.timestamp || new Date().toISOString(),
              },
            ]);
          }

          if (data.avg_latency !== undefined) {
            setLatencyHistory([
              {
                value: Number(data.avg_latency),
                timestamp: data.timestamp || new Date().toISOString(),
              },
            ]);
          }
        }

        const statusResponse = await fetch(
          "http://127.0.0.1:8000/quality/status"
        );

        if (statusResponse.ok) {
          const data = await statusResponse.json();

          setMetrics((previous) => ({
            ...previous,
            ...data,
          }));

          if (data.quality_score !== undefined) {
            setQualityHistory([
              {
                score: Number(data.quality_score),
                timestamp: new Date().toISOString(),
              },
            ]);
          }

          setLastUpdated(new Date());
        }

        const profileResponse = await fetch(
          "http://127.0.0.1:8000/quality/profile"
        );

        if (profileResponse.ok) {
          const data = await profileResponse.json();
          setQualityProfile(data);
        }

        const alertsResponse = await fetch(
          "http://127.0.0.1:8000/alerts"
        );

        if (alertsResponse.ok) {
          const data = await alertsResponse.json();

          const alertList = Array.isArray(data)
            ? data
            : data.alerts || [];

          setAlerts(alertList.slice(0, 10));

          if (alertList.length > 0) {
            setAlert(alertList[0]);
          }
        }
      } catch (error) {
        console.error("Failed to fetch initial dashboard data:", error);
      }
    };

    fetchInitialData();
  }, []);

  // =====================================================
  // OBSERVABILITY OVERVIEW API
  // =====================================================

  useEffect(() => {
    const fetchOverview = async () => {
      try {
        const response = await fetch(
          "http://127.0.0.1:8000/observability/overview"
        );

        if (!response.ok) {
          throw new Error(`Overview request failed: ${response.status}`);
        }

        const data = await response.json();
        setObservabilityOverview(data);
      } catch (error) {
        console.error("Failed to fetch observability overview:", error);
      }
    };

    fetchOverview();
  }, []);

  // Refresh overview after WebSocket connects.

  useEffect(() => {
    if (!isConnected) return;

    const refreshOverview = async () => {
      try {
        const response = await fetch(
          "http://127.0.0.1:8000/observability/overview"
        );

        if (!response.ok) return;

        const data = await response.json();

        setObservabilityOverview(data);
        setLastUpdated(new Date());
      } catch (error) {
        console.error("Failed to refresh observability overview:", error);
      }
    };

    refreshOverview();
  }, [isConnected]);

  // =====================================================
  // HANDLE WEBSOCKET MESSAGES
  // =====================================================

  useEffect(() => {
    if (!latestMessage) return;

    console.log("Dashboard received:", latestMessage);

    const message = latestMessage;
    const timestamp = getTimestamp(message);

    // -----------------------------------------------------
    // OBSERVABILITY OVERVIEW
    // -----------------------------------------------------

    if (message.type === "OBSERVABILITY_OVERVIEW") {
      setObservabilityOverview(message);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // LIVE METRICS
    // Preserve the last known values when messages stop.
    // -----------------------------------------------------

    if (message.type === "LIVE_METRICS") {
      setLiveMetrics(message);

      setLiveLastUpdated(
        message.timestamp ? new Date(message.timestamp) : new Date()
      );

      const throughputValue = Number(message.throughput);

      if (Number.isFinite(throughputValue)) {
        setLiveThroughputHistory((previous) =>
          [
            ...previous,
            {
              value: throughputValue,
              timestamp: timestamp,
            },
          ].slice(-30)
        );
      }

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // PIPELINE METRICS
    // -----------------------------------------------------

    if (message.type === "PIPELINE_METRICS") {
      setPipelineMetrics(message);

      if (message.throughput !== undefined) {
        setThroughputHistory((previous) =>
          [
            ...previous,
            {
              value: Number(message.throughput),
              timestamp,
            },
          ].slice(-30)
        );
      }

      if (message.avg_latency !== undefined) {
        setLatencyHistory((previous) =>
          [
            ...previous,
            {
              value: Number(message.avg_latency),
              timestamp,
            },
          ].slice(-30)
        );
      }

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // QUALITY METRICS
    // -----------------------------------------------------

    if (message.type === "QUALITY_METRICS") {
      setMetrics((previous) => ({
        ...previous,
        ...message,
      }));

      if (message.quality_score !== undefined) {
        setQualityHistory((previous) =>
          [
            ...previous,
            {
              score: Number(message.quality_score),
              timestamp,
            },
          ].slice(-50)
        );
      }

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // DATA QUALITY PROFILE
    // -----------------------------------------------------

    if (message.type === "QUALITY_PROFILE") {
      setQualityProfile(message);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // QUALITY ANOMALY
    // -----------------------------------------------------

    if (message.type === "QUALITY_ANOMALY") {
      setAnomaly(message);
      setAnomalyRecovered(false);

      setAnomalyHistory((previous) =>
        [message, ...previous].slice(0, 10)
      );

      setAnomalyCount((previous) => previous + 1);

      addAlert(message);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // ANOMALY RECOVERY
    // -----------------------------------------------------

    if (message.type === "QUALITY_ANOMALY_RECOVERY") {
      setAnomalyRecovered(true);
      setAnomaly(message);

      addAlert(message);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // QUALITY ALERT
    // -----------------------------------------------------

    if (message.type === "QUALITY_ALERT") {
      addAlert(message);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // QUALITY RECOVERY
    // -----------------------------------------------------

    if (message.type === "QUALITY_RECOVERY") {
      addAlert(message);

      if (message.quality_score !== undefined) {
        setMetrics((previous) => ({
          ...previous,
          quality_score: message.quality_score,
          status: message.status || "HEALTHY",
        }));

        setQualityHistory((previous) =>
          [
            ...previous,
            {
              score: Number(message.quality_score),
              timestamp,
            },
          ].slice(-50)
        );
      }

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // PIPELINE DEGRADED
    // -----------------------------------------------------

    if (message.type === "PIPELINE_DEGRADED") {
      setPipelineRecovered(false);

      setPipelineMetrics((previous) => ({
        ...previous,
        ...message,
        status: message.status || "DEGRADED",
      }));

      addAlert(message);

      setComponentHealth((previous) => ({
        ...previous,
        Pipeline: "DEGRADED",
      }));

      addSystemEvent({
        type: "DEGRADED",
        component: "Pipeline",
        timestamp,
        message: message.message || "Pipeline is degraded",
      });

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // PIPELINE RECOVERY
    // -----------------------------------------------------

    if (message.type === "PIPELINE_RECOVERY") {
      setPipelineRecovered(true);

      setPipelineMetrics((previous) => ({
        ...previous,
        ...message,
        status: message.status || "HEALTHY",
      }));

      addAlert(message);

      setComponentHealth((previous) => ({
        ...previous,
        Pipeline: "HEALTHY",
      }));

      addSystemEvent({
        type: "HEALTHY",
        component: "Pipeline",
        timestamp,
        message: message.message || "Pipeline is healthy",
      });

      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // SYSTEM ALERT
    // -----------------------------------------------------

    if (message.type === "SYSTEM_ALERT") {
      const component = normalizeComponent(message);

      const reason =
        message.message ||
        message.reason ||
        `${component} is unavailable.`;

      const systemFailure = {
        ...message,
        component,
        message: reason,
        timestamp,
      };

      setComponentHealth((previous) => ({
        ...previous,
        [component]: "FAILED",
      }));

      setFailedComponents((previous) => ({
        ...previous,
        [component]: systemFailure,
      }));

      setSystemAlert(systemFailure);
      setSystemRecovery(null);

      addSystemEvent({
        type: "FAILED",
        component,
        timestamp,
        message: reason,
      });

      addAlert(systemFailure);
      setLastUpdated(new Date());
    }

    // -----------------------------------------------------
    // SYSTEM RECOVERY
    // -----------------------------------------------------

    if (message.type === "SYSTEM_RECOVERY") {
      const component = normalizeComponent(message);

      setComponentHealth((previous) => ({
        ...previous,
        [component]: "HEALTHY",
      }));

      setFailedComponents((previous) => {
        const updated = { ...previous };
        delete updated[component];
        return updated;
      });

      const recoveryMessage = {
        ...message,
        component,
        timestamp,
        message: message.message || `${component} recovered`,
      };

      setSystemAlert((previous) => {
        if (!previous || previous.component === component) {
          return null;
        }

        return previous;
      });

      setSystemRecovery(recoveryMessage);

      addSystemEvent({
        type: "RECOVERED",
        component,
        timestamp,
        message: recoveryMessage.message,
      });

      addAlert(recoveryMessage);
      setLastUpdated(new Date());
    }
  }, [latestMessage]);

  // =====================================================
  // CLEAR RECOVERY NOTIFICATION AFTER FIVE SECONDS
  // =====================================================

  useEffect(() => {
    if (!systemRecovery) return;

    const timer = setTimeout(() => {
      setSystemRecovery(null);
    }, 5000);

    return () => clearTimeout(timer);
  }, [systemRecovery]);

  // =====================================================
  // WEBSOCKET COMPONENT HEALTH
  // =====================================================

   
    
  
useEffect(() => {
  let cancelled = false;

  setComponentHealth((previous) => ({
    ...previous,
    WebSocket: isConnected ? "HEALTHY" : "FAILED",
  }));

  if (!isConnected) {
    setHasBeenDisconnected(true);
    setLiveDataResumed(false);

    setSystemEvents((previous) => {
      if (
        previous[0]?.component === "WebSocket" &&
        previous[0]?.type === "DISCONNECTED"
      ) {
        return previous;
      }

      return [
        {
          type: "DISCONNECTED",
          component: "WebSocket",
          timestamp: new Date().toISOString(),
          message: "WebSocket disconnected; last metrics are retained",
        },
        ...previous,
      ].slice(0, 20);
    });
  } else {
    setSystemEvents((previous) => {
      if (
        previous[0]?.component === "WebSocket" &&
        previous[0]?.type === "CONNECTED"
      ) {
        return previous;
      }

      return [
        {
          type: "CONNECTED",
          component: "WebSocket",
          timestamp: new Date().toISOString(),
          message: "WebSocket connection restored",
        },
        ...previous,
      ].slice(0, 20);
    });

    if (hasBeenDisconnected) {
      setLiveDataResumed(true);

      const refreshSystemStatus = async () => {
        try {
          const response = await fetch(
            "http://127.0.0.1:8000/system/status"
          );

          if (!response.ok) {
            throw new Error(`Status request failed: ${response.status}`);
          }

          const data = await response.json();

          if (cancelled || !data.components) return;

          const statusMap = {};

          for (const [name, value] of Object.entries(data.components)) {
            const rawStatus =
              typeof value === "object" && value !== null
                ? value.status
                : value;

            if (!rawStatus) continue;

            const normalizedName =
              name.charAt(0).toUpperCase() + name.slice(1);

            const status = String(rawStatus).toUpperCase();

            if (status === "UP") {
              statusMap[normalizedName] = "HEALTHY";
            } else if (status === "DOWN") {
              statusMap[normalizedName] = "FAILED";
            }
          }

          if (Object.keys(statusMap).length > 0) {
            setComponentHealth((previous) => ({
              ...previous,
              ...statusMap,
              WebSocket: "HEALTHY",
            }));
          }
        } catch (error) {
          console.error("Could not refresh component status:", error);
        }
      };

      refreshSystemStatus();
    }
  }

  return () => {
    cancelled = true;
  };
}, [isConnected, hasBeenDisconnected]);



  // =====================================================
  // OVERALL STATUS
  // =====================================================

  const qualityStatus = String(
    metrics?.status || "CRITICAL"
  ).toUpperCase();

  const pipelineStatus = String(
    pipelineMetrics?.status || "HEALTHY"
  ).toUpperCase();

  const failedComponentNames = Object.keys(failedComponents);

  const hasFailedComponent = failedComponentNames.length > 0;

  const criticalComponentNames = failedComponentNames.filter(
    (name) => name !== "WebSocket"
  );

  let overallStatus = "HEALTHY";

  if (
    qualityStatus === "CRITICAL" ||
    pipelineStatus === "CRITICAL" ||
    criticalComponentNames.length > 0 ||
    (
      anomaly?.is_anomaly === true &&
      String(anomaly?.severity || "").toUpperCase() === "CRITICAL"
    )
  ) {
    overallStatus = "CRITICAL";
  } else if (
    qualityStatus === "DEGRADED" ||
    qualityStatus === "WARNING" ||
    pipelineStatus === "DEGRADED" ||
    hasFailedComponent ||
    !isConnected ||
    (
      anomaly?.is_anomaly === true &&
      String(anomaly?.severity || "").toUpperCase() === "WARNING"
    )
  ) {
    overallStatus = "DEGRADED";
  }

  const overallStatusIcon =
    overallStatus === "HEALTHY"
      ? "â—"
      : overallStatus === "DEGRADED"
      ? "â—"
      : "â—";

  const overallStatusMessage =
    overallStatus === "CRITICAL"
      ? criticalComponentNames.length > 0
        ? `${criticalComponentNames.join(", ")} unavailable`
        : qualityStatus === "CRITICAL"
        ? "Data quality is critical"
        : "Pipeline status is critical"
      : overallStatus === "DEGRADED"
      ? !isConnected
        ? "WebSocket disconnected or system needs attention"
        : "Some monitored services need attention"
      : "All monitored components OK";

  // =====================================================
  // NAVIGATION HELPERS
  // =====================================================

  const getNavClass = (view) =>
    activeView === view ? "nav-item active" : "nav-item";

  // =====================================================
  // UI
  // =====================================================

  return (
    <main className="app-shell">
      {/* SIDEBAR */}

      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="brand-icon">IS</div>

          <div>
            <h1>ICE-STREAM</h1>
            <span>Observability</span>
          </div>
        </div>

        <div className="sidebar-section-title">
          MONITORING
        </div>

        <nav className="sidebar-nav">
          <button
            className={getNavClass("overview")}
            onClick={() => setActiveView("overview")}
          >
            <span className="nav-icon">â—«</span>
            <span>Overview</span>
          </button>

          <button
            className={getNavClass("live")}
            onClick={() => setActiveView("live")}
          >
            <span className="nav-icon">â—·</span>
            <span>Live Metrics</span>
          </button>

          <button
            className={getNavClass("health")}
            onClick={() => setActiveView("health")}
          >
            <span className="nav-icon">â™¥</span>
            <span>Component Health</span>

            {hasFailedComponent && (
              <span className="nav-alert-dot">!</span>
            )}
          </button>

          <button
            className={getNavClass("pipeline")}
            onClick={() => setActiveView("pipeline")}
          >
            <span className="nav-icon">â‡„</span>
            <span>Pipeline</span>
          </button>

          <button
            className={getNavClass("quality")}
            onClick={() => setActiveView("quality")}
          >
            <span className="nav-icon">âœ“</span>
            <span>Data Quality</span>
          </button>

          <button
            className={getNavClass("anomalies")}
            onClick={() => setActiveView("anomalies")}
          >
            <span className="nav-icon">âŒ</span>
            <span>Anomalies</span>

            {anomalyCount > 0 && (
              <span className="nav-count">{anomalyCount}</span>
            )}
          </button>

          <button
            className={getNavClass("alerts")}
            onClick={() => setActiveView("alerts")}
          >
            <span className="nav-icon">âš‘</span>
            <span>Alerts</span>

            {alerts.length > 0 && (
              <span className="nav-count">{alerts.length}</span>
            )}
          </button>
        </nav>

        <div className="sidebar-status">
          <div className="sidebar-section-title">
            SYSTEM STATUS
          </div>

          <div className="sidebar-status-row">
            <span>Backend</span>

            <strong
              className={
                componentHealth.Backend === "FAILED"
                  ? "status-offline"
                  : "status-online"
              }
            >
              {componentHealth.Backend === "FAILED"
                ? "OFFLINE"
                : "ONLINE"}
            </strong>
          </div>

          <div className="sidebar-status-row">
            <span>WebSocket</span>

            <strong
              className={
                isConnected ? "status-online" : "status-offline"
              }
            >
              {isConnected ? "LIVE" : "DISCONNECTED"}
            </strong>
          </div>
        </div>

        <div className="sidebar-footer">
          <span>ICE-STREAM</span>
          <small>Real-Time Data Platform</small>
        </div>
      </aside>

      {/* MAIN DASHBOARD */}

      <section className="dashboard-content">
        <header className="dashboard-topbar">
          <div className="main-heading">
            <h1>ICE-STREAM</h1>
            <p>Unified Observability Dashboard</p>

            <div
              className={`main-overall-status ${overallStatus.toLowerCase()}`}
            >
              {overallStatusIcon} Overall Status: {overallStatus}
              <p>{overallStatusMessage}</p>
            </div>
          </div>
        </header>

        {/* CRITICAL SYSTEM ALERT */}

        {systemAlert && (
          <div
            className="system-critical-banner"
            role="alert"
          >
            <div className="system-critical-icon">âš </div>

            <div>
              <h2>SYSTEM ALERT</h2>

              <strong>
                {systemAlert.component} is unavailable.
              </strong>

              <p>
                {systemAlert.message ||
                  "Pipeline processing may be affected."}
              </p>
            </div>
          </div>
        )}

        {/* RECOVERY NOTIFICATION */}

        {systemRecovery && (
          <div
            className="system-recovery-banner"
            role="status"
          >
            <strong>
              âœ“ {systemRecovery.component} recovered
            </strong>
          </div>
        )}

        {/* OVERVIEW */}

        {activeView === "overview" && (
          <div className="view-container">
            <div className="overview-welcome">
              <div>
                <h1>Real-Time Data Pipeline</h1>

                <p>
                  Monitor streaming quality, pipeline performance
                  and system health from one place.
                </p>
              </div>

              <div className="overview-live-badge">
                <span>â—</span>
                {isConnected ? "LIVE MONITORING" : "CONNECTION LOST"}
              </div>
            </div>

            <ObservabilityOverview
              metrics={metrics}
              pipelineMetrics={pipelineMetrics}
              anomaly={anomaly}
              qualityProfile={qualityProfile}
              alerts={alerts}
              systemHealth={observabilityOverview?.system_health}
              websocketStatus={status}
            />

            <ComponentHealth
              componentHealth={componentHealth}
              websocketStatus={status}
            />

            <SystemEvents events={systemEvents} />
            <section className="recovery-statistics">
    <h2>Recovery Summary</h2>

    <div className="recovery-statistics-grid">
      <div>
        <span>Total Failures</span>
        <strong>{failureCount}</strong>
      </div>

      <div>
        <span>Total Recoveries</span>
        <strong>{recoveryCount}</strong>
      </div>

      <div>
        <span>Last Recovery</span>
        <strong>
          {lastRecovery
            ? new Date(lastRecovery.timestamp).toLocaleString()
            : "No recovery recorded"}
        </strong>
      </div>
    </div>
  </section>


            <Pipeline />
          </div>
        )}

        {/* LIVE METRICS */}

        {activeView === "live" && (
          <div className="view-container">
            <LiveMetrics
              metrics={liveMetrics}
              throughputHistory={liveThroughputHistory}
              connectionStatus={status}
              lastUpdated={liveLastUpdated}
            />

            <div className="section-grid">
              <PipelineMetrics
                metrics={pipelineMetrics}
                throughputHistory={throughputHistory}
                latencyHistory={latencyHistory}
                pipelineRecovered={pipelineRecovered}
              />
            </div>
          </div>
        )}
        
{!isConnected && (
  <div className="live-data-warning" role="status">
    âš  Connection lost. Showing the last received metrics; values may be stale.
  </div>
)}

{isConnected && liveDataResumed && (
  <div className="live-data-resumed" role="status">
    âœ“ Live data resumed.
    <button onClick={() => setLiveDataResumed(false)}>
      Dismiss
    </button>
  </div>
)}


        {/* COMPONENT HEALTH */}

        {activeView === "health" && (
          <div className="view-container">
            <ComponentHealth
              componentHealth={componentHealth}
              websocketStatus={status}
            />

            <SystemEvents events={systemEvents} />

            <ObservabilityOverview
              metrics={metrics}
              pipelineMetrics={pipelineMetrics}
              anomaly={anomaly}
              qualityProfile={qualityProfile}
              alerts={alerts}
              systemHealth={observabilityOverview?.system_health}
              websocketStatus={status}
            />
          </div>
        )}

        {/* PIPELINE */}

        {activeView === "pipeline" && (
          <div className="view-container">
            <Pipeline />

            <PipelineStatus status={metrics.status} />

            <PipelineMetrics
              metrics={pipelineMetrics}
              throughputHistory={throughputHistory}
              latencyHistory={latencyHistory}
              pipelineRecovered={pipelineRecovered}
            />
          </div>
        )}

        {/* DATA QUALITY */}

        {activeView === "quality" && (
          <div className="view-container">
            <div className="quality-kpi-grid">
              <MetricsCard
                total={metrics.total_events}
                valid={metrics.valid_events}
                invalid={metrics.invalid_events}
              />

              <QualityScore score={metrics.quality_score} />

              <InvalidEventRate rate={metrics.invalid_event_rate} />
            </div>

            <div className="quality-dashboard-grid">
              <ErrorStatistics errors={metrics.error_counts} />

              <TopError errors={metrics.error_counts} />

              <DLQStatus invalidEvents={metrics.invalid_events} />

              <DataQualityProfile profile={qualityProfile} />
            </div>

            <QualityTrend
              history={qualityHistory}
              anomalies={anomalyHistory}
            />
          </div>
        )}

        {/* ANOMALIES */}

        {activeView === "anomalies" && (
          <div className="view-container">
            <div className="anomaly-summary-grid">
              <div className="anomaly-big-card">
                <span>ANOMALIES DETECTED</span>

                <strong>{anomalyCount}</strong>

                <small>Current monitoring period</small>
              </div>

              <QualityAnomaly anomaly={anomaly} />
            </div>

            {anomalyRecovered && (
              <section className="anomaly-recovery">
                <h2>ANOMALY RECOVERED</h2>

                <p>
                  Quality returned to expected range.
                </p>
              </section>
            )}

            <AnomalyHistory anomalies={anomalyHistory} />

            <QualityTrend
              history={qualityHistory}
              anomalies={anomalyHistory}
            />
          </div>
        )}

        {/* ALERTS */}

        {activeView === "alerts" && (
          <div className="view-container">
            <AlertPanel alert={alert} alerts={alerts} />

            <AlertHistory alerts={alerts} />

            <SystemEvents events={systemEvents} />
          </div>
        )}
      </section>
    </main>
  );
}

export default App;
