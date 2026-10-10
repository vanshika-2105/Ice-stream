 import { useState } from "react";

function AlertHistory({ alerts = [] }) {
  const [filter, setFilter] = useState("ALL");

  const filteredAlerts = alerts.filter((item) => {
    if (filter === "ALL") {
      return true;
    }

    if (filter === "ANOMALY") {
      return (
        item.type === "QUALITY_ANOMALY" ||
        item.type === "QUALITY_ANOMALY_RECOVERY"
      );
    }

    if (filter === "QUALITY") {
      return (
        item.type === "QUALITY_ALERT" ||
        item.type === "QUALITY_RECOVERY" ||
        item.type === "QUALITY_ANOMALY" ||
        item.type === "QUALITY_ANOMALY_RECOVERY"
      );
    }

    if (filter === "SYSTEM") {
      return (
        item.type === "SYSTEM_ALERT" ||
        item.type === "SYSTEM_RECOVERY"
      );
    }

    return true;
  });

  return (
    <section className="alert-history">
      <h2>Alert History</h2>

      {/* Filters */}
      <div className="alert-filters">
        <button
          className={filter === "ALL" ? "active" : ""}
          onClick={() => setFilter("ALL")}
        >
          All
        </button>

        <button
          className={filter === "QUALITY" ? "active" : ""}
          onClick={() => setFilter("QUALITY")}
        >
          Quality
        </button>

        <button
          className={filter === "ANOMALY" ? "active" : ""}
          onClick={() => setFilter("ANOMALY")}
        >
          Anomaly
        </button>

        <button
          className={filter === "SYSTEM" ? "active" : ""}
          onClick={() => setFilter("SYSTEM")}
        >
          System
        </button>
      </div>

      {/* Alert List */}
      {filteredAlerts.length === 0 ? (
        <p>No alerts found</p>
      ) : (
        <div className="alert-history-list">
          {filteredAlerts.map((item, index) => (
            <div
              key={`${item.timestamp || "alert"}-${index}`}
              className="alert-history-item"
            >
              <div>
                <strong>
                  {item.type === "QUALITY_ANOMALY"
                    ? " ANOMALY"
                    : item.type ===
                      "QUALITY_ANOMALY_RECOVERY"
                    ? " ANOMALY RECOVERY"
                    : item.type ===
                      "SYSTEM_ALERT"
                    ? " SYSTEM"
                    : item.type ===
                      "SYSTEM_RECOVERY"
                    ? " SYSTEM RECOVERY"
                    : item.severity ===
                      "CRITICAL"
                    ? " CRITICAL"
                    : " WARNING"}
                </strong>
              </div>

              <div>
                {item.timestamp
                  ? new Date(
                      item.timestamp
                    ).toLocaleTimeString()
                  : "N/A"}
              </div>

              {item.quality_score !==
                undefined && (
                <div>
                  Quality:{" "}
                  {item.quality_score}%
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
