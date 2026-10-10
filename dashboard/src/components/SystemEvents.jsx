 
function SystemEvents({ events = [] }) {
  const formatTime = (timestamp) => {
    if (!timestamp) return "Time unavailable";

    const date = new Date(timestamp);

    if (Number.isNaN(date.getTime())) {
      return "Time unavailable";
    }

    return date.toLocaleString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const getEventClass = (type) => {
    switch (String(type).toUpperCase()) {
      case "FAILED":
      case "DISCONNECTED":
        return "system-event-failed";

      case "DEGRADED":
        return "system-event-degraded";

      case "RECOVERED":
      case "CONNECTED":
      case "HEALTHY":
        return "system-event-recovered";

      default:
        return "";
    }
  };

  const getEventLabel = (type) => {
    switch (String(type).toUpperCase()) {
      case "FAILED":
        return "FAILURE";
      case "RECOVERED":
        return "RECOVERED";
      case "DISCONNECTED":
        return "CONNECTION LOST";
      case "CONNECTED":
        return "CONNECTION RESTORED";
      case "DEGRADED":
        return "DEGRADED";
      case "HEALTHY":
        return "HEALTHY";
      default:
        return String(type || "EVENT").toUpperCase();
    }
  };

  return (
    <section className="system-events">
      <div className="system-events-header">
        <div>
          <h2>Recovery Timeline</h2>
          <span>Failures, connection changes, and recoveries</span>
        </div>
      </div>

      {events.length === 0 ? (
        <p className="system-events-empty">
          No system events detected yet.
        </p>
      ) : (
        <div className="system-events-list">
          {events.map((event, index) => (
            <div
              className="system-event-row"
              key={`${event.timestamp}-${event.component}-${event.type}-${index}`}
            >
              <span className="system-event-time">
                {formatTime(event.timestamp)}
              </span>

              <div className="system-event-details">
                <div className="system-event-title">
                  <span className="system-event-component">
                    {event.component || "System"}
                  </span>

                  <strong className={getEventClass(event.type)}>
                    {getEventLabel(event.type)}
                  </strong>
                </div>

                {event.message && (
                  <p className="system-event-message">
                    {event.message}
                  </p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default SystemEvents;
