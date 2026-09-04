function MetricsCard({ total, valid, invalid }) {
  return (
    <section className="metrics-grid">
      <div className="metric-card">
        <h3>Total Events</h3>
        <p>{total}</p>
      </div>

      <div className="metric-card">
        <h3>Valid Events</h3>
        <p>{valid}</p>
      </div>

      <div className="metric-card">
        <h3>Invalid Events</h3>
        <p>{invalid}</p>
      </div>
    </section>
  );
}

export default MetricsCard;
