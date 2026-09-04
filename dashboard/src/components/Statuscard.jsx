 function StatusCard() {
  return (
    <section className="status-card">
      <h2>Pipeline Status</h2>

      <div className="status-row">
        <span>Kafka</span>
        <span>Online</span>
      </div>

      <div className="status-row">
        <span>Flink</span>
        <span>Online</span>
      </div>

      <div className="status-row">
        <span>Iceberg</span>
        <span>Online</span>
      </div>

      <div className="status-row">
        <span>Backend</span>
        <span>Online</span>
      </div>
    </section>
  );
}

export default StatusCard;
