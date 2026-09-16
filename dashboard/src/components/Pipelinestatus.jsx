function PipelineStatus({ status }) {
  const currentStatus = status || "UNKNOWN";

  return (
    <section className="pipeline-status">
      <h2>PIPELINE STATUS</h2>

      <div className={`pipeline-status-value ${currentStatus.toLowerCase()}`}>
        ● {currentStatus}
      </div>
    </section>
  );
}

export default PipelineStatus;
