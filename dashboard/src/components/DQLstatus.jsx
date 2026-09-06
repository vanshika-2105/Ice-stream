function DLQStatus({ invalidEvents = 0 }) {
  const hasErrors = invalidEvents > 0;

  return (
    <section className="dlq-status">
      <h2>Dead Letter Queue</h2>

      <p>
        <strong>Invalid Events:</strong> {invalidEvents}
      </p>

      <p>
        DLQ{" "}
        {hasErrors ? "● Active" : "● No Errors"}
      </p>
    </section>
  );
}

export default DLQStatus;
