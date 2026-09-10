 function ErrorStatistics({ errors = {} }) {
  const entries = Object.entries(errors);

  return (
    <section className="error-statistics">
      <h2>Error Statistics</h2>

      {entries.length === 0 ? (
        <p>No errors recorded.</p>
      ) : (
        entries.map(([error, count]) => (
          <div className="error-row" key={error}>
            <span>{error}</span>
            <strong>{count}</strong>
          </div>
        ))
      )}
    </section>
  );
}

export default ErrorStatistics;
