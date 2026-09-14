function TopError({ errors = {} }) {
  const errorEntries = Object.entries(errors);

  if (errorEntries.length === 0) {
    return (
      <section className="top-error">
        <h2>Top Data Quality Error</h2>
        <p>No errors recorded</p>
      </section>
    );
  }

  const [topError, count] = errorEntries.reduce(
    (max, current) =>
      current[1] > max[1] ? current : max,
    errorEntries[0]
  );

  return (
    <section className="top-error">
      <h2>Top Data Quality Error</h2>

      <h3>{topError}</h3>

      <p>{count} occurrences</p>
    </section>
  );
}

export default TopError;
