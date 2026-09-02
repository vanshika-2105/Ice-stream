 function QualityScore({ score }) {
  let status = "HEALTHY";

  if (score < 95 && score >= 90) {
    status = "WARNING";
  } else if (score < 90) {
    status = "CRITICAL";
  }

  return (
    <section className="quality-score">
      <h2>Quality Score</h2>

      <div className="score">
        {score}%
      </div>

      <p>{status}</p>
    </section>
  );
}

export default QualityScore;
