 function QualityScore({ score = 0 }) {
  let status = "CRITICAL";

  if (score >= 95) {
    status = "HEALTHY";
  } else if (score >= 90) {
    status = "WARNING";
  }

  return (
    <section className="quality-score">
      <h2>Quality Score</h2>

      <div className="score">
        {score}%
      </div>

      <div className="quality-status">
        Status: {status}
      </div>
    </section>
  );
}

export default QualityScore;
