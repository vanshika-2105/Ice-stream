function QualityTrend({ history = [] }) {
  return (
    <section className="quality-trend">
      <h2>Quality Trend</h2>

      {history.length === 0 ? (
        <p>No quality history available</p>
      ) : (
        <div className="quality-history">
          {history.map((item, index) => (
            <div
              key={index}
              className="quality-point"
            >
              <strong>{item.score}%</strong>

              <span>
                {item.timestamp
                  ? new Date(item.timestamp).toLocaleTimeString()
                  : ""}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default QualityTrend;
