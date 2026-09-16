function InvalidEventRate({ rate = 0 }) {
  return (
    <section className="invalid-rate-card">
      <h2>Invalid Event Rate</h2>
      <p>{rate}%</p>
    </section>
  );
}

export default InvalidEventRate;
