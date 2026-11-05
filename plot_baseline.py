import matplotlib.pyplot as plt
from sentinel_sim.replay import DataSource

sid = "kafka_consumergroup_lag|orders-etl"
ds = DataSource("data")

hist = ds.query(sid, start="2026-09-28", end="2026-10-08")[sid]   # quiet history
base = hist.groupby(hist.index.hour).median()                     # typical lag per hour of day

day = ds.query(sid, start="2026-10-09", end="2026-10-10")[sid]
expected = day.index.hour.map(base)

plt.figure(figsize=(10, 4))
plt.plot(day.index, day.values, label="actual")
plt.plot(day.index, expected, label="expected (median of same hour, Sep 28-Oct 7)")
plt.yscale("log")   # log scale so normal and the spike are both visible
plt.legend(); plt.title("Actual vs. expected lag on Oct 9"); plt.show()