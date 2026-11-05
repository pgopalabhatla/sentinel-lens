import matplotlib.pyplot as plt
import pandas as pd
from sentinel_sim.replay import DataSource

ds = DataSource("data")
s = ds.query("kafka_consumergroup_lag|orders-etl", start="2026-10-08", end="2026-10-10")
s.plot(figsize=(10, 4), legend=False)
plt.axvline(pd.Timestamp("2026-10-09 14:00", tz="UTC"), color="red", linestyle="--")
plt.title("orders-etl consumer lag (red line = fraud-api deploy)")
plt.show()