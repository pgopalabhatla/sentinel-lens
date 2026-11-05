from sentinel_sim.replay import DataSource

ds = DataSource("data")
rel = ds.relationships()
print(rel.columns.tolist())
print(len(rel))
print(rel.head(30).to_string())