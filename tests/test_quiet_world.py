"""The quiet world (sentinel_stream/quiet_world.py) must be quiet, and must not overwrite a dataset."""
import json

import pandas as pd
import pytest

from sentinel_stream import quiet_world


@pytest.fixture(scope="module")
def quiet(tmp_path_factory):
    out = tmp_path_factory.mktemp("quiet") / "data"
    quiet_world.main(["--days", "8", "--seed", "3", "--out", str(out)])
    return out


def test_quiet_world_writes_empty_labels_and_no_changes(quiet):
    assert json.loads((quiet / "labels/incidents.json").read_text()) == []
    windows = pd.read_csv(quiet / "labels/anomaly_windows.csv")
    assert len(windows) == 0 and list(windows.columns) == quiet_world.ANOMALY_WINDOW_COLUMNS
    assert (quiet / "changes/changes.jsonl").read_text() == ""
    topology = json.loads((quiet / "catalog/topology.json").read_text())
    assert topology["splits"] == {}                       # the splits belong to the scenario calendar
    assert topology["time_range"] == {"start": "2026-09-28T00:00:00Z", "end": "2026-10-06T00:00:00Z", "step": "1m"}


def test_quiet_world_has_the_same_series_and_a_reading_every_minute(quiet):
    from sentinel_sim.replay import DataSource, load_labels

    ds = DataSource(quiet)
    m = ds.query(list(ds.series().series_id), start="2026-09-28", end="2026-10-06")
    assert m.shape == (8 * 1440, 164)
    assert len(ds.job_runs(window="30D")) > 8 * 24        # the DAGs still run on schedule
    incidents, windows = load_labels(quiet)               # the scorer's loader reads the empty labels
    assert incidents == [] and windows.empty
    # nothing that only a scenario does: no restarts, no under-replicated partitions
    restarts = m[[c for c in m.columns if c.startswith("kube_pod_container_status_restarts_total|")]]
    assert (restarts.ffill().diff().fillna(0) == 0).all().all()
    assert (m["kafka_under_replicated_partitions|kafka-ingest"] == 0).all()


def test_quiet_world_needs_at_least_8_days(tmp_path):
    with pytest.raises(SystemExit, match="at least 8"):
        quiet_world.main(["--days", "7", "--out", str(tmp_path / "x")])
    assert not (tmp_path / "x").exists()


def test_quiet_world_refuses_to_overwrite_a_dataset_with_splits(tmp_path):
    out = tmp_path / "data"
    (out / "catalog").mkdir(parents=True)
    (out / "catalog/topology.json").write_text(json.dumps({"splits": {"test": ["a", "b"]}}))
    (out / "keep.txt").write_text("evaluated")
    with pytest.raises(SystemExit, match="not overwriting"):
        quiet_world.main(["--days", "8", "--out", str(out)])
    assert (out / "keep.txt").read_text() == "evaluated"


def test_quiet_world_may_overwrite_a_quiet_world(quiet):
    quiet_world.main(["--days", "8", "--seed", "3", "--out", str(quiet)])
    assert json.loads((quiet / "catalog/topology.json").read_text())["splits"] == {}
