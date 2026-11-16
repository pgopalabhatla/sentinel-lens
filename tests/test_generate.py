"""The generator: the evaluated dataset must never change, and the quiet world must be quiet.

`python -m sentinel_sim.generate --out data --seed 7` is the dataset every number in docs/ was
measured on. The `--no-incidents` option was added to the generator afterwards (for the live stream),
so the first test pins the content of every seed-7 file: if it fails, the generator no longer
reproduces the evaluated dataset, and the change that caused it must be undone, not the hashes updated.

The hashes were taken from the generator as it was before the option existed, and are equal to the
hashes of the data/ directory the test week was run on. The two .gz files are hashed after
decompression, because a gzip header holds the time the file was written.
"""
import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from sentinel_sim import generate

SEED_7_SHA256 = {
    "catalog/canonical_events.yaml": "3b0bb11522eb5c6ca6eb040f83753997dc92d078be1ba9eb848031606d8aca77",
    "catalog/mappings/airflow.yaml": "dbc45ac4c0d420fd9b8cfa676d9687f57abd23456de5f9a463223e2a238f07cc",
    "catalog/mappings/flink.yaml": "b35f85df3a19eed56386c4fb4cc83f4162280e3ff4f38715a1dcfb86b7a3fb12",
    "catalog/mappings/kafka.yaml": "7873205d5975383699cd9ce0103e8956a635cd8e6ad235f17559ba0d0679af7e",
    "catalog/mappings/spark.yaml": "0c658f7c6dea041f000468910b33f8ac26a9cf7aa8597fea34ae5eea626635ad",
    "catalog/mappings/trino.yaml": "172c7d7d4270fe3bb49e3b161fcb0bf178947a18949dd25d705ab2ee4372f23e",
    "catalog/series.csv": "78bfb556e1ff83d8a968a6d6d74af5d3ea62d73e5246f294682e716dd4c824de",
    "catalog/topology.json": "6d7577fd0a05eab5963bedbafec4e435a37be83469c9742d9f707686c9eacf1a",
    "changes/changes.jsonl": "2c22cd8c82e62e36f0c0cc233df6583e4ceec6ccf6056bce1bc1971f9bc4df37",
    "jobs/dag_runs.jsonl": "c7bbeaa4d92498205ecd85f6ddc3014d523945904a88f854ce408782a07ca2d1",
    "jobs/job_runs.csv": "865693901c085a7e5fe4b955ac274ca519cfed4f3ad8666cce6c60f85577ad4c",
    "jobs/spark_apps.jsonl": "9728e22ea2c7dce65972dc43151f0ef91e8e23f0b0f2f7498dbe853b455820dc",
    "k8s/events.jsonl": "ca49d0bb66020bfc6dba3a53c478636a4812e20c76a90e4827bc7391f5562316",
    "labels/anomaly_windows.csv": "ad2fefa494b59cfbd26abf9d13330b63b4aeb55e4a868980d35c3964757e7ee3",
    "labels/incidents.json": "7d086bb9fcc717d0b8107ad1c7e44f353a670f83b5987aee5db2c920b30a43df",
    "labels/noise_blips.csv": "5a986ddef35dbccd8d30fa3d2a38bdf797069bedc1cf8b28da9e4d41068192f9",
    "logs/logs.jsonl.gz": "cd294a1bd71d360e104f5142e40d27a6f8bb72ecddfccbca6277099f8bf13af4",
    "metrics/metrics.csv.gz": "b8070ba07cb9d8f381facfe692d0ebac5f4f9005ba8d6fb17e4d4a23383ebbbb",
}


def content_hashes(root: Path) -> dict:
    out = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            raw = p.read_bytes()
            out[p.relative_to(root).as_posix()] = hashlib.sha256(gzip.decompress(raw) if p.suffix == ".gz" else raw).hexdigest()
    return out


@pytest.mark.slow
def test_default_generation_is_byte_identical_to_the_evaluated_dataset(tmp_path):
    generate.main(["--out", str(tmp_path / "data"), "--seed", "7"])
    assert content_hashes(tmp_path / "data") == SEED_7_SHA256


@pytest.fixture(scope="module")
def quiet(tmp_path_factory):
    out = tmp_path_factory.mktemp("quiet") / "data"
    generate.main(["--no-incidents", "--days", "8", "--seed", "3", "--out", str(out)])
    return out


def test_no_incidents_writes_empty_labels_and_no_changes(quiet):
    assert json.loads((quiet / "labels/incidents.json").read_text()) == []
    windows = pd.read_csv(quiet / "labels/anomaly_windows.csv")
    assert len(windows) == 0 and list(windows.columns) == generate.ANOMALY_WINDOW_COLUMNS
    assert (quiet / "changes/changes.jsonl").read_text() == ""
    topology = json.loads((quiet / "catalog/topology.json").read_text())
    assert topology["splits"] == {}                       # the splits belong to the scenario calendar
    assert topology["time_range"] == {"start": "2026-09-28T00:00:00Z", "end": "2026-10-06T00:00:00Z", "step": "1m"}


def test_no_incidents_world_has_the_same_series_and_a_reading_every_minute(quiet):
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


def test_other_lengths_need_no_incidents(tmp_path):
    with pytest.raises(SystemExit, match="--no-incidents"):
        generate.main(["--days", "35", "--out", str(tmp_path / "x")])
    with pytest.raises(SystemExit, match="at least 8"):
        generate.main(["--no-incidents", "--days", "7", "--out", str(tmp_path / "x")])
    assert not (tmp_path / "x").exists()
