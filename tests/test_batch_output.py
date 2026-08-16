"""Tests for pipeline.split_into_batches -- the workaround for Garmin Connect's
web importer failing on individual files within very large single-shot batches
(confirmed against a real account: a structurally valid file failed generically
as part of a ~3900-file batch, succeeded uploaded alone)."""

from fitbit2garmin.pipeline import split_into_batches


def test_splits_into_correctly_sized_batches(tmp_path):
    for i in range(250):
        (tmp_path / f"file_{i:04d}.fit").write_bytes(b"x")

    batch_dirs = split_into_batches(tmp_path, batch_size=100)

    assert len(batch_dirs) == 3
    assert len(list(batch_dirs[0].iterdir())) == 100
    assert len(list(batch_dirs[1].iterdir())) == 100
    assert len(list(batch_dirs[2].iterdir())) == 50


def test_idempotent_on_already_batched_directory(tmp_path):
    for i in range(10):
        (tmp_path / f"file_{i:04d}.fit").write_bytes(b"x")

    first = split_into_batches(tmp_path, batch_size=5)
    assert len(first) == 2

    second = split_into_batches(tmp_path, batch_size=5)
    assert second == []  # nothing left directly in the directory to batch


def test_empty_directory_returns_no_batches(tmp_path):
    assert split_into_batches(tmp_path, batch_size=100) == []
