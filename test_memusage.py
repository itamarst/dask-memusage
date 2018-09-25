"""Tests for dask_memusage."""

import os
from time import sleep
from csv import DictReader

import numpy as np
from dask.bag import from_sequence
from dask import compute
from dask.distributed import Client, LocalCluster

from dask_memusage import install


def allocate_50mb(x):
    """Allocate 50MB of RAM."""
    arr = np.ones((50, 1024, 1024), dtype=np.uint8)
    sleep(0.1)
    return x * 2

def no_allocate(y):
    """Don't allocate any memory."""
    return y * 2


def make_bag():
    """Create a bag."""
    return from_sequence(
        [1, 2], npartitions=2).map(allocate_50mb).sum().apply(no_allocate)


def test_highlevel_python_usage(tmpdir):
    """We can add a MemoryUsagePlugin and get memory usage written out."""
    tempfile = os.path.join(tmpdir, "out.csv")
    cluster = LocalCluster(n_workers=2, threads_per_worker=1,
                           memory_limit=None)
    install(cluster.scheduler, tempfile)
    client = Client(cluster)
    compute(make_bag())

    result = []
    with open(tempfile) as f:
        csv = DictReader(f)
        for row in csv:
            result.append((row["task_key"],
                           float(row["max_memory_mb"]) - float(row["min_memory_mb"])))
    assert len(result) == 3
    assert "sum-part" in result[0][0]
    assert "sum-part" in result[1][0]
    assert 52 > result[0][1] > 49
    assert 52 > result[1][1] > 49
    assert "no_allocate" in result[2][0]
    assert 1 > result[2][1] >= 0
