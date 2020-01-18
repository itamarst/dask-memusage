"""Tests for dask_memusage."""

import os
from time import sleep
from csv import DictReader
from subprocess import Popen

import numpy as np
from dask.bag import from_sequence
from dask import compute
from dask.distributed import Client, LocalCluster

from dask_memusage import install, _WorkerMemory


def allocate_50mb(x):
    """Allocate 50MB of RAM."""
    sleep(1)
    arr = np.ones((50, 1024, 1024), dtype=np.uint8)
    sleep(1)
    return x * 2

def no_allocate(y):
    """Don't allocate any memory."""
    return y * 2


def make_bag():
    """Create a bag."""
    return from_sequence(
        [1, 2], npartitions=2
    ).map(allocate_50mb).sum().apply(no_allocate)


def test_workermemory():
    """_WorkerMemory adds memory per-worker and removes it per-worker.

    If no info is known, we just get 0.
    """
    wm = _WorkerMemory("")
    assert wm.memory_for_task("123") == [0]
    wm._add_memory("123", 7)
    wm._add_memory("456", 3)
    wm._add_memory("123", 8)
    assert wm.memory_for_task("123") == [7, 8]
    assert wm.memory_for_task("456") == [3]
    assert wm.memory_for_task("123") == [0]
    assert wm.memory_for_task("456") == [0]


def test_highlevel_python_usage(tmpdir):
    """We can add a MemoryUsagePlugin and get memory usage written out."""
    tempfile = str(tmpdir / "out.csv")
    cluster = LocalCluster(n_workers=2, threads_per_worker=1,
                           memory_limit=None)
    install(cluster.scheduler, tempfile)
    client = Client(cluster)
    compute(make_bag())
    check_csv(tempfile)


def test_commandline_usage(tmpdir):
    """We can add a MemoryUsagePlugin to a dask-scheduler subprocess."""
    tempfile = str(tmpdir / "out.csv")
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    scheduler = Popen(["dask-scheduler",
                       "--port", "3333",
                       "--host", "127.0.0.1",
                       "--preload", "dask_memusage",
                       "--memusage-csv", tempfile],
                      env=env)
    worker = Popen(["dask-worker", "tcp://127.0.0.1:3333",
                    "--nthreads", "1"],
                   env=env)
    try:
        client = Client("tcp://127.0.0.1:3333")
        compute(make_bag())
        check_csv(tempfile)
    finally:
        worker.kill()
        scheduler.kill()


def check_csv(csv_file):
    """Make sure CSV contains appropriate information."""
    with open(csv_file) as f:
        print(f.read())
    result = []
    found_50mb = 0
    found_0mb = 0
    with open(csv_file) as f:
        csv = DictReader(f)
        for row in csv:
            allocated = float(row["max_memory_mb"]) - float(row["min_memory_mb"])
            if "allocate_50mb" in row["task_key"]:
                assert 70 > allocated > 49
                found_50mb += 1
            elif "no_allocate" in row["task_key"]:
                assert 1 > allocated >= 0
                found_0mb += 1
    assert found_50mb >= 2
    assert found_0mb >= 1
