"""Low-impact, task-level memory profiling for Dask.

API usage:

    from dask_memoryusage import install
    install(scheduler, "/tmp/memusage.csv")

CLI usage:

    dask-scheduler --preload dask_memusage --memusage.csv /tmp/memusage.csv

"""

import os
import csv
from time import sleep
from threading import Lock, Thread
from collections import defaultdict

from psutil import Process
import click

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.client import Client
from distributed.scheduler import Scheduler


__all__ = ["install"]
__version__ = "1.1"


def _process_memory():
    """Return process memory usage, in MB.

    We include memory used by subprocesses.
    """
    proc = Process(os.getpid())
    return sum([
        p.memory_info().rss / (1024 * 1024)
        for p in [proc] + list(proc.children(recursive=True))
    ])


class _WorkerMemory(object):
    """Track memory usage by each worker."""

    def __init__(self, scheduler_address):
        self._scheduler_address = scheduler_address
        self._lock = Lock()
        self._worker_memory = defaultdict(list)

    def start(self):
        """Start the thread."""
        t = Thread(target=self._fetch_memory, name="WorkerMemory")
        t.setDaemon(True)
        t.start()

    def _add_memory(self, worker_address, mem):
        """Record memory timepoint for a worker."""
        self._worker_memory[worker_address].append(mem)

    def _fetch_memory(self):
        """Retrieve worker memory every 10ms."""
        client = Client(self._scheduler_address, timeout=30)
        while True:
            worker_to_mem = client.run(_process_memory)
            with self._lock:
                for worker, mem in worker_to_mem.items():
                    self._add_memory(worker, mem)
            sleep(0.01)

    def memory_for_task(self, worker_address):
        """The worker finished its previous task.

        Return its memory usage and then reset it.
        """
        with self._lock:
            result = self._worker_memory[worker_address]
            if not result:
                result = [0]
            del self._worker_memory[worker_address]
            return result


class MemoryUsagePlugin(SchedulerPlugin):
    """Record max and min memory usage for a task.

    Assumptions:

    * One task per process: each process has a single thread running a single
      task at a time.

    Limitations:

    * Statistical profiling at 10ms resolution.
    """
    def __init__(self, scheduler, csv_path):
        SchedulerPlugin.__init__(self)
        f = open(os.path.join(csv_path), "w", buffering=1)
        self._csv = csv.writer(f)
        self._csv.writerow(["task_key", "min_memory_mb", "max_memory_mb"])
        self._worker_memory = _WorkerMemory(scheduler.address)
        self._worker_memory.start()

    def transition(self, key, start, finish, *args, **kwargs):
        """Called by the Scheduler every time a task changes status."""
        # If the task finished, record its memory usage:
        if start == "processing" and finish in ("memory", "erred"):
            worker_address = kwargs["worker"]
            memory_usage = self._worker_memory.memory_for_task(worker_address)
            max_memory_usage = max(memory_usage)
            min_memory_usage = min(memory_usage)
            self._csv.writerow([key, min_memory_usage, max_memory_usage])


def install(scheduler: Scheduler, csv_path: str):
    """Register the memory usage profiler with a distributed Scheduler.

    :param scheduler: The Distributed Scheduler to register with.
    :param csv_path: The filesystem path where the CSV file will be written.
    """
    plugin = MemoryUsagePlugin(scheduler, csv_path)
    scheduler.add_plugin(plugin)


@click.command()
@click.option("--memusage-csv", default="memusage.csv")
def dask_setup(scheduler, memusage_csv):
    install(scheduler, memusage_csv)
