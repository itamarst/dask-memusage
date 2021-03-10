# dask-memusage

If you're using Dask with tasks that use a lot of memory, RAM is your bottleneck for parallelism.
That means you want to know how much memory each task uses:

1. So you can set the highest parallelism level (process or threads) for each machine, given available to RAM.
2. In order to know where to focus memory optimization efforts.

`dask-memusage` is an MIT-licensed statistical memory profiler for Dask's Distributed scheduler that can help you with both these problems.

`dask-memusage` polls your processes for memory usage and records the minimum and maximum usage for each task in the Dask execution graph in a CSV:

```csv
task_key,min_memory_mb,max_memory_mb
"('from_sequence-map-sum-part-e15703211a549e75b11c63e0054b53e5', 0)",44.84765625,96.98046875
"('from_sequence-map-sum-part-e15703211a549e75b11c63e0054b53e5', 1)",47.015625,97.015625
"('sum-part-e15703211a549e75b11c63e0054b53e5', 0)",0,0
"('sum-part-e15703211a549e75b11c63e0054b53e5', 1)",0,0
sum-aggregate-apply-no_allocate-4c30eb545d4c778f0320d973d9fc8ea6,0,0
apply-no_allocate-4c30eb545d4c778f0320d973d9fc8ea6,47.265625,47.265625
task_key,min_memory_mb,max_memory_mb
"('from_sequence-map-sum-part-e15703211a549e75b11c63e0054b53e5', 0)",44.84765625,96.98046875
"('from_sequence-map-sum-part-e15703211a549e75b11c63e0054b53e5', 1)",47.015625,97.015625
"('sum-part-e15703211a549e75b11c63e0054b53e5', 0)",0,0
"('sum-part-e15703211a549e75b11c63e0054b53e5', 1)",0,0
sum-aggregate-apply-no_allocate-4c30eb545d4c778f0320d973d9fc8ea6,0,0
apply-no_allocate-4c30eb545d4c778f0320d973d9fc8ea6,47.265625,47.265625
```

You may also find the [Fil memory profiler](https://pythonspeed.com/fil) useful in tracking down which specific parts of your code are responsible for peak memory allocations.

## Example

Here's a working standalone program using `dask-memusage`; notice you just need to add two lines of code:

```python
from time import sleep
import numpy as np
from dask.bag import from_sequence
from dask import compute
from dask.distributed import Client, LocalCluster

from dask_memusage import install  # <-- IMPORT

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

def main():
    cluster = LocalCluster(n_workers=2, threads_per_worker=1,
                           memory_limit=None)
    install(cluster.scheduler, "memusage.csv")  # <-- INSTALL
    client = Client(cluster)
    compute(make_bag())

if __name__ == '__main__':
    main()
```

## Usage

*Important:* Make sure your workers only have a single thread! Otherwise the results will be wrong.

### Installation

On the machine where you are running the Distributed scheduler, run:

```console
$ pip install dask_memusage
```

Or if you're using Conda:

```console
$ conda install -c conda-forge dask-memusage
```

### API usage

```python
# Add to your Scheduler object, which is e.g. your LocalCluster's scheduler
# attribute:
from dask_memoryusage import install
install(scheduler, "/tmp/memusage.csv")
```

### CLI usage

```console
$ dask-scheduler --preload dask_memusage --memusage.csv /tmp/memusage.csv
```

## Limitations

* Again, make sure you only have one thread per worker process.
* This is statistical profiling, running every 10ms.
  Tasks that take less than that won't have accurate information.

## Help

Need help? File a ticket at https://github.com/itamarst/dask-memusage/issues/new
