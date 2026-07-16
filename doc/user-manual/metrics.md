<!-- -*-visual-line-*- -->

[[Previous]](performance-tracking.md)
[[Up]](performance-tracking.md)
[[Next]](performance-report.md)

## Metrics

In this article we'll discuss the metrics that Ziggy tracks during pipeline execution. There are metrics that operate on each subtask, and other metrics that operate on each task.

### Subtask-Level Metrics

Ziggy tracks the wall time for each subtask, and also tracks the amount of memory used by the subtask as a function of time (i.e., you can see whether the memory consumption went up or down as execution proceeded).

#### Storage of Subtask-Level Metrics

To see the way that metrics are stored, let's go to the `pipeline-results/run` directory:

```bash
$ ls -F
1-2-permuter/	1-3-permuter/	1-4-flip/	1-5-flip/	1-6-averaging/	1-7-averaging/
```

Okay, so directories for each of the tasks. So what? Well, let's go further and look at the contents of `1-2-permuter`:

```bash
$ ls -F 1-2-permuter
st-0-max-mem-usage.h5	st-0-mem-usage.h5
st-1-max-mem-usage.h5	st-1-mem-usage.h5
st-2-max-mem-usage.h5	st-2-mem-usage.h5
st-3-max-mem-usage.h5	st-3-mem-usage.h5
subtask-walltimes.h5
```

We see that each subtask has two HDF5 files, a `max-mem-usage` file and a `mem-usage` file. If you open one of these files up (see [the article on HDF5 APIs](hdf5.md)), you'll see that the `mem-usage` file contains a time series: each entry in the time series contains a timestamp (an 8-byte integer representing the number of milliseconds since the UNIX/Linux beginning of time) and a memory usage (in bytes). The `max-mem-usage` file contains the maximum memory consumption by the given subtask, again in bytes.

The `subtask-walltimes.h5` file shows the wall times for each subtask, in milliseconds.

#### How to Use

As discussed in [the article on HDF5](hdf5.md), Ziggy provides APIs that allow you to read these files into Python, MATLAB, C++, or Java. This will allow you to analyze them as you see fit.

### Task-Level Metrics

Pipeline tasks are also all about time and space. In this case, there are actually several time metrics that are of potential interest:

- Time spent marshaling inputs
- Time spent in batch system queue (for remote execution)
- Total time spent running all subtask algorithms
- Time spent storing outputs.

The two "space" metrics are disk space, not RAM consumption. The metrics are:

- Size of the inputs
- Size of the outputs.

#### Storage of Task-Level Metrics

Task-level metrics are stored in the relational database.

[[Previous]](performance-tracking.md)
[[Up]](performance-tracking.md)
[[Next]](performance-report.md)
