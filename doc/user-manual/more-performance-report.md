[[Previous]](display-logs.md)
[[Up]](ziggy-gui-troubleshooting.md)
[[Next]](rerun-task.md)

## Using the Performance Report

The [Displaying the Performance Report](performance-report.md) article describes how to display the
report and explains that it is used to display metrics. Two of those metrics that are useful for
debugging include the memory and disk usage plots. Display a report from one of your pipeline runs
to use as an example for this article.

### The Subtask Wall Time Histogram

A subtask wall time histogram is shown for each node. If the bins approach the `Max subtask wall
time` or `Typical subtask wall time` values that you configured in the `Edit remote execution
parameters` dialog, those values are shown on the histogram. If those values are shown, it could
mean that you have configured those values exquisitely, but it could also mean that you may have an
issue in your algorithm or that your values are too small.

The statistics below the plot are self-explanatory. The units are the same as those shown in the
histogram's x-axis. The middle columns in the Top 10 table show the task and subtask. This is
helpful to identify a problematic subtask directory that took longer than expected. For example,
let's assume that `2:3` showed up in the permuter subtask wall time page. The task directory
associated with this pair is `sample-pipeline/build/pipeline-results/task-data/1-2-permuter/st-2/`.

### The Subtask Memory Usage Histogram

If there is memory usage data available (see [Enabling the Memory
Monitor](#enabling-the-memory-monitor)), a subtask memory usage histogram is shown for each node. If
the bins approach the heap size divided by the number of workers or the per-subtask HPC memory
request, those values are shown on the histogram. If those values are shown, it could mean that you
have configured those values exquisitely, but it could also mean that you may have a memory leak or
that your values are too small.

The default heap size is configured with the `ziggy.worker.heapSizeGigabytes` property in your
[property file](properties.md). This value can be adjusted for each node using the `Maximum heap
size` value in the Edit node configuration dialog. The default number of workers is
configured with the `ziggy.worker.count` property. This value can also be adjusted in the Edit node
configuration dialog using the `Maximum workers` field. In addition, for processes running on an
HPC, the `Gigs per subtask` value that you configured in the `Edit remote execution parameters`
dialog may also be displayed.

The statistics below the plot are self-explanatory. The units are the same as those shown in the
histogram's x-axis. The middle columns in the Top 10 table show the task and subtask. This is
helpful to identify a problematic subtask directory that used more memory than expected. For
example, let's assume that `2:3` showed up in the permuter subtask memory usage page. The task
directory associated with this pair is
`sample-pipeline/build/pipeline-results/task-data/1-2-permuter/st-2/`.

### Enabling the Memory Monitor

To view the [subtask memory usage histogram](#the-subtask-memory-usage-histogram), set the
`ziggy.pipeline.memory-monitor.enabled` property to `true` (see [Appendix A. Once this property is
set, memory statistics will be gathered when the pipeline runs. Note that the subtask memory usage
histogram will appear in the report as long as there are memory statistics available, regardless of
the value of the `ziggy.pipeline.memory-monitor.enabled` property.

Another value that controls the memory monitoring is
`ziggy.pipeline.memory-monitor.intervalSeconds`, which determines the rate that memory samples are
taken. By default, this is 60 seconds, but this can be adjusted lower if you are running out of
memory and need better resolution, or higher if you don't want to commit processing and disk
resources to the memory monitor.

[[Previous]](display-logs.md)
[[Up]](ziggy-gui-troubleshooting.md)
[[Next]](rerun-task.md)
