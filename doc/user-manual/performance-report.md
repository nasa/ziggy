[[Previous]](metrics.md)
[[Up]](performance-tracking.md)
[[Next]](console-cli.md)

## Displaying the Performance Report

The previous article described how you can use the [HDF5 APIs](hdf5.md) to slice and dice the
metrics Ziggy collects as you wish. This article shows you how Ziggy displays these metrics.

### From the Console

To view the metrics, go to the instances panel and run the Performance report command from the
context menu of the selected instance. This command displays a PDF that includes the metrics
described in the previous article. A copy of the PDF is copied to the reports directory. For the
sample pipeline, this is `sample-pipeline/build/pipeline-results/reports/`.

The content of the Performance Report should be self-explanatory. In addition to the metrics, the
report lists all of the tasks in one appendix and the parameters and data model registry in another
appendix.

### From the Command Line

You can also generate the performance report from the command line. For example:

```bash
$ ziggy perf-report --instance 1 --nodes 1:2
Wrote report to /path/to/ziggy/sample-pipeline/build/pipeline-results/reports/sample-1-performance-20260430T102814.pdf
```

This command writes the PDF to the reports directory, but does not display it.

If you were to drop the `--nodes` option, you'd get the same report described above. The `--nodes`
options restricts the output to the given nodes. The numbers are 0-based, so for the sample
pipeline, data receipt would be node 0, permuter would be node 1, and so on.

[[Previous]](metrics.md)
[[Up]](performance-tracking.md)
[[Next]](console-cli.md)
