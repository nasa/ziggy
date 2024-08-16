<!-- -*-visual-line-*- -->

[[Previous]](rerun-task.md)
[[Up]](ziggy-gui-troubleshooting.md)
[[Next]](select-hpc.md)

## Halting Tasks

Sometimes it's necessary to stop the execution of tasks after they start running. Tasks that are running as jobs under control of a batch system at an HPC facility will provide command line tools for this, but they're a hassle to use when you're trying to halt a large number of jobs. Trying to halt tasks running locally is likewise hassle-tastic.

Fortunately, Ziggy will let you do this from the console.

### Halt all Jobs for a Task

To halt all jobs for a task, go to the tasks table on the instances panel, right click the task, and run the `Halt selected tasks` command:

<img src="images/halt-task-menu-item.png" style="width:16cm;"/>

You'll be prompted to confirm that you want to halt the task. When you do that, you'll see something like this:

<img src="images/halt-in-progress.png" style="width:35cm;"/>

The processing step of the task will immediately receive an `ERROR` prefix. The instance will go to state `ERRORS_RUNNING` because the other task is still running; once it completes, the instance will go to `ERRORS_STALLED`. Meanwhile, the alert looks like this:

<img src="images/halt-alert.png" style="width:35cm;"/>

As expected, it notifies you that the task stopped because it was halted and not due to an error of some kind. Note that the `Pi` (pipelines) light has gone from yellow to red since the other task has since completed and the pipeline has stopped.

### Halt all Tasks for an Instance

This is the same idea, except it's the pop-up menu for the instance table, and you select `Halt all incomplete tasks`.

[[Previous]](rerun-task.md)
[[Up]](ziggy-gui-troubleshooting.md)
[[Next]](select-hpc.md)
