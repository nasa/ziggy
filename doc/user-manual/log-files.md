<!-- -*-visual-line-*- -->

[[Previous]](troubleshooting.md)
[[Up]](troubleshooting.md)
[[Next]](ziggy-gui-troubleshooting.md)

## Log Files in Ziggy

Ziggy produces a substantial number of different kinds of log files. All of them are in the logs directory under the main pipeline results directory, with one exception: the subtask algorithm log, which is in the subtask directory.

Anyways, if you look at the logs directory, you'll see this:

```console
logs$ ls
algorithms    cli    db    manifests    state    supervisor    ziggy
logs$
```

Let us consider each of these directories in turn.

### db Directory

This is where the log files from relational database applications may be stored. If you're using an non-system PostgreSQL database, this directory should contain `pg.log`. If a system database is being used, this directory will not be present since the sysadmin and DBA get to decide where the logs go, not you. It is also not present for databases such as HSQLDB.

### state Directory

These aren't -- quite -- log files as such. They're actually files that are used to communicate between the algorithm processing system and the supervisor system (remember that the computer with the supervisor might not be the one trying to run the algorithm). Here's the contents of that directory:

```console
logs$ ls state
ziggy.1.2.permuter.COMPLETE_4-4-0
ziggy.1.3.permuter.COMPLETE_4-4-0
ziggy.1.4.flip.COMPLETE_4-4-0
ziggy.1.5.flip.COMPLETE_4-4-0
ziggy.1.6.averaging.COMPLETE_1-1-0
ziggy.1.7.averaging.COMPLETE_1-1-0
ziggy.2.8.permuter.COMPLETE_4-4-0
ziggy.2.9.permuter.COMPLETE_4-4-0
ziggy.2.10.flip.COMPLETE_4-4-0
ziggy.2.11.flip.COMPLETE_4-4-0
ziggy.3.12.permuter.COMPLETE_4-3-1
ziggy.3.13.permuter.COMPLETE_4-3-1
logs$
```

Each task has a state file that the task execution system updates with the task processing step and the subtask counts for that task; by which we mean, the task execution system keeps changing the name of the file to reflect the current state of the task. The monitoring subsystem in the supervisor looks at these files to determine the current state of each task, which is then reflected on the GUI.

In this case, we see that instance 3, task 12 has completed (i.e., the algorithm is no longer running) with a final score of 4 total subtasks of which 3 completed and 1 failed, which we already knew.

### cli Directory

Here we see the logs that are produced by various parts of Ziggy that are started from the command line. Looking inside the directory, we see this:

```console
logs$ ls cli
cluster.log    console.log
logs$
```

The `cluster.log` file is logging from all the cluster commands; `console.log` captures all logging from the console. You'll sometimes see another file, `ziggy.log`, which is the logging from other commands executed by `ziggy`. Feel free to look into these, but none of them will provide any insight to the exception we're trying to understand.

### Supervisor Directory

The supervisor directory's contents look like this:

```console
logs$ ls supervisor
metrics-dump-90188.txt          metrics-dump-90188.txt.old      supervisor.log
logs$
```

The `supervisor.log` file logs everything the supervisor does. This is useful for troubleshooting problems that are more directly focused on misbehavior by the supervisor itself, which isn't our problem today.

### ziggy Directory

The `ziggy` directory has the log files for the "Ziggy-side" parts of task execution: specifically, marshaling of inputs and persisting of outputs. The directory looks like this:

```console
logs$ ls ziggy
1-1-data-receipt.0-0.log
1-4-flip.0-2.log
1-7-averaging.0-2.log
2-8-permuter.0-2.log
1-2-permuter.0-0.log
1-5-flip.0-0.log
2-10-flip.0-0.log
2-9-permuter.0-0.log
1-2-permuter.0-2.log
1-5-flip.0-2.log
2-10-flip.0-2.log
2-9-permuter.0-2.log
1-3-permuter.0-0.log
1-6-averaging.0-0.log
2-11-flip.0-0.log
1-3-permuter.0-2.log
1-6-averaging.0-2.log
2-11-flip.0-2.log
1-4-flip.0-0.log
1-7-averaging.0-0.log
2-8-permuter.0-0.log
3-12-permuter.0-0.log
3-13-permuter.0-0.log
logs$
```

Every task has logs with the usual nomenclature of instance number, task number, and module name separated by hyphens. Note that most tasks have 2 log files: the first ends in `-0.log`; the second, `-2.log`. The thing to understand is that the logs from task execution are numbered in order. Thus, `1-2-permuter.0-0.log` is the first log file for `1-2-permuter`, which is the marshaling log, while `1-2-permuter.0-2.log` is the third log file, for persisting.

So where is `1-2-permuter.0-1.log`, and what does it cover? That's the log file for the step that comes between marshaling and persisting, which is algorithm execution, and that file is in ...

### algorithms Directory

Here's what the algorithm directory looks like:

```console
logs$ ls algorithms
1-2-permuter.0-1.log
1-4-flip.0-1.log
1-6-averaging.0-1.log
2-10-flip.0-1.log
2-8-permuter.0-1.log
1-3-permuter.0-1.log
1-5-flip.0-1.log
1-7-averaging.0-1.log
2-11-flip.0-1.log
2-9-permuter.0-1.log
3-12-permuter.0-1.log
3-13-permuter.0-1.log
logs$
```

There's one log file for every task.

Because execution failed during algorithm processing, not marshaling or persisting, this is likely to be the log file to use if we want insight into the problem. If you open the file `3-12-permuter.0-1.log` and swim down, a bit, sure enough you find this block:

```console
2022-09-19 19:05:03,361 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine] (st-0) Traceback (most recent call last):
2022-09-19 19:05:03,361 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine] (st-0)  File "ziggy/sample-pipeline/build/env/lib/python3
2022-09-19 19:05:03,362 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine] (st-0)   permute_color(data_file, throw exception, produce_output)
2022-09-19 19:05:03,362 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine) (st-0) File "ziggy/sample-pipeline/build/env/lib/python3
2022-09-19 19:05:03,362 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine] (st-0)  raise ValueError("Value error raised because throw_exception is true
2022-09-19 19:05:03,362 INFO [Exec Stream Pumper:WriterLogOutputStream.processLine] (st-0) ValueError: Value error raised because throw exception is true
2022-09-19 19:05:03,383 WARN [pool-2-thread-1:SubtaskExecutor.execAlgorithm] (st-0) Marking subtask as failed because retCode = 1
2022-09-19 19:05:03,385 WARN [pool-2-thread-1:SubtaskExecutor.execAlgorithm] (st-0) Marking subtask as failed because an error file exists
2022-09-19 19:05:03,386 INFO [pool-2-thread-1:SubtaskMaster.executeSubtask] (st-0) FINISH subtask on host, rc: 1
2022-09-19 19:05:03,386 ERROR [pool-2-thread-1: SubtaskMaster.processSubtasks] (st-0) Error occurred during subtask processing
gov.nasa.ziggy.module.ModuleFatalProcessingException: Failed to run: permuter, retCode=1
  at gov.nasa.ziggy.module.SubtaskMaster.executeSubtask (SubtaskMaster.java:2381 ~[ziggy.jar:?]
  at gov.nasa.ziggy.module.SubtaskMaster.processSubtasks (SubtaskMaster.java:123) [ziggy.jar:?]
  at gov.nasa.ziggy.module.SubtaskMaster.run(SubtaskMaster.java:79) [ziggy.jar:?]
  at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [?:1.8.0_265]
  at java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_265]
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_265]
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_265]
  at java.lang.Thread run(Thread.java:749) [?:1.8.0_265]
```

There's the Python traceback, which is what Python does when an exception occurs. The text shows you exactly what was wrong in this case: permuter errored because we told it to error! Okay, so in this case it's not exactly a revelation, but under ordinary circumstances, when you don't deliberately crash your algorithm, this will be much more informative.

While we're here, note the parts of the log that have `(st-0)` in them. The algorithm log covers all of the subtasks in a single log. In order to figure out which subtask goes to a particular line of logging, the subtask number is inserted into each line of logging that comes from a subtask.

### Subtask Algorithm Log

This is the one exception to the log files being in the logs directory. If you go to the `3-12-permuter/st-0` directory under task-data, you'll see the `permuter-stdout-0.log` file. This is a transcription of all the standard output from the permuter algorithm module. It looks like this:

```console
Traceback (most recent call last):
  File "ziggy/sample-pipeline/build/env/lib/python3.8/site-packages/major_tom/permuter.py", line 50, in <module>
    permute_color(data_file, throw_exception, produce_output)
  File "ziggy/sample-pipeline/build/env/lib/python3.8/site-packages/major_tom/major_tom.py", line 34, in permute_color
    raise ValueError("Value error raised because throw_exception is true")
ValueError: Value error raised because throw_exception is true
```

This is a subset of the algorithm log file, above. What's up with that?

Well, the answer is: Ziggy automatically sends all the standard output from the algorithms to the appropriate `stdout-0.log` file in the subtask directory; but it also sends that same information back through the main logging system so it ends up in the task algorithm log.

Why do it this way?

Remember what we said earlier about subtask directories: sometimes it's helpful to copy them to a different file system on a different computer so that a subject matter expert can troubleshoot issues on the system they're familiar with (and one where they have write permissions for all the contents). By including all of the standard output from the subtask in the `stdout-0.log` file, we ensure that if the expert on the algorithm code wants to see all the output from the algorithm, it's right there for them. Even better, it's right there for them without all the "overhead" of logging messages that these experts won't be interested in (i.e., the rest of the algorithm log contents).

[[Previous]](troubleshooting.md)
[[Up]](troubleshooting.md)
[[Next]](ziggy-gui-troubleshooting.md)
