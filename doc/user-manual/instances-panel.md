[[Previous]](start-pipeline.md)
[[Up]](ziggy-gui.md)
[[Next]](start-end-nodes.md)

## The Instances Panel

The instances panel is the single most useful part of Ziggy when it comes to monitoring execution, so it's worth some discussion of exactly what it's trying to tell you. In the process, we'll introduce some concepts that will remain vital as we move through Ziggy-land.

### Instances, Tasks, and Subtasks

The first thing to explain is the concept of instances, tasks, and subtasks. Instances are the things shown on the left side of the display; tasks are on the right; the numbers in the last column of the tasks table represent counts of subtasks.

And now you know as much as you did when we started. Okay, keep reading.

#### Instances

A pipeline instance is just what it sounds like: it's a single instance of one of the pipelines defined in the `pd-*.xml` files. Recall that the sample pipeline has 4 nodes: data receipt, permuter, flip, and averaging. When you pressed the `Fire!` button, Ziggy created a copy of that pipeline, in a form that Ziggy knows how to execute.

Each instance has a unique ID number, with instance 1 being the first (so 1-based, not 0-based). The instance contains its own copies of all the parameter sets used in the pipeline, and these are stored permanently. Thus, even if you later change the values of some parameters, the copies that were made for the instance won't change.

#### Tasks

A pipeline task is also just what it sounds like: it's a chunk of work that Ziggy has to execute as part of a pipeline instance. Each task uses a specific algorithm module to process a specific collection of data. Tasks, then, are the things that pipeline instances use to run the individual algorithms. Every task is associated with one and only one instance; an instance can have as many tasks as it needs to get the work done.

In the same way that a pipeline instance is defined by a `pipeline` definition in one of the `pd-*.xml` files, each task is created from a `node` in the pipeline. As the pipeline instance executes, it steps through the node list; at each node, it creates tasks that run the algorithm defined by the node; once those tasks are done, the instance moves on to the next node until the tasks created from the last node finish.

Like pipeline instances, pipeline tasks have unique ID numbers that start at 1 and increase monotonically. Task numbers are never "recycled." That is to say, if pipeline instance 1 has tasks 1 through 7, pipeline instance 2 will start with task 8.

#### Subtasks

It turns out that, in many (perhaps most) cases, data analysis is what we call "embarrassingly parallel:" that is to say, there are a lot of nuggets of processing that can be performed in parallel with one another, that do not need to interact with each other at any time, and which run the same code against different chunks of data. In cases like this, there's a big execution time advantage if the nuggets are run simultaneously with one another to the extent that the compute hardware will allow.

These nuggets are known in Ziggy-land as subtasks.

Subtasks, confusingly, are numbered from zero. Also, subtask numbers are "recycled:" that is to say, if one task has subtasks 0 to N, another will have subtasks 0 to M. The subtask numbers appear in the subdirectories of the task directory as `st-0` through `st-<whatever>`.

With all that in mind, let's look at the instances panel again:

![](images/pipeline-done.png)

The instance on the left, instance 1, is the pipeline instance that's going to plow through the entire pipeline, from data receipt to averaging. On the right, we see the tasks that instance 1 uses for the processing: one task for data receipt, two each for permuter, flip, and averaging. The numbers in the `P-state` column represent subtask counts: the first number is total number of subtasks, the second is number completed, the third is number failed. Each of permuter and flip used 4 subtasks per task; averaging ran with just 1 subtask per task.

The scoreboard at the top rolls up the tasks table according to algorithm name. The permuter line shows the aggregated results of the 2 permuter tasks, and so on. The final line is the roll-up across all tasks within the pipeline. The scoreboard presents the task information slightly differently in that it shows totals of `Submitted`, `Processing`, `Completed`, and `Failed`. `Completed` and `Failed` are self-explanatory (I hope). `Processing` indicates tasks that are currently using computer time to process their data. `Submitted` tasks are tasks that are waiting for some hardware, somewhere, to decide to process them. Depending on your system, tasks may go instantly from `Submitted` to `Processing`, or some of them might have to wait around awhile in the `Submitted` queue.

### Unit of Work

In the midst of all this is a column under tasks labeled `UOW`, which stands for "Unit of Work." As a general matter, "Unit of Work" is a $10 word for, "What's the chunk of data that this task is in charge of?"

The parameter set that Ziggy uses to figure out how to divide work up into tasks also provides a means by which the user can specify a name that gets associated with each task. This is what's displayed in the `UOW` column. In the event that some tasks for a given algorithm module succeed and others fail, the `UOW` label lets you figure out where Ziggy got the data that caused the failed task. This can be useful, as I'm sure you can imagine.

#### Can You be a Bit More Specific About That?

Sure! Let's look again at the definition of the permuter node from [The Pipeline Definition article](pipeline-definition.md):

```xml
<node moduleName="permuter" childNodeNames="flip">
  <inputDataFileType name="raw data"/>
  <outputDataFileType name="permuted colors"/>
  <modelType name="dummy model"/>
  <moduleParameter name="Remote Parameters (permute color)"/>
  <moduleParameter name="Multiple subtask configuration"/>
</node>
```

The definition of the node includes a parameter set, `Multiple subtask configuration`, which is an instance of the `TaskConfigurationParameters`. From [the article on The Task Configuration Parameter Sets](task-configuration.md), we see that it looks like this:

```xml
<parameter-set name="Multiple subtask configuration"
               classname="gov.nasa.ziggy.uow.TaskConfigurationParameters">
  <parameter name="taskDirectoryRegex" value="set-([0-9]{1})"/>
  <parameter name="singleSubtask" value="false"/>
  <parameter name="maxFailedSubtaskCount" value="0"/>
  <parameter name="reprocess" value="true"/>
  <parameter name="reprocessingTasksExclude" value="0"/>
  <parameter name="maxAutoResubmits" value="0"/>
</parameter-set>
```

The `taskDirectoryRegex` parameter is `set-([0-9]{1})`. In plain (but New York accented) English, what this means is, "Go to the datastore and find every directory that matches the regex. Every one of those, you turn into a task. You got a problem with that?" Thus you wind up with a task for `set-1` and another for `set-2`.

Meanwhile, the `taskDirectoryRegex` has a regex group in it, `([0-9]{1})`. This tells Ziggy to take that part of the directory name (i.e., a digit) and make it the name of the unit of work on the tasks table. If I had been smarter and written the `taskDirectoryRegex` as `(set-[0-9]{1})`, the UOW display would have shown `set-1` and `set-2` instead of `1` and `2`.

#### What About Subtask Definition?

Now we've seen how Ziggy uses the `TaskConfigurationParameters` instance to define multiple tasks for a given pipeline node. How do subtasks get defined? This uses a combination of 2 things: the `TaskConfigurationParameters` and the definition of input data file types for the node. Let's look at how that works.

In TaskConfigurationParameters, there's a boolean parameter, `singleSubtask`. This does what it says: if set to `true`, Ziggy creates one and only one subtask for each task, and copies all the inputs into that subtask's directory. When set to false, as here, it generates multiple subtasks for the task.

The way it does this is to create a subtask for each input data file. If we look at how the inputs to the permuter are defined in [the article on Data File Types](data-file-types.md), we see this:

```xml
<dataFileType name="raw data"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9]).png"
              fileNameWithSubstitutionsForDatastore="$2/L0/$1-$3.png"/>
```

The file names in the datastore are going to be things like `set-1/L0/nasa-logo-file-0.png`, `set-1/L0/nasa-logo-file-1.png`, and so on. So -- cool! All the files in `set-1/L0` will be processed in the task for `set-1` data; there will be a subask for `nasa-logo-file-0.png`, another for `nasa-logo-file-1.png`, and so on.

### Unit of Work

In the midst of all this is a column under tasks labeled `UOW`, which stands for "Unit of Work." As a general matter, "Unit of Work" is a $10 word for, "What's the chunk of data that this task is in charge of?" 

The parameter set that Ziggy uses to figure out how to divide work up into tasks also provides a means by which the user can specify a name that gets associated with each task. This is what's displayed in the `UOW` column. In the event that some tasks for a given algorithm module succeed and others fail, the `UOW` label lets you figure out where Ziggy got the data that caused the failed task. This can be useful, as I'm sure you can imagine. 

#### Can You be a Bit More Specific About That?

Sure! Let's look again at the definition of the permuter node from [The Pipeline Definition article](pipeline-definition.md):

```XML
    <node moduleName="permuter" childNodeNames="flip">
        <inputDataFileType name="raw data"/>
        <outputDataFileType name="permuted colors"/>
        <modelType name="dummy model"/>
        <moduleParameter name="Remote Parameters (permute color)"/>
        <moduleParameter name="Multiple subtask configuration"/>
    </node>
```

The definition of the node includes a parameter set, `Multiple subtask configuration`, which is an instance of the `TaskConfigurationParameters`. From [the article on The Task Configuration Parameter Sets](task-configuration.md), we see that it looks like this:

```XML
    <parameter-set name="Multiple subtask configuration" 
        classname="gov.nasa.ziggy.uow.TaskConfigurationParameters">
        <parameter name="taskDirectoryRegex" value="set-([0-9]{1})"/>
        <parameter name="singleSubtask" value="false"/>
        <parameter name="maxFailedSubtaskCount" value="0" />
        <parameter name="reprocess" value="true"/>
        <parameter name="reprocessingTasksExclude" value="0"/>
      	<parameter name="maxAutoResubmits" value="0"/>
    </parameter-set>
```

The `taskDirectoryRegex` parameter is `set-([0-9]{1})`. In plain (but New York accented) English, what this means is, "Go to the datastore and find every directory that matches the regex. Every one of those, you turn into a task. You got a problem with that?" Thus you wind up with a task for `set-1` and another for `set-2`. 

Meanwhile, the `taskDirectoryRegex` has a regex group in it, `([0-9]{1})`. This tells Ziggy to take that part of the directory name (i.e., a digit) and make it the name of the unit of work on the tasks table. If I had been smarter and written the `taskDirectoryRegex` as `(set-[0-9]{1})`, the UOW display would have shown `set-1` and `set-2` instead of `1` and `2`. 

#### What About Subtask Definition?

Now we've seen how Ziggy uses the `TaskConfigurationParameters` instance to define multiple tasks for a given pipeline node. How do subtasks get defined? This uses a combination of 2 things: the `TaskConfigurationParameters` and the definition of input data file types for the node. Let's look at how that works. 

In TaskConfigurationParameters, there's a boolean parameter, `singleSubtask`. This does what it says: if set to `true`, Ziggy creates one and only one subtask for each task, and copies all the inputs into that subtask's directory. When set to false, as here, it generates multiple subtasks for the task.

The way it does this is to create a subtask for each input data file. If we look at how the inputs to the permuter are defined in [the article on Data File Types](data-file-types.md), we see this:

```XML
<dataFileType name="raw data"
    fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9]).png"
    fileNameWithSubstitutionsForDatastore="$2/L0/$1-$3.png"
/>
```

The file names in the datastore are going to be things like `set-1/L0/nasa-logo-file-0.png`, `set-1/L0/nasa-logo-file-1.png`, and so on. So -- cool! All the files in `set-1/L0` will be processed in the task for `set-1` data; there will be a subask for `nasa-logo-file-0.png`, another for `nasa-logo-file-1.png`, and so on. 

### Pipeline States

The instances panel also has numerous indicators named `State` and `p-State` that deserve some explanation.

#### Pipeline Instance States

The possible states for a pipeline instance are described below.

| State          | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| INITIALIZED    | Instance has been created but Ziggy hasn't yet gotten around to running any of its tasks. |
| PROCESSING     | Tasks in this instance are being processed, and none have failed yet. |
| ERRORS_RUNNING | Tasks in this instance are being processed, but at least 1 has failed. |
| ERRORS_STALLED | Processing has stopped because of task failures.             |
| STOPPED        | Not currently used.                                          |
| COMPLETED      | All done!                                                    |

About ERRORS_RUNNING and ERRORS_STALLED: as a general matter, tasks that are running the same algorithm in parallel are totally independent, so if one fails the others can keep running; this is the ERRORS_RUNNING state. However: once all tasks for a given algorithm are done, if one or more has failed, it's not guaranteed that the next algorithm can run. After all, a classic pipeline has the outputs from one task become the inputs of the next, and in this case some of the outputs from some of the tasks aren't there. In this case, the instance goes to ERRORS_STALLED, and nothing more will happen until the operator addresses whatever caused the failure.

#### Pipeline Task States

The possible states for a pipeline task are described below.

| State       | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| INITIALIZED | Task has been created and is waiting for some kind of attention. |
| SUBMITTED   | The task will run as soon as the worker has available resources to devote to it. |
| PROCESSING  | The task is running.                                         |
| ERROR       | All subtasks have run, and at least one subtask has failed.  |
| COMPLETED   | All subtasks completed successfully and results were copied back to the datastore. |
| PARTIAL     | Not currently used.                                          |

#### Pipeline Task Processing States (p-States)

When a task is in the `PROCESSING` state, it's useful to have a more fine-grained sense of what it's doing, where it is in the process, etc. This is the role of the processing state, or `P-state`, of the task. Each `P-state` has an abbreviation that's shown in the last column of the tasks table. The `P-states` are shown below.

| P-state              | Abbreviation | Description                                                  |
| -------------------- | ------------ | ------------------------------------------------------------ |
| INITIALIZING         | I            | Nothing has happened yet, the task is still in the state it was in at creation time. |
| MARSHALING           | M            | The inputs for the task are being assembled.                 |
| ALGORITHM_SUBMITTING | As           | The task is ready to run and is being sent to whatever system is in charge of scheduling its execution. |
| ALGORITHM_QUEUED     | Aq           | In the case of execution environments that use a batch system, the task is waiting in the batch queue to run. |
| ALGORITHM_EXECUTING  | Ae           | The algorithm is running, data is getting processed.         |
| ALGORITHM_COMPLETE   | Ac           | The algorithm is done running.                               |
| STORING              | S            | Ziggy is storing results in the datastore. Sometimes referred to as "persisting." |
| COMPLETE             | C            | The results have been copied back to the datastore.          |

### Worker

The `Worker` column on the tasks table shows which worker is managing task execution, and which thread on that worker.

At the moment, the "which worker" question is kind of dull, since there's only one worker per cluster, and the console has to run on the same computer as the worker. This is why the worker is always listed as "localhost". This may not be true in the future, so we've left this information on the display.

Recall from the discussion on [Running the Cluster](running-pipeline.md) that the worker has multiple threads that can execute in parallel. The thread number tells you which of these is occupied with a given task.

### P-Time

Both the instances table and the tasks table have a column labeled `P-time`. This represents the total "wall time" the instance or task has been running. Put simply, `P-time` is a timer or stopwatch that starts when the task or instance starts running, stops when the instance or task completes or fails, and starts again if the task or instance restarts. It's thus the total-to-date time spent processing, including any time spent in execution attempts that failed.

[[Previous]](start-pipeline.md)
[[Up]](ziggy-gui.md)
[[Next]](start-end-nodes.md)
