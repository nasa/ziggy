[[Previous]](datastore-task-dir.md)
[[Up]](intermediate-topics.md)
[[Next]](properties.md)

## The Task Configuration Parameter Sets

If you look back at [what happened when we ran the pipeline](start-pipeline.md), you'll note that each of the nodes after data receipt -- permuter, flip, and averaging -- had 2 tasks; and in each case there was one task with a `UOW` of 1 and another with `UOW` 2. You may have wondered: how does this work? How did Ziggy decide how to divide up the work into tasks?

Excellent question! We will now answer that question. We'll also show you some other cool features that are all controlled by the parameter sets that manage task configuration.

### Task Configuration in XML Files

If you go back to pl-sample.xml, you'll see a parameter set that looks like this:

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

Then, looking at pd-sample.xml, if you swim down to the spot where the permuter node of the sample pipeline is defined, you where this parameter set is referenced:

```xml
<node moduleName="permuter" childNodeNames="flip">
  <inputDataFileType name="raw data"/>
  <outputDataFileType name="permuted colors"/>
  <modelType name="dummy model"/>
  <moduleParameter name="Remote Parameters (permute color)"/>
  <moduleParameter name="Multiple subtask configuration"/>
</node>
```

And finally, if you recall from [the datastore layout](datastore-task-dir.md), there are directories under the datastore root directory named `set-1` and `set-2`.

Put it all together, and you can sorta-kinda see what happened. I'll spell it out for you here:

- The permuter node uses the `Multiple subtask configuration` to, uh, configure its tasks.
- The `Multiple subtask configuration` parameter set has a regex of `set-([0-9]{1})` that is used to define task boundaries.
- Every directory that matches the regex is turned into a pipeline task. There are 2 directories in the datastore that match the regex: `set-1` and `set-2`. Thus, there's 1 task for `set-1` and another for `set-2`.
- The permuter uses `raw data` as its input data file type. Ziggy goes into the set-1 directory and looks for all the files that match the datastore file name specification. These are `set-1/L0/nasa-logo-file-0.png`, et. al. Ziggy moves copies of these files into the task directory for the `set-1` task. The `set-2` task files are procured the same way.
- The task directory regex has a regex group, `([0-9]{1})`. This is used as a label for the UOW column of the tasks table on the instances panel. Thus when the tasks show up on the instances panel, one is called `1` and the other `2`.

Every node needs an instance of `TaskConfigurationParameters`, so you may well wind up with more than one in your `pl-*.xml` files. The sample pipeline has a total of 3.

#### How to Run a Subset of Tasks

What happens if I want to run the `set-1` tasks but not the `set-2` tasks? All I have to do is change the regex so that only `set-1` matches. In other words, I want `taskDirectoryRegex="set-1"`.

This remains the case for more complicated arrangements. Consider for example what would happen if we had `set-1`, `set-2`, ... `set-9`. Now imagine that we want to run `set-1`, `set-3`, and `set-6`. In that case the regex would be `taskDirectoryRegex="set-([136]{1})"`.

#### Datastore Directory Organization

For the sample pipeline, we put the dataset directory at the top level, the `L0`, `L1`, etc., directories at the next level down. What if I wanted to do it the other way around? That is, what if I wanted `L0`, `L1`, etc., at the top and `set-1` and `set-2` under those? We can do it that way, sure!

First, the datastore would need to change, which means that the `fileNameWithSubstitutionsForDatastore` entries in the data file type definitions need to change. For example: for raw data, instead of `"$2/L0/$1-$3"`, it would need to be `"L0/$2/$1-$3"`; and so on for the other data file type definitions.

Second the `taskDirectoryRegex` values would have to change. For the permuter, the regex would need to be `"L0/set-([0-9]{1})"`. This is because the task directory regex needs to match the layout of the datastore and the naming conventions of the data file types.

There are two major downsides to organizing the datastore by processing step / data set rather than data set / processing step:

First, if you look at the pipeline definition, you can see that I use the same task configuration parameter set for both permuter and flip nodes. I can do that because in both cases, the regex for task generation is the same: `"set-([0-9]{1})"`. If I exchanged the order of directories, then permuter would need `"L0/set-([0-9]{1})"`, while the one for flip would be `"L1/set-([0-9]{1})"`.

Second, consider the situation in which I want to process `set-1` but not `set-2` through both permuter and flip. With the current setup, that's easy to do because they use the same task configuration parameters: I change the one regex, and both pipeline modules are affected. If I had a separate task configuration parameter set for each pipeline node, then I'd need to adjust the regex in each set to get the same effect. It's less convenient and introduces a greater chance of pilot error.

The moral of this story is that it's worthwhile to think a bit about the datastore organization, in particular thinking about what operations should be easy to group together.

### All Those Other Parameters

So far we've discussed only 1 parameter in the task configuration parameter set. What about all the others?  Let's walk through them.

#### singleSubtask

For permuter and flip, we wanted 1 subtask for each image, so 4 subtasks in each task. The averaging algorithm is different: in that case, we're averaging together a bunch of images. That means that we want all the data files to get processed together, not in a subtask each (I mean, you can average together 1 file, but it's kind of dull).

What this tells us is that Ziggy needs to support algorithms that get executed once per task and process all their data in that single execution (like the averaging algorithm), as well as algorithms that get executed multiple times per task and process one input file in each execution (like permuter and flip). The choice between these two modes of execution is controlled by the `singleSubtask` parameter. Setting this to true tells Ziggy that all the data files for the task are processed in 1 subtask, not multiple. Note that the averaging node has a different task configuration parameter set, and that set has `singleSubtask` set to true.

The default for `singleSubtask` is `false`.

#### maxFailedSubtaskCount

Under normal circumstances, as we'll see shortly, if even a single subtask fails then the entire task is considered as failed and the operator has to intervene. However, it may be the case that the mission doesn't want to stop and deal with the issue if only a few subtasks fail. Imagine your task has 5,000 images in it, and the requirements state that the mission shall process at least 90% of all images successfully. Well, you can set `maxFailedSubtaskCount` in that case to 500. If the number of failed subtasks is less than 500, then Ziggy will declare the task successful and move on to the next processing activity; if it's 501 or more, Ziggy will declare the task failed and an operator will need to address the issue.

The default for `maxFailedSubtaskCount` is zero.

The default for `maxFailedSubtaskCount` is zero.

#### reprocess

Most missions don't get all their data all at once. Most missions get data at some regular interval: every day, every month, whatever.

In that case, a lot of the time you'll only want to process the new data. If you've got 5 years of data in the datastore, and you get a monthly delivery, you don't want to process the 5 years of data you've already processed; you want to process only the data you just got. However: it may be that every once in a while you'll want to go back and process every byte since the beginning of the mission. Maybe you do this because your algorithms have improved. Maybe it's time series data, and you want to generate time series across the whole 5 years.

The `reprocess` parameter controls this behavior. When `reprocess` is true, Ziggy will try to process all the mission data, subject to the `taskDirectoryRegex` for each pipeline node. When it's set to false, Ziggy will only process new data.

The default for `reprocess` is `false`.

##### How does Ziggy Know Whether Data is "New" or "Old"?

Excellent question! Here's a long answer:

Ziggy keeps track of the "producer-consumer" information for every file in the datastore. That is, it knows which pipeline task produced each file, and it knows which pipeline tasks used each file as input ("consumed" it). Ziggy also knows what algorithm was employed by every task it's ever run.

Thus, when Ziggy marshals inputs for a new task, and that task's `TaskConfigurationParameters` instance has `reprocess` set to `false`, the system filters out any inputs that have been, at some point in the past, processed using the same algorithm as the task that's being marshaled. Result: a task directory that has only data files that haven't been processed before.

One subtlety: Ziggy not only tracks which pipeline tasks consumed a given file as input; it also looks to see whether they did so successfully. If a task consumes a file as input, but then the algorithm process that was running on that data bombs, Ziggy doesn't record that task as a consumer of that file. That way, the next time Ziggy processes "new" data, it will also include any "old" data that was only used in failed processing attempts.

The default for `reprocess` is `false`. 

##### How does Ziggy Know Whether Data is "New" or "Old"?

Excellent question! Here's a long answer:

Ziggy keeps track of the "producer-consumer" information for every file in the datastore. That is, it knows which pipeline task produced each file, and it knows which pipeline tasks used each file as input ("consumed" it). Ziggy also knows what algorithm was employed by every task it's ever run. 

Thus, when Ziggy marshals inputs for a new task, and that task's `TaskConfigurationParameters` instance has `reprocess` set to `false`, the system filters out any inputs that have been, at some point in the past, processed using the same algorithm as the task that's being marshaled. Result: a task directory that has only data files that haven't been processed before. 

One subtlety: Ziggy not only tracks which pipeline tasks consumed a given file as input; it also looks to see whether they did so successfully. If a task consumes a file as input, but then the algorithm process that was running on that data bombs, Ziggy doesn't record that task as a consumer of that file. That way, the next time Ziggy processes "new" data, it will also include any "old" data that was only used in failed processing attempts. 

#### reprocessingTasksExclude

Okay, this one's a bit complicated.

Imagine that you ran the pipeline and everything seemed fine, everything ran to completion. But then a subject matter expert looks at the results and sees that some of them are good, some are garbage. After some effort, the algorithm code is fixed to address the problems with the ones that are garbage, and you're ready to reprocess the tasks that produced garbage outputs.

How do you do that?

If you select `reprocess="false"`, nothing will get processed. As far as Ziggy is concerned, all the data was used in tasks that ran to completion, so there's nothing that needs to be processed.

If you select `reprocess="true"`, both the inputs that produced garbage and the ones that didn't will get reprocessed. If the fraction of inputs that produced garbage is small, you may not want to do this.

What you do is set `reprocess="true"`, but then you tell Ziggy to exclude the inputs that were used in tasks that produced good output. So imagine that tasks 101 to 110 were run, and only 105 is bad. To re-run the data from 105 in a new pipeline instance, you'd set `reprocessingTasksExclude="101, 102, 103, 104, 106, 107, 108, 109, 110"`.

In my experience, we haven't had to do this very often, but we have had to do it. So now you can, too!

The default for `reprocessingTasksExclude` is empty (i.e., don't exclude anything).

#### maxAutoResubmits

It will sometimes happen that a Ziggy task will finish in such a way that you actually want to resubmit the task to the processing system without skipping a beat. There are a couple of circumstances where this may happen:

- If you're uncertain of the settings you've put in for the amount of wall time your task should request, then it's possible that the task will reach its wall time limit before all the subtasks are processed. In this case, you would simply want to resubmit the task so that the remaining subtasks (or at least some of them) complete.
- If you're using an algorithm that has non-deterministic errors, the result will be that subtasks will "fail," but if the subtasks are re-run they will (probably) complete successfully. In this case, you would want to resubmit to get the handful of "failed" subtasks completed.

Because of issues like these, Ziggy has the capacity to automatically resubmit a task if it has incomplete or failed subtasks. The user can limit the number of automatic resubmits that the task gets; this prevents you from getting into an infinite loop of resubmits if something more serious is going wrong. the `maxAutoResubmits` parameter tells Ziggy how many times it may resubmit a task without your involvement before it gives up and waits for you to come along and see what's broken.

Note that this is a potentially dangerous option to use! The problem is that Ziggy can't tell the difference between a failure that a typical user would want to automatically resubmit, and a failure that is a real problem that needs human intervention (for example, a bug in the algorithm software). If the situation is in the second category, you can wind up with Ziggy resubmitting a task that's simply going to fail in exactly the same way as it did the first time it was submitted.

The default for `maxAutoResubmits` is zero.

[[Previous]](datastore-task-dir.md)
[[Up]](intermediate-topics.md)
[[Next]](properties.md)
