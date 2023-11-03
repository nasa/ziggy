<!-- -*-visual-line-*- -->

[[Previous]](select-hpc.md)
[[Up]](select-hpc.md)
[[Next]](remote-dialog.md)

## Remote Parameters

The way that you set up a pipeline module to run on a remote (i.e., high-performance computing / cloud computing) system is to create a `ParameterSet` of the `RemoteParameters` class, and then make it a module parameter set for the desired node.

Wow! What does all that mean? Let's start with the second half: "make it a module parameter set for the desired node." If you look at `pd-sample.xml`, you'll see this:

```xml
<node moduleName="permuter" childNodeNames="flip">
  <inputDataFileType name="raw data"/>
  <outputDataFileType name="permuted colors"/>
  <modelType name="dummy model"/>
  <moduleParameter name="Remote Parameters (permute color)"/>
  <moduleParameter name="Multiple subtask configuration"/>
</node>
```

That line that says `<moduleParameter name="Remote Parameters (permute color)"/> `tells Ziggy that there's a parameter set with the name `Remote Parameters (permute color)` that it should connect to tasks that run the `permuter` module in the sample pipeline.

Now consider the first part of the sentence: "create a `ParameterSet` of the `RemoteParameters` class." If you look at pl-sample.xml, you'll see this:

```xml
<parameter-set name="Remote Parameters (permute color)"
               classname="gov.nasa.ziggy.module.remote.RemoteParameters">
  <parameter name="enabled" value="false"/>
  <parameter name="gigsPerSubtask" value="10"/>
  <parameter name="subtaskMaxWallTimeHours" value="0.15"/>
  <parameter name="subtaskTypicalWallTimeHours" value="0.15"/>
  <parameter name="optimizer" value="COST"/>
  <parameter name="nodeSharing" value="true"/>
  <parameter name="wallTimeScaling" value="false"/>
</parameter-set>
```

This is the parameter set that, we saw above, got attached to the `permuter` node.

Let's go back to that sentence: all we need to do to run on a supercomputer is to have one of these parameter sets in the parameter library file, and tell the relevant node to use it. Is that true?

Yes. Pretty much. But as always, the deity is in the details.

### RemoteParameters class

Unlike the parameter sets you'll want to construct for use by algorithms, the parameter set above is supported by a Java class in Ziggy: `gov.nasa.ziggy.module.remote.RemoteParameters`. This means you'll need to include that XML attribute for any parameter set that you want to use to control remote execution, and that you can't make up your own parameters for the parameter set; you'll need to stick to the ones that the Java class defines (but on the other hand you won't need to specify the data types, since the definition of `RemoteParameters` does that for you).

The `RemoteParameters` class has a lot of parameters, but there are only four that you, personally, must set. These can be set either in the parameter library file or via the [module parameters editor](change-param-values.md) on the console. The remainder can, in principle, be calculated on your behalf when Ziggy goes to submit your jobs via the Portable Batch System (PBS). In practice, it may be the case that you don't like the values that Ziggy selects when left to its own devices. For this reason, you can specify your own values for the optional parameters. Ziggy will still calculate values for the optional parameters you leave blank. In this case, Ziggy's calculated parameters will always (a) result in parameters that provide sufficient compute resources to run the job, while (b) taking into account the values you have specified for any of the optional parameters.

Note that, rather than setting optional parameters via the module parameters editor, there's an entire separate system in Ziggy that allows you to try out parameter values, see what Ziggy calculates for the remaining optional parameters, and make changes until you are satisfied with the result. This is the [remote execution dialog](remote-dialog.md).

Anyway, let's talk now about all those parameters.

#### Required Parameters

##### enabled (boolean)

The `enabled` parameter does what it sounds like: if `enabled` is true, the node will use remote execution; if it's false, it will run locally (i.e., on the same system where the supervisor process runs).

Note that, since you can edit the parameters, you can use this parameter to decide at runtime whether to run locally or remotely.

##### minSubtasksForRemoteExecution (int)

One thing about remote execution is that you may not know whether you want remote execution when you're about to submit the task. How can that happen? The main way is that you might not know at that time how many subtasks need to run! You may be in a situation where if there's only a few subtasks you'd rather run locally, but if there are more you'll use HPC.

Rather than force you to figure out the number of subtasks and manually select or deselect `enabled`, Ziggy provides a way to override the `enabled` parameter and force it to execute locally if the number of subtasks is small enough. This is controlled by the `minSubtasksForRemoteExecution` parameter. If remote execution is enabled, Ziggy will determine the number of subtasks that need to be run in the task. If the number is smaller than `minSubtasksForRemoteExecution`, Ziggy will execute the task locally.

The default value for this parameter is zero, meaning that Ziggy will always run remotely regardless of how many or how few subtasks need to be processed.

##### subtaskTypicalWallTimeHours and subtaskMaxWallTimeHours (float)

"Wall Time" is a $10 word meaning, "Actual time, as measured by a clock on the wall." This term is used to distinguish it from values like "CPU time" (which is wall time multiplied by the number of CPUs), or other compute concepts that refer to time in some way. Compute nodes in HPC systems are typically reserved in units of wall time, rather than CPU time or any other parameter.

When Ziggy is computing the total resources needed for a particular task, it needs to know the wall time that a single, typical subtask would need to run from start to finish. With that information, and the total number of subtasks, it can figure out how many compute nodes will be needed, and how much wall time is needed for each compute node. The typical subtask start-to-finish time is specified as the `subtaskTypicalWallTimeHours`.

That said: for some algorithms and some data, there will be subtasks that take much longer than the typical subtask to run from start to finish. Consider, for example, an algorithm that can process the typical subtask in 1 hour, but needs 10 hours for some small number of subtasks. If you ignore the handful of tasks that need 10 hours, you (or Ziggy) might be tempted to ask for a large number of compute nodes, with 1 hour of wall time for each. If you do this, the subtasks that need 10 hours won't finish. Your requested compute resources need to take into account those long-running subtasks. (This is sometimes analogized as, "If the brownie recipe says bake at 350 degrees for 1 hour, you can't bake at 700 degrees for 30 minutes instead." In this case, a more accurate analogy would be, "You can't split the brownie batter into 2 batches and bake each batch at 350 degrees for 30 minutes.")

To address this, you specify the `subtaskMaxWallTimeHours`. Ziggy guarantees that it won't ask for a wall time less than this value, which should (fingers crossed!) ensure that all subtasks finish.

One thing about this: Ziggy will ask for a wall time that's sufficient for the `subtaskMaxWallTimeHours` parameter, but it will ask for a total number of CPU hours that's determined by the subtask count and the `subtaskTypicalWallTimeHours`, under the assumption that the number of long-running subtasks is small compared to the number of typical ones. Imagine for example that you have 1000 subtasks, with a typical wall time of 1 hour and a max wall time of 10 hours; and you're trying to run on a system where the compute nodes have 10 CPUs each. The typical usage would be satisfied by getting 100 compute nodes for 1 hour each, but that leaves the long-running subtasks high and dry. The `subtaskMaxWallTimeHours` tells Ziggy that it needs to ask for 10 hour wall times for this task; thus it will ask for 10 hour wall times, but only 10 compute nodes total.

##### gigsPerSubtask (float)

This parameter tells Ziggy how many GB each subtask will need at its peak.

The reason this needs to be specified is that the compute nodes on your typical HPC facility have some number of CPUs and some amount of RAM per compute node. By default, Ziggy would like to utilize all the cores on all the compute nodes it requests. Unfortunately, it's not guaranteed that each subtask can get by with an amount of RAM given by node total RAM / node total CPUs; if the subtasks need more than this amount of RAM, then running subtasks on all the CPUs of a compute node simultaneously will run out of RAM. Which is bad.

The specification of `gigsPerSubtask` allows Ziggy to figure out the maximum number of CPUs on each compute node that can run simultaneously, which in turn ensures that it asks for enough compute nodes when taking into account that it may not be possible to run all the cores on the nodes simultaneously.

#### Optional Parameters

##### optimizer (String)

Typical HPC systems and cloud computing facilities have a variety of compute nodes. The different flavors of compute nodes will have different numbers of cores, different amounts of RAM, different costs for use, and different levels of demand (translating to different wait times to get nodes). Ziggy will use the information about these parameters to select a node architecture.

There are four optimizers that Ziggy allows you to use:

**COST:** This is the default optimization. Ziggy looks for the node that will result in the lowest cost, taking into account the different per-node costs and different capabilities of the different node architectures (because the cheapest node architecture on a per-node basis might lead to a solution that needs more nodes or more hours, so the "cheapest" node may not result in the cheapest jobs).

**CORES:** This attempts to minimize the fraction of CPUs left idled. If the subtasks need a lot of RAM, it will optimize for nodes with more RAM per CPU. If there are multiple architectures that have the same idled core fraction (for example, if all of the architectures can run 100% of their nodes), then the lowest-cost solution from the set of "semifinalist" architectures will be picked.

**QUEUE_DEPTH:** This is one of the optimizers that tries to minimize the time spent waiting in the queue. The issue here is that some architectures are in greater demand than others. The `QUEUE_DEPTH` optimiztaion looks at each architecture's queued jobs and calculates the time it would take to run all of them. The architecture that has the shortest time based on this metric wins.

**QUEUE_TIME:** This is a different optimization related to queues, but in this case it attempts to minimize the total time you spend waiting for results (the time in queue plus the time spent running the jobs). This looks at each architecture and computes the amount of queue time "overhead" that typical jobs are seeing. The architecture that produces the shortest total time (queue time plus execution time) wins.

###### A Note About Queue Optimizations

The optimization options for queue depth and queue time are only approximate, and can potentially wind up being very wrong. This is because the queue management is sufficiently complicated that the current estimates are only modestly reliable predictors of performance.

What makes the management complicated? For one thing, the fact that new jobs are always being submitted to the queues, and there's no way to predict what gets submitted between now and when your job runs. Depending on the number, size, and priority of jobs that come in before your job runs, these might move ahead of your jobs in the queue. Relatedly, if a user decides to delete a job that's in the queue ahead of you, that represents an unpredictable occurrence that improves your waiting time. Jobs that don't take as long as their full wall time are another unpredictable effect.

Anyway. The point is, caveat emptor.

##### remoteNodeArchitecture (String)

All of the foregoing is about selecting an architecture from the assorted ones that are available on your friendly neighborhood HPC facility. However: it may be the case that this isn't really a free parameter for you! For example: if you have compute nodes reserved, you probably have nodes with a specific architecture reserved. If for this reason, or any other, you want to specify an architecture, use this parameter.

##### subtasksPerCore (float)

Ziggy will generally gravitate to a solution in which all the subtasks in a task run simultaneously, which means it will ask for a lot of compute nodes. This parameter allows the user to force Ziggy to a solution that has fewer nodes but for more wall time. A `subtasksPerCore` of 1.0 means all the subtasks run in parallel all at the same time. A value of 2.0 means that 50% of the tasks will run in parallel and the remainder will wait for tasks in the first "wave" to finish before they can run.

##### maxNodes (int)

This is a more direct way to force Ziggy to a solution with a smaller number of nodes than it would ordinarily request. When this is set, Ziggy will not pursue any solution that uses more compute nodes than the `maxNodes` value. This will of course result in longer wall times than if you just ask for as many nodes as it takes to finish as fast as possible, but asking for thousands of nodes for 30 seconds each may get you talked about in the control room, and not in a good way.

Note that Ziggy can request a number of nodes that is smaller than the value for `maxNodes` (which is why it's called `maxNodes` in the first place). This happens if the number of subtasks is small: if you only have 8 subtasks, and `maxNodes` is set to 30, it would clearly be useless to actually ask for 30 nodes, since most of them will be idle but you'll get charged for them anyway. In these sorts of situations (where even the value of `maxNodes` is too large, given the number of subtasks to process), Ziggy will select a number of nodes that ensures that none of the nodes sits idle.

Note that Ziggy can request a number of nodes that is smaller than the value for `maxNodes` (which is why it's called `maxNodes` in the first place). This happens if the number of subtasks is small: if you only have 8 subtasks, and `maxNodes` is set to 30, it would clearly be useless to actually ask for 30 nodes, since most of them will be idle but you'll get charged for them anyway. In these sorts of situations (where even the value of `maxNodes` is too large, given the number of subtasks to process), Ziggy will select a number of nodes that ensures that none of the nodes sits idle.

##### nodeSharing (boolean)

In all the foregoing, we've assumed that the algorithm will permit multiple subtasks to run in parallel on a given compute node (albeit using different CPUs). In some cases, this proves to be untrue! For example, there are algorithms that have their own, internal concurrency support, but they rely on a single process having the use of all the CPUs. If this is the case for your algorithm, then set `nodeSharing` to `false`. This will tell Ziggy that each compute node can only process one subtask at a time, and that it should book nodes and wall times accordingly.

##### wallTimeScaling (boolean)

Related to the above: if your algorithm can use all the CPUs on a compute node, it's obviously going to run faster on compute nodes with more CPUs than on nodes with fewer CPUs. But this leads to a problem: how can Ziggy select the correct parameters when the actual run time depends on number of cores, which depends on architecture?

To avoid having to retype the wall time parameters whenever Ziggy wants a different architecture, enter the wall time parameters that would be valid in the absence of concurrency (i.e., how long it would take to run your algorithm on 1 CPU), and set `wallTimeScaling` to true. When this is set, Ziggy knows that it has to scale the actual wall time per subtask down based on the number of CPUs in each node. Ziggy will assume a simple linear scaling, i.e.: true wall time = wall time parameter / CPUs per node. This probably isn't quite correct, but hopefully is correct enough.

#### A Note on Setting Optional Parameters

When you set some of the optional parameters, Ziggy will compute values for all the rest. Ziggy has 2 requirements for this process. First, it **must** use any optional parameter values you specify, it can never change those values. Second, it **must** produce a result that allows the task to run to completion. That is, it has to select enough compute nodes and enough wall time to process all of the subtasks.

A close reading of the paragraph above reveals a potential problem: what happens if you specify a set of parameters that makes it impossible for Ziggy to satisfy that second requirement? Just as an example, if you set the maximum number of nodes, the compute node architecture, and the requested wall time, it's possible to ask for so little in total compute resources that it's impossible to run all of your subtasks!

If Ziggy determines that it can't ask for enough resources to run your task, it will throw an exception at runtime, and your pipeline will stop. You'll then need to adjust the parameters and restart.

The best way to avoid this outcome is to use the [remote execution dialog](remote-dialog.md) to set the optional parameters. The remote execution dialog won't allow you to save your parameter values if they result in tasks that can't finish because they're starved of compute resources.

[[Previous]](select-hpc.md)
[[Up]](select-hpc.md)
[[Next]](remote-dialog.md)