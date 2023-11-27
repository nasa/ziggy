<!-- -*-visual-line-*- -->

[[Previous]](halt-tasks.md)
[[Up]](user-manual.md)
[[Next]](remote-parameters.md)

## High Performance Computing Overview

Large data volumes and/or compute-intensive processing algorithms demand the use of equally large-scale  computing hardware to perform the processing. Fortunately, Ziggy supports the use of High Performance Computing (HPC) facilities.

### What Kind of HPC Facilities?

"High Performance Computing" can mean a bunch of things, including commercial cloud computing systems. In this case, it specifically means [the systems at the NASA Advanced Supercomputer (NAS)](https://www.nas.nasa.gov/hecc/).

At the moment, the NAS is the only supported HPC option for Ziggy. This is mainly because it's the only one that any of the Ziggy developers have had access to, and it's hard to support an HPC system if you can't even log on to it. That said, if you have such a system and would like us to adapt Ziggy to use it, I'm sure we could do that without much ado (assuming you get us accounts).

In the near future the Ziggy team hopes to add cloud computing support for cloud systems that are supported by NASA. We'll keep you posted.

### The RemoteParameters Parameter Set

Let's look again at the XML that defined the sample pipeline. At one point, you can see this:

```xml
<node moduleName="permuter" childNodeNames="flip">
  <inputDataFileType name="raw data"/>
  <outputDataFileType name="permuted colors"/>
  <modelType name="dummy model"/>
  <moduleParameter name="Remote Parameters (permute color)"/>
  <moduleParameter name="Multiple subtask configuration"/>
</node>
```

 The parameter set `Remote Parameters (permute color)` is defined in the parameter library XML file:

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

The parameter set has a Java class associated with it, which as you've probably gathered means that it's a parameter set that Ziggy uses for its own management purposes.

#### Enabling Remote Execution

If you change the value of `enabled` to true, then any node that includes this parameter set will try to run on HPC.

That's all it takes. Well, that and access to the NAS.

Note the implications here. First, for a given pipeline the user can select which nodes will run on HPC and which will run locally (here "locally" means "on the system that hosts the supervisor process", or more generally, "on the system where the cluster runs"). Second, even for nodes that have an instance of `RemoteParameters` connected to them, you can decide at runtime whether you want to run locally or on HPC!

The RemoteParameters class is discussed in greater detail in [the Remote Parameters article](remote-parameters.md).

#### HPC Execution Flow

When Ziggy runs a task that's configured to go on HPC, it issues commands to the HPC's batch system (in the case of the NAS, that's the [Portable Batch System](https://www.altair.com/pbs-professional), or PBS). The PBS commands specify the number of compute nodes to be used and the allowed wall time (i.e., how long you have the use of the nodes before the HPC repos them). While the PBS commands are being issued, the task status will be `ALGORITHM_SUBMITTING`, or `As`. Once each PBS submission has been accepted, the task will go to `ALGORITHM_QUEUED`, or `Aq`. Depending on how heavily loaded the queues are, sooner or later your jobs will start to run on the HPC system, at which time the task goes to `ALGORITHM_EXECUTING`, `Ae`.

#### Tasks and Jobs

Depending on your parameters, it's often the case that your remote execution of a task will need more than 1 compute node. HPC systems generally allow users to submit jobs that request multiple nodes; nevertheless, Ziggy doesn't do this. What it does is submit multiple jobs, each of which requests 1 node. Each of these 1-node jobs has a system by which it identifies subtasks that need processing and allocates them to itself as resources permit.

The reason we do it this way is that it improves throughput: if the HPC system has a single available node, and it has a job that only needs 1 node, it will often allocate that node to that job. If, on the other hand, you have a job that needs 10 nodes, you wind up waiting until 10 nodes are available. With more small jobs you spend less time waiting for execution to begin.

For the most part, you don't really need to know about this except for the fact that each of the jobs has its own algorithm log file. If you have, say, 10 jobs that are processing task 12 in instance 3, and the module is permuter, then the algorithm logs will be `3-12-permuter.0-1.log`, `3-12-permuter.1-1.log`, ... `3-12-permuter.9-1.log`. When you use the GUI to display logs, all of these logs will appear as separate entries.

#### When Your Jobs Time Out

If you haven't set the parameters for remote execution correctly, it's possible that your HPC jobs will reach their wall time limits and exit while there are still subtasks waiting to be processed. If this happens, the task will go to state `ERROR` and P-state `Ac` (`ALGORITHM_COMPLETE`). What then?

One option is to resubmit the task to PBS via the `Resubmit` option on the task menu (see the article [Re-run or Resume a Failed Task](rerun-task.md) for details). When you do this, Ziggy will automatically adjust the PBS request based on how many subtasks are left. That is, if 80% of subtasks ran successfully, on resubmit Ziggy will only ask for 20% as much compute capacity as it asked for the first time.

Given that the first request didn't have enough resources to run the whole task, it's likely the second one won't have enough to run the remaining tasks. Given this, before you resubmit you might want to adjust the parameters so that you get more nodes and/or wall time.

BUT WAIT! Didn't we say earlier that once a parameter set is sent to a pipeline instance, that instance will always use the set as it was at that moment? That is, that changes to the parameters don't get propagated to existing instances and tasks?

Yes, we did, and that's true. In general. But the exception is `RemoteParameters`. When Ziggy runs or reruns tasks, it always goes and gets the current values of any `RemoteParameters` instance. This is because we've encountered this exact issue so frequently that we realized that it's essential to be able to fiddle with the parameters that are used for remote job submissions. Also, the values in `RemoteParameters` can't affect the science results. Because of this, there's no need to rigorously track the exact values of `RemoteParameters` used in any particular processing activity.

In the event that a really small number of subtasks haven't run when your jobs exit, there's something else you can do: you can set `enabled` to false and resubmit the task. The remaining subtasks will then run locally! This can be useful in cases where there's so few subtasks left that they can't even use up a whole compute node, or if the run time on the local system is so small that it makes sense to run locally rather than going back into the PBS queue.

##### Options for Automatic Resubmission

In the discussion above, we suggested that, in the event that execution times out, you can resubmit the task and the subtasks that didn't run to completion will then be run (subject to the possibility that some of them will then also time out). We also offered the option to disable remote execution if the number of subtasks gets small enough that your local system (i.e., the system that runs the supervisor process) can readily handle them. This is all true, but it can be a nuisance. For this reason, there are some additional options to consider.

First: Ziggy can automatically resubmit tasks that don't complete successfully. This is discussed in [the article on TaskConfigurationParameters](task-configuration.md). You can specify that, in the event that a task doesn't complete, Ziggy should resubmit it. In fact, you can specify the number of times that Ziggy should resubmit the task: after the number of automatic resubmits is exhausted, the task will wait in the ALGORITHM_COMPLETE processing state for you to decide what to do with it (i.e., try to resubmit it again, or fix underlying software problems, or decide that the number of completed subtasks is sufficient and that you want to move on to persisting the results).

At this point, we need to reiterate the warning in the TaskConfigurationParameters article regarding this parameter: Ziggy can't tell the difference between a task that didn't finish because it ran out of wall time and a task that didn't finish because of a bug in the algorithm code somewhere. If there's an algorithm bug, Ziggy will nonetheless resubmit an incomplete task (because Ziggy doesn't know the problem is a bug), and the task will fail again when it hits that bug.

Second: Ziggy allows you to automate the decision on whether a given number of subtasks is too small to bother with remote execution. The automation is a bit crude: as shown in [the RemoteParameters article](remote-parameters.md), there is an option, `minSubtasksForRemoteExecution`. This option does what it says: it sets the minimum number of subtasks that are needed to send a task to remote execution; if the number of subtasks is below this number, the task will run locally, **even if enabled is set to true**!

By using these two parameters, you can, in effect, tell Ziggy in advance about your decisions about whether to resubmit a task and whether to use remote execution even if the number of subtasks to process is fairly small.

[[Previous]](halt-tasks.md)
[[Up]](user-manual.md)
[[Next]](remote-parameters.md)
