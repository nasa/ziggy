<!-- -*-visual-line-*- -->

[[Previous]](datastore.md)
[[Up]](configuring-pipeline.md)
[[Next]](pipelines-and-nodes.md)

## Remote Environments

At the start of this user manual, we stated that Ziggy is designed to be able to handle missions that generate terabytes of raw data per day. For data rates like that, it's self-evident that you can't possibly run algorithms of any kind on your laptop or workstation, or even a server. You'll need to go to much larger systems that can queue up jobs that run across vast numbers of compute nodes. 

These are what we're talking about when we say, "remote environments." A remote environment can be a High Performance Computing (HPC) system, a cloud system, or something more exotic. What all these systems have in common are two things:

- They have lots of compute nodes, often incorporating a variety of configurations as far as cores per node, RAM per node, bandwidth per node, and cost per node are concerned. 
- They have a batch queue system for the management of processing activities on their compute nodes, often incorporating a variety of different queues. 

### XML Definition of a Remote Environment

Let's take a look at the [sample remote environment XML file](../../sample-pipeline/etc/ziggy.d/sample-remote-environment.xml):

```xml
<remoteEnvironment name="sample" batchSystem="pbs" costUnit="$" description="Sample">
  	<architecture name="vax" description="VAX" cores="4" ramGigabytes="1" cost="1"
  		  bandwidthGbps="10"/>
  	<architecture name="alpha" description="Alpha" cores="16" ramGigabytes="4" cost="4"/>
  	<queue name="dec" description="DEC" maxWallTimeHours="8"/>
  	<queue name="compaq" description="Compaq" maxWallTimeHours="24"/>
  	<queue name="hpe" description="HPE" maxWallTimeHours="72"/>
</remoteEnvironment>  	
```

This is a fairly simple version of a remote environment, but it does demonstrate the essential features of the species. 

#### The `remoteEnvironment` Element

The `remoteEnvironment` XML element has four attributes, and then contains within it additional XML elements that define the queues and the compute node architectures. The attributes are:

- Attribute `name`, which as usual must be unique and is required
- Attribute `batchSystem`, which specifies the batch system the environment uses and is required
- Attribute `costUnit`, which specifies the way that the environment accounts for usage (in this example it's dollars, hence "$", but it could be anything); this is an optional text string
- Attribute `description`, which somewhat unusually is actually required, and serves to provide additional information regarding the environment.

At the moment, the only supported batch system is PBS. We're totally willing to add any batch system you need, so if you need something different send us an e-mail! You could also try to implement a batch system yourself and submit it as a pull request on GitHub, but that's probably more complicated than anything the typical user will want to deal with.

#### The `architecture` Element

An architecture defines a type of compute node. The attributes of an `architecture` are:

- Attribute `name`, which is required and must be unique within a given remote environment; this has to be the text string that the batch system uses to specify that the batch job should use this architecture
- Attribute `description`, a required text string that can be a more "human-friendly" descriptor for the architecture than the `name` attribute (remember that name has to be whatever the batch system calls this architecture, which may not be what humans want to call it)
- Attribute `cores`, which is required and specifies the number of cores per node
- Attribute `ramGigabytes`, which is required and specifies the amount of RAM per node, in gigabytes
- Attribute `cost`, which represents the cost per hour per node for this architecture; the cost is in whatever `costUnit` is specified for the remote environment
- Attribute `bandwidthGbps`, which specifies the network bandwidth for all compute nodes with this architecture; this is an optional attribute.

#### The `queue` Element

This describes a batch queue that is available on the remote environment. The attributes of a `queue` element are:

- Attribute `name`, which is required and must be unique within a given remote environment; this has to be the text string that the batch system uses to specify the queue requested by the user
- Attribute `description`, which is required and is a more human-friendly descriptor for the queue
- Attribute `maxWallTimeHours`, which is optional and specifies the elapsed wall time for jobs in this queue before the batch system pulls the plug on them.

### More Complicated Queue Elements

The queues in the sample remote environment are somewhat rudimentary. The sample pipeline also provides an XML file that describes the actual HECC (High End Computing Capability) supercomputer at NASA Ames Research Center (sometimes referred to as "the NAS", after the NASA Advanced Supercomputer organization within NASA). Let's look at the queue definitions in [hecc-environment.xml](../../sample-pipeline/config/hecc-environment.xml):

```xml
<queue name="low" description="Low" maxWallTimeHours="4"/>
<queue name="normal" description="Normal" maxWallTimeHours="8"/>
<queue name="long" description="Long" maxWallTimeHours="120"/>
<queue name="debug" description="Debug" maxWallTimeHours="2" maxNodes="128"
       autoSelectable="false"/>
<queue name="devel" description="Development" maxWallTimeHours="2" maxNodes="512"
       autoSelectable="false"/>
<queue name="reserved" description="Reserved" autoSelectable="false" reserved="true"/>
```

Here we see that `queue` elements can have some additional, optional attributes.

#### Attribute `maxNodes`

Some batch queues will allow the user to ask for as many compute nodes as they want for a given job, but others apply limits to these requests. If the queue does apply limits, the `maxNodes` attribute can specify the limit.

#### Attribute `reserved`

On some systems, users can request a special-purpose, only-for-me (or only-for-my-group) queue. In these cases, the system administrators will often create a queue with some kind of special name that's not the name of any of the regular queues. 

One way to handle this situation is to add the reserved queue, with its special name, to the XML files and then re-import it. Unfortunately, this becomes unwieldy after you've called and requested reserved queues a few times, especially since the name of the queue is probably different on every request!

The alternative is to add a `queue` element to the remote environment that has optional attribute `reserved` set to `true`. This tells Ziggy that it's going to need to get some additional information from you (specifically, the name of the special-purpose queue) before it can submit any jobs (we'll see later exactly how you do this).

#### Attribute `autoSelectable`

To understand this, we need to have some spoilers for stuff you'll encounter properly at in later articles. 

When you go to set up remote jobs, you often don't actually know which architecture or which queue you should use, or even how long your jobs will take. Ziggy's remote execution UI asks you to provide some basic information, which it then uses to estimate how much the jobs will cost, how long they'll run, and which architecture is the best for your purposes. As part of this, it looks for the correct queue, based on the `maxWallTimeHours` attribute and its estimate of how much wall time you'll need. Ziggy will then present its recommendations for things like the architecture and the queue.

However! There are some batch queues that you probably don't want Ziggy to select for you, under any circumstances. These are batch queues that the user should select manually if they want them. The way you tell Ziggy that it should never select a given batch queue as part of its automatic process is by setting the `autoSelectable` attribute to `false`. 

Notice in the queues shown above that the `debug`, `devel`, and `reserved` queues are not auto-selectable. For the `reserved` queue, this is self-explanatory: you may not have a queue reserved for your use, in which case you sure don't want Ziggy to pick that option for you! For the `debug` and `devel` queues, these queues are intended for testing activities that are extremely limited in scale. Given that, you clearly won't want Ziggy deciding that one of these queues is a good one for your 47 terabyte processing activity. 

### How Do I Use All This?

Notice that this article tells you only how to describe a remote environment to Ziggy so that you can make use of it. It doesn't actually tell you how to set things up so that your 47 terabyte processing activity is sent to the correct remote environment, rather than trying to run it on your laptop. 

Not to worry, though: The [high performance computing](select-hpc.md) articles explain in fabulous detail how to go about setting up remote execution. Feel free to jump ahead if you want to. We'll understand.

[[Previous]](datastore.md)
[[Up]](configuring-pipeline.md)
[[Next]](pipelines-and-nodes.md)