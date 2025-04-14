<!-- -*-visual-line-*- -->

[[Previous]](remote-environment.md)
[[Up]](configuring-pipeline.md)
[[Next]](building-pipeline.md)

## Pipelines and Nodes

The place where all that XML argle-bargle comes together is in the definition of pipelines, and the definition of the subunits of a pipeline (known as "pipeline nodes").

Let's take a look at the actual pipeline in [sample-pipeline.xml](../../sample-pipeline/etc/ziggy.d/sample-pipeline.xml). Here's the relevant content, with the comments removed to improve readability:

```xml
  <pipeline name="sample" description="Sample Pipeline" rootNodeNames="data-receipt">
    <parameterSet name="Algorithm Parameters"/>
    <node name="data-receipt" childNodeNames="permuter">
      <inputDataFileType name="raw data"/>
    </node>
    <node name="permuter" childNodeNames="flip">
      <inputDataFileType name="raw data"/>
      <outputDataFileType name="permuted colors"/>
      <modelType name="dummy model"/>
    </node>
    <node name="flip" childNodeNames="averaging">
      <inputDataFileType name="permuted colors"/>
      <outputDataFileType name="left-right flipped"/>
      <outputDataFileType name="up-down flipped"/>
    </node>
    <node name="averaging" singleSubtask="true">
      <inputDataFileType name="left-right flipped"/>
      <inputDataFileType name="up-down flipped"/>
      <outputDataFileType name="averaged image"/>
    </node>
  </pipeline>

```

Lots of stuff here! But let's start with the highest-level stuff.

### XML `pipeline` Element

This does exactly what you think it does: it defines a pipeline and includes all the information the pipeline needs in order to run. All that you need to specify in the `pipeline` element are the `name` (must be unique, can contain whitespace); the `description`; and the `rootNodeNames` (the names of the first step or steps to be run). 

### XML `node` Element

The `node`s in a `pipeline` are the algorithms (or the pipeline steps, if we're being pedantic about it.) Each node has one required attribute, its `name`. This must match the name of a `step` element (see [the article on pipeline algorithms](pipeline-algorithms.md)). Unless the node is the last node in a pipeline, it also needs a `childNodeNames` attribute to tell Ziggy which algorithms to run after this one is done. There's also an optional `singleSubtask` attribute (we'll get to that).

The way Ziggy runs a pipeline is to start by running the node or nodes in the pipeline's `rootNodeNames` attribute; when that's done, it runs the node or nodes in that node's `childNodeNames` attribute; and so on until the end of the pipeline is reached (or an error occurs). From looking at the XML above, we can see that the pipeline will run `data-receipt`, then `permuter`, then `flip`, then `averaging`. 

#### Wait, What? Data Receipt?

Notice that I've now told you 2 things that contradict each other. On the one hand, the user defines the pipeline nodes with `step` elements, and the root node of a pipeline (indeed, every node) must be a `step`. On the other hand, this pipeline's root node is `data-receipt`, which isn't a defined step! What up with that?

What up is the following:

Data receipt is a special case. Data receipt is a pre-defined step that takes data and instrument model files from some outside directory and transfers them to the datastore. This is the way that raw mission data and models get into the datastore in the first place. This is a sufficiently important, and generic, capability that Ziggy "ships" with a data receipt pipeline step built in. The user doesn't need to define that step; you can simply use it.

Note that data receipt is the only pre-defined step in Ziggy. There's more information on how it works in the article on [Data Receipt Execution Flow](data-receipt.md).

#### Wait, What? `singleSubtask`?

Based on our experience, most of the time, you'll be running algorithms that are "embarrassingly parallel," in which each data file in the datastore gets run in a separate process from any other. Consider for example the `permuter` step: this step takes an image and permutes its color map, then stores the resulting image back in the datastore. There are 4 images in the `set-1` data and 4 in the `set-2` data. Each of these is processed independently of all the others. Thus, Ziggy spins up 8 "subtasks" for processing, one subtask for each image. This is useful because subtasks can be run in parallel on a remote environment, which can really speed things up. 

Sometimes, though, you'll encounter an algorithm that needs all of the data files for a task, but can't process them all independently. Take for example the `averaging` step: it takes the 4 images from a data set and averages them together. This obviously can't be done on the 4 images in parallel! Somehow, we need to tell Ziggy that in this case it should provide all the data to a single processing activity. 

The way we do that is with the `singleSubtask` attribute. When Ziggy sees this, it spins up a single process per task, regardless of how many data files are to be processed by that task. 

### Parameter Sets

If you look back at [the article on algorithm parameters](algorithm-parameters.md), you'll see that we use `parameterSet` XML elements to represent a collection of parameters, and `parameter` elements within a `parameterSet` for the individual parameters. Of course, parameter sets are usually not global in scope. In some cases, one pipeline within a collection of pipelines needs a given parameter set; in other cases, some but not all of the nodes in a pipeline need a given parameter set. 

Ziggy allows parameter sets to be defined for a pipeline or for a node. This is done with a `parameterSet` element. 

The sample pipeline's sole parameter set is provided to all of its nodes. For this reason, the `parameterSet` element sits in the `pipeline` element. 

If, on the other hand, we wanted a particular parameter set to be made available to some nodes but not others, we would put the `parameterSet` element in the appropriate `node` elements. We didn't include an example of that here because life's too short. 

### Data File Types

Each of the nodes in the pipeline has one or more `inputDataFileType` elements and one or more `outputDataFileType` elements. These refer to the dataFileType definitions we encountered in [the article on the datastore](datastore.md). This defines for Ziggy what kinds of data files Ziggy should provide to the algorithm at runtime, and what kinds of data files Ziggy can expect the algorithm to produce. 

#### Output Data File Types

A question you might have is, "Do I need to have an output data file type for every kind of file my algorithm produces?" In our experience, lots of algorithms produce assorted diagnostic files that the users don't want to preserve forever, but which can be useful right after a pipeline run so the users can figure out whether something subtle went wrong (i.e., something that results in a wrong answer but not a crash). 

The answer is, you only need output data file types to represent the files you want to put into the datastore. Typically, these are outputs from the algorithm that will be used as inputs to another algorithm later on. Anything that doesn't go into the datastore doesn't need an output data file type. 

### Model Types

Like data file types, the user specifies which nodes need which models by including a `modelType` element in the node. 

[[Previous]](remote-environment.md)
[[Up]](configuring-pipeline.md)
[[Next]](building-pipeline.md)
