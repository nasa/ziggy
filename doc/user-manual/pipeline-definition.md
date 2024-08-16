<!-- -*-visual-line-*- -->

[[Previous]](datastore.md)
[[Up]](configuring-pipeline.md)
[[Next]](building-pipeline.md)

## Pipeline Definition

The pipeline definition files are where all the pieces get put together. Ziggy expects these files to have names that start with "pd-" (for "Pipeline Definition"). In the case of the sample pipeline, the file is [config/pd-sample.xml](../../sample-pipeline/config/pd-sample.xml). There can be more than one pipeline definition file. These files define the pipelines themselves, but first they define the individual algorithm elements of the pipelines, the modules.

### Pipeline Modules

Here's the pipeline module definitions for the sample pipeline:

```xml
<module name="permuter" description="Color Permuter"/>
<module name="flip" executableName="flipper" description="Flip Up-Down and Left-Right"/>
<module name="averaging" description="Average Images Together"/>
```

Pretty simple. The main thing is that the name of the module is the name that Ziggy will look for when it comes time to execute the module in question. Thus, **each module must be an executable file that can run without parameters.** Put differently, when the permuter module runs, Ziggy will look for an executable named `permuter` and start it. Done and done.

#### Module names and Executable Names

If you look at the pipeline modules above, you'll see that one of these things is not like the other. Specifically, the `flip` pipeline module has an extra attribute, `executableName`, which is set to `flipper`. What's the purpose of this? 

The purpose is to tell Ziggy that the `flip` pipeline module doesn't run an executable named `flip`; it runs one named `flipper`. If a module's executable name is absent, Ziggy will use the module name, but if the executable name is set, it overrides the module name when Ziggy goes looking for the executable it's supposed to run. 

Why would anyone want to do that? 

The reason is that module names must all be unique. Usually this is super-easy, barely an inconvenience, but occasionally you may encounter a situation in which you want to use the same executable more than once in a given pipeline. In this case, you would encounter an issue, which is: each node in any pipeline has to have the name of its module; all module names must be unique; and each module can be used only once in any given pipeline.  Put this all together, and you'd find that you need multiple copies of the algorithm code, each of which has a unique name that matches the module name. Ugly!

The `executableName` attribute gives you a way out of this. With `executableName` attributes, you can have two pipeline modules (with names, say, `flip` and `anti-flip`), each of which runs the same executable (say, `flipper`). You can now write a pipeline that calls `flip` at one point and `anti-flip` at some other point in execution, and each of them runs the `flipper` executable. 

### Pipelines

At last, the main event! The pipeline definition starts with something like this:

​    `<pipeline name="sample" description="Sample Pipeline" rootNodeNames="data-receipt">`

A name, a description and "root node names." The root node name(s) are the pipeline modules that are executed first when the pipeline starts. In this case, `data-receipt` is executed first. If the pipeline had called for multiple pipeline modules running in parallel at this first step, we could specify a comma-separated list of module names in `rootNodeNames`.

#### Wait, What? Data Receipt?

Notice that I've now told you 2 things that contradict each other. On the one hand, the user defines the pipeline modules with `module` elements, and the root node of a pipeline is a pipeline module. On the other hand, this pipeline's root node is `data-receipt`, which isn't a defined node! What up with that?

What up is the following:

Data receipt is a special case. Data receipt is a pre-defined module that takes data and instrument model files from some outside directory and transfers them to the datastore. This is the way that raw mission data and models get into the datastore in the first place. This is a sufficiently important, and generic, capability that Ziggy "ships" with a data receipt pipeline module built in. The user doesn't need to define that module; you can simply use it.

Note that data receipt is the only pre-defined module in Ziggy. There's more information on how it works in the article on [Data Receipt Execution Flow](data-receipt.md).

#### Back to the Pipeline Definition

The next chunk of the pipeline definition is thus (minus comments, which I removed in the interest of brevity):

```xml
  <parameterSet name="Algorithm Parameters"/>

  <node moduleName="data-receipt" childNodeNames="permuter">
    <inputDataFileType name="raw data"/>
  </node>

  <node moduleName="permuter" childNodeNames="flip">
    <inputDataFileType name="raw data"/>
    <outputDataFileType name="permuted colors"/>
    <modelType name="dummy model"/>
  </node>
```

Each step in the pipeline is a node. The `node` specifies the name of the module for that node and the name of any nodes that execute next, as `childNodeNames`. Here we see the `data-receipt` node is followed by `permuter`, and `permuter` is followed by `flip`.

##### Parameter Sets

Parameter sets can be supplied for either the entire pipeline as a whole, or else for individual nodes.

In the text above we see a `parameterSet` named `Algorithm Parameters` that is defined outside of the nodes. This means that the `Algorithm Parameters` set from `pl-sample.xml` will be provided to each and every module when it starts to execute. On the other hand, it's possible to imagine that the permuter module would have some parameters that it needs but which aren't used by the other modules. To do this, we would put a `parameterSet` element into the `permuter` node definition. Here's what that would look like:

```xml
  <node moduleName="permuter" childNodeNames="flip">
    <inputDataFileType name="raw data"/>
    <outputDataFileType name="permuted colors"/>
    <parameterSet name="Some other parameter set"/>
    <modelType name="dummy model"/>
  </node>
```

A given parameter set can be provided as a `parameterSet` to any number of nodes. For example, if we wanted to provide `Some other parameter set` to both `permuter` and `flip`, but not to `data-receipt` or `average`, we could simply copy the `parameterSet` element from the `permuter` node definition into the `flip` node definition.

##### Data File and Model Types

Each node can have `inputDataFileType`, `outputDataFileType`, and `modelType` elements. This is how the user defines what file types are used for inputs, which for outputs, and which models are needed for each node. Here we see that the `raw data` type is used as input for `data-receipt` and for `permuter`. The `permuted colors` type is the output type for permuter, and it uses the `dummy model` model type. Thus: we have now defined the names of the files that will be used as the input to the permuter node, and the ones that will be produced as output.

A node can have multiple output types (see for example the `flip` node in `pd-sample.xml`) or multiple input types (as in the `averaging` node). Each node can use any combination of model types it requires, and each model type can be provided to as many nodes as need it.

[[Previous]](datastore.md)
[[Up]](configuring-pipeline.md)
[[Next]](building-pipeline.md)
