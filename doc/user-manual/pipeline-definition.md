<!-- -*-visual-line-*- -->

[[Previous]](configuring-pipeline.md)
[[Up]](user-manual.md)
[[Next]](pipelines-and-nodes.md)

## Pipeline Definition

When we say, "pipeline defintion", what do we mean by that? Well, if you take a look at the [architecture diagram](images/architecture-diagram.png), you'll see a box labeled, "Pipeline Definition (XML)," and based on the color code of the diagram, it's one of those things that the user needs to implements. Here's where we tell you what that means!

### What That Means

The pipeline definition XML files, like it says on the label, defines all of the content of your pipeline or pipelines. This means it includes:

- The actual pipelines themselves -- the ordered list of processing operations that each pipeline will execute. The processing operations themselves are called "pipeline nodes."
- The collections of parameters that the nodes will use to, well, tell them what parameters to use at runtime.
- The collection of algorithms (or "pipeline steps") that perform the actual processing.
- Definitions of data file and model types that are used in processing, and the assignment of these to pipeline nodes. 
- The layout of the datastore directories, where Ziggy stores all its data files and all its model files.
- Information on any remote environments you can use for execution of your algorithms -- cloud computing or HPC environments. 

If you look at the [sample-pipeline.xml file](../../sample-pipeline/etc/ziggy.d/sample-pipeline.xml), you'll see all the pipeline definition argle-bargle for the sample pipeline, with the exception of the remote environments. The remote environments are defined in [the HECC environment file](../../sample-pipeline/etc/ziggy.d/hecc-environment.xml), which is the one you would use on NASA's supercomputer at the High End Computing Capability (HECC) center; and in [the sample environment file](../../sample-pipeline/etc/ziggy.d/sample-remote-environment.xml), which defines a totally fictitious remote environment.

At this point, I don't recommend that you get put off by this odd-looking document. We'll go through each of its bits and bobs in the next few sections. The one thing you should note is the <pipelineDefinition> and </pipelineDefiniton> tags in the file. These define the start and end of any and all pipeline definition elements, so each XML file needs to start with <pipelineDefinition> and end with </pipelineDefinition>.

[[Previous]](configuring-pipeline.md)
[[Up]](user-manual.md)
[[Next]](pipelines-and-nodes.md)