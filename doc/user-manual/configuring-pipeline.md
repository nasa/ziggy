<!-- -*-visual-line-*- -->

[[Previous]](downloading-and-building-ziggy.md)
[[Up]](user-manual.md)
[[Next]](pipeline-algorithms.md)

## Configuring a Pipeline

In this article, we'll walk through the process by which you can write your own pipeline and connect it to Ziggy. As we do so, we'll show how the sample pipeline addresses each of the steps, so you can see a concrete example. For this reason, it's probably worthwhile to have the sample-pipeline folder open as we go along (though we'll make use of screen shots, so it's not absolutely essential to have that open; just recommended).

It also might be worthwhile to open the [article on pipeline architecture](pipeline-architecture.md) in a separate window, as we'll be referring to it below.

### Write the Algorithm Software

At the heart of your pipeline are the algorithm packages that process the data and generate the results; on the architecture diagram, it's the big green "Algorithms" box on the bottom. On the one hand, we can't help you much with this -- only you know what you want your pipeline to do! On the other hand, Ziggy doesn't really put a lot of requirements on how you do this. You can write what you want, the way you want it, in the language you want. At the moment, Ziggy has especially good support for C++, Java, MATLAB, and Python as algorithm languages, but really, it can be anything!

In the sample pipeline, the algorithm code is in `sample-pipeline/src/main/python/sample_pipeline/major_tom/major_tom.py`; with a little luck, [this link](../../sample-pipeline/src/main/python/sample_pipeline/major_tom/major_tom.py) will open the file for you! There are 4 algorithm functions, each of which does some simple image processing on PNG images: one of them permutes the color maps, one performs a left-right flip, one does an up-down flip, and one averages together a collection of PNG files. They aren't written particularly well, and I can't advocate for using them as an example of how to write Python code, but the point is that they don't do anything in particular to be usable in Ziggy.

#### Design Rule for Algorithm Software

There's really only one design rule for your algorithm software: it must return 0 to the caller if it succeeds and any nonzero value if it fails. This is how Ziggy knows whether it, er, succeeded or failed. 

### Write the Pipeline Definition Files

That said, when you write your pipeline, there are a number of design issues that you must address:

- What steps will the pipeline perform, and in what order?
- What will be the file name conventions for the inputs and outputs of each step?
- What additional information will each step need: instrument models, parameters, etc.

The issues described above are collectively the "pipeline definition." This is represented on the architecture diagram by the green box in the upper left, "Pipeline Definition (XML)." 

Ziggy allows you to put all the bits and pieces of your pipeline defintions in one file, or in multiple files. Everything is pretty free-form: as long as the file begins with <pipelineDefinition> and ends with </pipelineDefinition>, you can but any combination of pipelines, algorithms, data types, etc., into any file. 

One thing we do recommend is that all the files be located in a single directory. This simplifies things when Ziggy needs to read in these files to set up its internal representation of your pipelines. We'll talk more about this in [the article on running the pipeline](running-pipeline.md).

In the interest of this article not being longer than *Dune*, we're going to cover the assorted parts of a pipeline definition in the following articles:

- [Pipeline Algorithms](pipeline-algorithms.md) talks about the part you're probably most invested in, which is the way that you plug your algorithms into Ziggy.
- [Algorithm Parameters](algorithm-parameters.md) talks about how you pass parameters to your algorithms, so that you can adjust their behavior at runtime.
- [The Datastore](datastore.md) talks about how you set up data file types (the files used for algorithm inputs and outputs) and how you configure the datastore (an organized directory tree where Ziggy keeps its mission data and all outputs from all pipeline runs).
- Remote Execution environments talks about how you set up to run your algorithms on a high performance computing (HPC) or cloud environment (you don't really want to process terabytes a day on your laptop, do you?).
- [Pipelines and Nodes](pipelines-and-nodes.md) shows how the actual pipelines are defined. This uses all of the above, which is why we've left it for last. 

The sample pipeline has three pipeline definition files, all in the `etc/ziggy.d` subdirectory of the `sample-pipeline` directory:

- The main pipeline definition file is [sample-pipeline.xml](../../sample-pipeline/etc/ziggy.d/sample-pipeline.xml). This covers the algorithms, parameters, datastore, pipelines, and nodes.
- The file [hecc-environment.xml](../../sample-pipeline/etc/ziggy.d/hecc-environment.xml) shows a remote environment definition for the High End Computing Capability (HECC) at NASA, which is the supercomputer run out of Ames Research Center (sometimes referred to as "the NAS" because it's part of the NASA Advanced Supercomputer (NAS) project).
- The file [sample-remote-environment.xml](../../sample-pipeline/etc/ziggy.d/sample-remote-environment.xml) shows a second remote environment. This one is totally fictitious, so you can't actually run any of its elements. It's here just so we can demonstrate how things work when you have a set of remote environments and can select from any of them to run your jobs.

### Set up the Properties File

As you can probably imagine, Ziggy actually uses a lot of configuration items: it needs to know numerous paths around your file system, which relational database application you want to use, how much heap space to provide to Ziggy, and on and on. All of this stuff is put into two locations for Ziggy: the pipeline properties file and the Ziggy properties file.

For the sample pipeline, the pipeline properties file is [etc/sample.properties](../../sample-pipeline/etc/sample.properties). It uses a fairly standard name-value pair formalism, with capabilities for using property values or environment variables as elements of other properties.

In real life, you would want the working properties file to be outside of the directories managed by the version control system. This allows you to modify the file without fear that you will accidentally push your changes back to the repository's origin! For our purposes, we've put together a pipeline properties file that you can use without modification, so feel free to just leave it where it is. We suggest that you start by copying the [pipeline.properties.EXAMPLE file](../../etc/pipeline.properties.EXAMPLE) to someplace outside of the Git-controlled directories, rename it, and modify it so that it suits your need.

Meanwhile, The Ziggy properties file is [etc/ziggy.properties](../../etc/ziggy.properties), which is in the etc subdirectory of the main Ziggy directory. The properties here are things that you are unlikely to ever need to change, but which Ziggy needs.

The properties file is a sufficiently important topic that it has its own separate article. See the article on [The Properties File](properties.md) for discussion of all the various properties in the pipeline properties file.

### Set up the Environment Variables

In a normal pipeline, you will need to set up only one environment variable: the variable `PIPELINE_CONFIG_PATH`, which has as its value the absolute path to the pipeline properties file. Ziggy can then use the pipeline properties file to get all its configuration parameters.

For the sample pipeline, it was necessary to add a second environment variable: `ZIGGY_ROOT`, which is set to the absolute path to the top-level Ziggy directory. Why was this necessary?

Under normal circumstances, the user would set the values of the path properties in the properties file based on their own arrangement of the file system, the location of the Ziggy directory, etc. All these things are known to the user, so the user can put all that path information into the pipeline properties file.

In the case of the sample pipeline, we wanted to provide a properties file that would work for the end user, but we don't know anything about any end user's file system organization.
So all the paths in the sample properties file are relative to the root of the Ziggy source tree, which is
set in that `ZIGGY_ROOT` environment variable.

You do not need to change any of those properties to run the sample pipeline, but when you define your
own pipeline or modify the sample pipeline you may need to edit these or define your own properties.

#### What About the Ziggy Properties File?

The sample properties file contains a property that is the location of the Ziggy properties file. Thus there's no need to have a separate environment variable for that information. Like we said, to the extent possible we've put everything configuration related into the pipeline properties file.

### And That's It

Well, in fact we've covered quite a lot of material here! But once you've reached this point, you've covered everything that's needed to set up your own data analysis pipeline and connect it to Ziggy.

<!--
TODO Discuss how the properties ziggy.pipeline.data.importer.classname, ziggy.pipeline.uow.defaultIdentifier.classname, ziggy.test.working.dir are used when rolling your own components.
See also customizing-ziggy.md.
-->

[[Previous]](downloading-and-building-ziggy.md)
[[Up]](user-manual.md)
[[Next]](pipeline-algorithms.md)
