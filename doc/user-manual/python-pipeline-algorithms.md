<!-- -*-visual-line-*- -->

[[Previous]](pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](other-pipeline-algorithms.md)

## Python Pipeline Algorithms

Let's look again at the step definitions from [the sample pipeline XML file](../../etc/ziggy.d/sample-pipeline.xml):

```xml
<step name="permuter" file="major_tom/major_tom.py" description="Color Permuter"/>
<step name="flip" file="major_tom/major_tom.py" description="Flip Up-Down and Left-Right"/>
<step name="averaging" file="major_tom/major_tom.py" description="Average Images Together"/>
```

These lines of XML define the pipeline steps for the sample pipeline. As you can see, the `file` attribute contains the name of a Python module: module `major_tom.py` in package `major_tom`. Each pipeline step also has a unique name with no whitespace.

Ziggy determines that a step is written in Python by the fact that the value in the `executableName` attribute ends with `.py`. In this case, all 3 steps are in Python. Notice that, while all 3 steps have unique names with no whitespace, all of them have the same `file`. Point being that the Python module doesn't have to be unique.

### Packages, Modules, and Functions

Consider the permuter step. The `file` is `major_tom/major_tom.py` and the step name is `permuter`. 

At runtime, Ziggy will look for module `major_tom` in package `major_tom`, and will look in that Python module for a function named `permuter`. The `permuter` function contains all the business logic of the permuter algorithm, hence it will be executed by Ziggy whenever Ziggy needs to run the permuter step.

#### Rules for the Python Function

The function that's run when a given pipeline step is called has a couple of design rules that you must follow:

- The function must take a single argument. 
- The function must raise an exception if it fails. 

Let's discuss each of these.

##### The Function Argument

When your function runs, Ziggy will pass it a Python dictionary as its sole argument. The dictionary contains all of the contents of the inputs file for the given subtask, read in from HDF5 and converted to a dictionary (see [the article on the datastore and the task directory](datastore-task-dir.md) for more information about input files). Typically this will be the list of data files that need to be processed, any parameter sets that the pipeline step needs, and the names of any instrument model files for the pipeline step.

Note that you are not required to use the dictionary that Ziggy passes to your function! In general, we've found it fairly convenient to have a single file that holds all the information needed by a pipeline step, but you're free to ignore it in your code, as long as you accept it as an argument to your function. TL;DR: Ziggy has to give it to you, but you don't have to use it.

##### Execution Flow of the Function

Your Python function can do anything: call other Python modules, read files from the working directory, write files to the working directory, etc. The only non-negotiable behavior of the Python function is that, if the algorithm fails, it needs to exit by raising an uncaught exception. 

The reason we're a stickler for this is that Ziggy needs some way to detect that algorithm processing has failed in one or more cases. This allows Ziggy to determine whether it can continue processing or whether it needs to halt and seek assistance from the user. For Python algorithms, the uncaught exception is that way. 

##### What About the Return Code?

You may remember from [the article on configuring a pipeline](configuring-pipeline.md) that we stated that an algorithm's executable has to return 0 for success, 1 for failure. 

In this case, that's not necessary. It's not necessary because in the event of failure, the code has to raise an exception, hence any return from the algorithm will be ignored anyway. Ziggy provides additional logic that determines whether the algorithm failed, processes the resulting stack trace, etc. 

You're welcome.

### Virtual Environments

Most Python applications rely on the use of virtual environments as a means of encapsulating everything that the application needs in one easy-to-find location. 

Ziggy supports the use of virtual environments. All virtual environments for use by the pipeline need to be in the `env` subdirectory of the pipeline's home directory; the pipeline's home directory is the directory that's given by the `ziggy.pipeline.home.dir` property in the properties file (see [the article on properties files](properties.md) for more information).

Before a pipeline step is executed, Ziggy looks for a virtual environment. The search order is as follows:

1. If the `env` directory is itself a virtual environment, it is activated. Otherwise:
2. If the `env` directory contains a subdirectory with the name of the pipeline step, that environment is activated. Otherwise:
3. If the `env` directory contains a subdirectory with the name of the Python module, that environment is activated. Otherwise:
4. If the `env` directory contains a subdirectory named `pipeline`, that environment is used. Otherwise:
5. Ziggy concludes that there is no virtual environment available.

To make this concrete: when the permuter pipeline step runs, Ziggy will look first at `env`, then look for `env/permuter`, then `env/major_tom`, then `env/pipeline`.

The virtual environment can be one created by `venv` or by `conda`. As with everything else, you're free to mix-and match: you can have `venv` environments and `conda` environments in the same pipeline.

### Where do my Python Modules go?

At runtime, Ziggy needs to be able to find any Python module you've told it to use! How does Ziggy do this? 

The simplest way to manage this, if you're using a virtual environment, is to put your Python packages into the virtual environment. This is what we do with our sample pipeline: the `major_tom` package is installed into the virtual environment.

If you don't want to do that, or can't do it, you specify the search path for Python packages and modules in the usual way: with a `PYTHONPATH` environment variable. However: Ziggy doesn't use the `PYTHONPATH` environment variable that's set up in your personal environment on your computer! This is because you may want your computer's environment to be different from Ziggy's runtime environment. Ziggy gets its environment variables from -- yeah, you got it, the properties file. In particular, the property `ziggy.pipeline.environment` is a comma-separated collection of environment variable definitions. As a simple example, let's imagine that, actually, you do want your personal `PYTHONPATH` to also be the `PYTHONPATH` Ziggy uses at runtime. Your `ziggy.pipeline.environment` entry in the properties file would look like this:

```
ziggy.pipeline.environment = PYTHONPATH=${env:PYTHONPATH}, OTHER_ENV=foobar
```

Note that I've told Ziggy that in addition to setting `PYTHYONPATH` in its runtime environment, I want `OTHER_ENV` to be set to foobar.

[[Previous]](pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](other-pipeline-algorithms.md)