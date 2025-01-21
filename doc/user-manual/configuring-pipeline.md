<!-- -*-visual-line-*- -->

[[Previous]](downloading-and-building-ziggy.md)
[[Up]](user-manual.md)
[[Next]](module-parameters.md)

## Configuring a Pipeline

In this article, we'll walk through the process by which you can write your own pipeline and connect it to Ziggy. As we do so, we'll show how the sample pipeline addresses each of the steps, so you can see a concrete example. For this reason, it's probably worthwhile to have the sample-pipeline folder open as we go along (though we'll make use of screen shots, so it's not absolutely essential to have that open; just recommended).

It also might be worthwhile to take open the [article on pipeline architecture](pipeline-architecture.md) in a separate window, as we'll be referring to it below.

### Write the Algorithm Software

At the heart of your pipeline are the algorithm packages that process the data and generate the results; on the architecture diagram, it's the big green "Algorithms" box on the bottom. On the one hand, we can't help you much with this -- only you know what you want your pipeline to do! On the other hand, Ziggy doesn't really put any particular requirements on how you do this. You can write what you want, the way you want it, in the language you want. At the moment, Ziggy has especially good support for C++, Java, MATLAB, and Python as algorithm languages, but really, it can be anything!

In the sample pipeline, the algorithm code is in sample-pipeline/src/main/python/major_tom/major_tom.py; with a little luck, [this link](../../sample-pipeline/src/main/python/major_tom/major_tom.py) will open the file for you! There are 4 algorithm functions, each of which does some rather dopey image processing on PNG images: one of them permutes the color maps, one performs a left-right flip, one does an up-down flip, and one averages together a collection of PNG files. They aren't written particularly well, and I can't advocate for using them as an example of how to write Python code, but the point is that they don't do anything in particular to be usable in Ziggy.

#### Pipeline Design

That said, when you write your pipeline, there are a number of design issues that you must implicitly address:

- What steps will the pipeline perform, and in what order?
- What will be the file name conventions for the inputs and outputs of each step?
- What additional information will each step need: instrument models, parameters, etc.

The reason I bring this up is that these are the things that you'll need to teach to Ziggy so it knows how to run your pipeline for you. We'll get into that in the next few sections.

### Write the Pipeline Configuration Files

The issues described above are collectively the "pipeline configuration." This is represented on the architecture diagram by the green box in the upper left, "Pipeline Configuration (XML)." As advertised, Ziggy uses a set of XML files to define the pipeline steps, data types, etc. In the interest of this article not being longer than *Dune*, we're going to cover each of them in its own article:

[Module Parameters](module-parameters.md)

[The Datastore](datastore.md)

[Pipeline Definition](pipeline-definition.md)

### Write the "Glue" Code between Algorithms and Ziggy

When writing the algorithm software, we asserted that there were no particular requirements on how the algorithms were written, which is true. However, it's also true that inevitably there has to be a certain amount of coding to the expectations of Ziggy, and this is what we mean when we talk about the "glue" code. "Glue" code is the code that is called directly by Ziggy and which then calls the algorithm code. For physicists and electrical engineers, you can think of this as providing an impedance match between Ziggy and the algorithms.

Ziggy has really 3 requirements for the code it calls:

1. The code has to be callable as a single executable with no arguments (i.e., it has to be something that could run at the command line).
2. The code should be effectively unable to error out. What this means is that any non-trivial content should be in a try / catch or try / except block.
3. The code has to return a value of 0 for successful execution, any other integer value for failure.

There's also a "desirement:" in the event that the algorithm code fails, Ziggy would like a stack trace to be provided in a particular format.

The good news here is that Ziggy will provide tools that make it easy to accomplish the items above. Also, Ziggy's "contract" with the algorithm code is as follows:

1. The data files and instrument model files needed as input will be provided in the working directory that the algorithm uses (so you don't need to worry about search paths for these inputs).
2. Results files can be written to the working directory (so you don't need to worry about sending them someplace special).
3. Ziggy will provide a file in the working directory that specifies the names of all data files, the names of all model files, and the contents of all parameter sets needed by the algorithm. Thus it is not necessary for the "glue" code to hunt around in the working directory looking for files with particular name conventions; just open the file that defines the inputs and read its contents.

#### "Glue" Code in the Sample Pipeline

In the case of the sample pipeline, the "glue" code is actually written in 2 pieces: each algorithm module has 1 Python script plus 1 shell script that calls Python and gives it the name of the Python script to execute. We'll examine each of these in turn.

##### Python-Side "Glue" Code

For the purposes of this discussion we'll use the code that calls the `permute_colors` Python function: in the Python source directory (src/main/python/sample_python/major_tom), it's [permuter.py](../../sample-pipeline/src/main/python/major_tom/permuter.py). This is the Python code that performs some housekeeping and then calls the permuter function in major_tom.py.

The `permuter.py` module starts off with the usual collection of import statements:

```Python
from ziggytools.stacktrace import ZiggyErrorWriter
from ziggytools.hdf5 import Hdf5ModuleInterface
from ziggytools.pidfile import write_pid_file
from major_tom import permute_color
```

These are the Ziggy-specific imports (the other imports are standard Python packages and modules). The first, `ZiggyErrorWriter`, provides the class that writes the stack trace in the event of a failure; `Hdf5ModuleInterface` provides a specialized HDF5 module that reads the file with Ziggy's input information and writes the stack trace; `write_pid_file` writes to the working directory a hidden file that contains the process ID for the Python process that runs the algorithm, which is potentially useful later as a diagnostic; `permute_color` is the name of the algorithm function that `permuter.py` will run.

The next block of code looks like this:

```python
# Define the HDF5 read/write class as a global variable.
hdf5_module_interface = Hdf5ModuleInterface()

if __name__ == '__main__':
    try:

        # Generate the PID file.
        write_pid_file()

        # Read inputs: note that the inputs contain the names of all the files
        # that are to be used in this process, as well as model names and
        # parameters. All files are in the working directory.
        inputs = hdf5_module_interface.read_file("permuter-inputs.h5")
        data_file = inputs['dataFilenames']
        parameters = inputs['moduleParameters']['Algorithm_Parameters']
        models = inputs['modelFilenames']
```

The main thing of interest here is that the HDF5 file with the inputs information is opened and read into a dictionary. The file name will always be the module name followed by `-inputs-0.h5`. The dictionary that's read from that file provides the data file names as a Python list in the `filenames` entry; all the module parameters in the `moduleParameters` entry; and the names of the models as a Python list in the `modelFilenames` entry. Note that, as described in [the article on parameter sets](module-parameters.md), the module parameter set named `Algorithm Parameters` in the parameters XML file is renamed to `Algorithm_Parameters` here.

Next:

```Python
        # Handle the parameter values that can cause an error or cause
        # execution to complete without generating output.
        dir_name = os.path.basename(os.getcwd())
        if dir_name == "st-0":
            throw_exception = parameters['throw_exception_subtask_0']
        else:
            throw_exception = False

        if dir_name == "st-1":
            produce_output = parameters['produce_output_subtask_1']
        else:
            produce_output = True
```

The main thing that's interesting here is that it shows how to access the individual parameters within a parameter set. You may well ask: What is this code actually doing with the parameters? Well, it's setting up to allow a demonstration of some of Ziggy's features later when we run the pipeline. For now, just focus on the fact that the parameters are entries in the parameter dictionary, and that whitespace in the parameter names has been turned to underscores.

```python
        # Run the color permuter. The permute_color function will produce
        # the output with the correct filename.
        permute_color(data_file, throw_exception, produce_output)
```

Here the actual algorithm code is called. Thus we see that the Python-side "glue" has taken the information from Ziggy and reorganized it for use by the algorithm code.

In this case the algorithm code doesn't return anything because it writes its outputs directly to the working directory. This is a choice, but not the only one: it would also be allowed for the algorithm code to return results, and for the "glue" code to perform some additional operations on them and write them to the working directory.

Anyway, moving on to the last chunk of the Python-side "glue" code, we see this:

```python
        # Sleep for a user-specified interval. This is here just so the
        # user can watch execution run on the pipeline console.
        time.sleep(parameters['execution_pause_seconds'])
        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)
```

The first part of this block does something you definitely won't want to do in real life: it uses a module parameter to insert a pause in execution! We do this here for demonstration purposes only: the algorithms in the sample pipeline are so simple that they run instantaneously, but in real life that won't happen; so this slows down the execution so you can watch what's happening and get more of a feel for what real life will be like.

Once execution completes successfully, the "glue" returns a value of 0. If an exception occurs at any point in all the foregoing, we jump to the `except` block: the stack trace is written and the "glue" returns 1.

The foregoing is all the Python "glue" code needed by an algorithm. The equivalent code in MATLAB or Java or C++ are all about equally simple.

###### Digression: Do We Really Need In-Language "Glue" Code?

Not really. In principle, all of the "glue" code, above, could have been included in the algorithm function itself. So why do it this way?

It's really a combination of convenience and division of labor. By separating the stuff that Ziggy needs into one file and the algorithm itself into another, the subject matter experts are free to develop the algorithm as they see fit and without need to worry about how it will fit into the pipeline. This also makes it easier for the algorithm code to run in a standalone or interactive way. This is especially helpful because the algorithm code is usually written, or at least prototyped, before the pipeline integration is performed. For example: in this case, all of the Python algorithm code in the sample pipeline was developed and debugged by running Python interactively; thus I provided an interface to each function that was optimal for interactive use (in this case, just send the name of the file you want to process as an argument). Once I was happy with the algorithm code, I wrote the Python "glue."

This also means that the algorithm packages can still be run interactively, which is generally useful. If the algorithm functions also had all of the Ziggy argle-bargle in them, it wouldn't be possible to run the algorithm outside of the context of either Ziggy itself or else an environment that emulates Ziggy.

Like I say, this isn't the only way to write a pipeline; but over time we've found that something like this has been the best way to do business.

##### Shell Script "Glue" Code

The shell script that provides the connection between permuter.py and Ziggy is in src/main/sh: [permuter.sh](../../sample-pipeline/src/main/sh/permuter.sh). The script can be seen as having 2 blocks of code. Here's the first:

```bash
# Check for a SAMPLE_PIPELINE_PYTHON_ENV.
if [ -n "$SAMPLE_PIPELINE_PYTHON_ENV" ]; then
    if [ -z "$ZIGGY_HOME" ]; then
        echo "SAMPLE_PIPELINE_PYTHON_ENV set but ZIGGY_HOME not set!"
        exit 1
    fi
else
    etc_dir="$(dirname "$PIPELINE_CONFIG_PATH")"
    sample_home="$(dirname "$etc_dir")"
    ZIGGY_HOME="$(dirname "$sample_home")"
    SAMPLE_PIPELINE_PYTHON_ENV=$sample_home/build/env
fi
```

All this really does is define 2 variables: `ZIGGY_HOME`, the location of the Ziggy main directory; and `SAMPLE_PIPELINE_PYTHON_ENV`, the location of the Python environment for use in running the pipeline.

The next block does all the actual work:

```bash
# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $SAMPLE_PIPELINE_PYTHON_ENV/bin/activate

# Get the location of the environment's site packages directory.
SITE_PKGS=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

# Use the environment's Python to run the permuter Python script.
python3 $SITE_PKGS/major_tom/permuter.py

# Capture the Python exit code and pass it to the caller as the script's exit code.
exit $?
```

Again, really simple: activate the environment; find the location of the environment's site-packages directory; run `permuter.py` in the Python copy in the environment; return the exit code from `permuter.py` as the exit code from the shell script. The `trap` statement at the top is the way that shell scripts implement a kind of try-catch mentality: it says that when the script exits, no matter the reason or the condition of the exit, deactivate the Python environment on the way out the door.

All of the above has focused on the permuter algorithm, but the same pattern of "glue" files and design patterns are used for the flip and averaging algorithms.

### Set up the Properties File

As you can probably imagine, Ziggy actually uses a lot of configuration items: it needs to know numerous paths around your file system, which relational database application you want to use, how much heap space to provide to Ziggy, and on and on. All of this stuff is put into two locations for Ziggy: the pipeline properties file and the Ziggy properties file.

For the sample pipeline, the pipeline properties file is [etc/sample.properties](../../sample-pipeline/etc/sample.properties). It uses a fairly standard name-value pair formalism, with capabilities for using property values or environment variables as elements of other properties.

In real life, you would want the working properties file to be outside of the directories managed by the version control system. This allows you to modify the file without fear that you will accidentally push your changes back to the repository's origin! For our purposes, we've put together a pipeline properties file that you can use without modification, so feel free to just leave it where it is. We suggest that you start by copying the [pipeline.properties.EXAMPLE file](../../etc.pipeline.properties.EXAMPLE) to someplace outside of the Git-controlled directories, rename it, and modify it so that it suits your need.

Meanwhile, The Ziggy properties file is [etc/ziggy.properties](../../etc/ziggy.properties), which is in the etc subdirectory of the main Ziggy directory. The properties here are things that you are unlikely to ever need to change, but which Ziggy needs.

The properties file is a sufficiently important topic that it has its own separate article. See the article on [The Properties File](properties.md) for discussion of all the various properties in the pipeline properties file.

### Set up the Environment Variables

In a normal pipeline, you will need to set up only one environment variable: the variable `PIPELINE_CONFIG_PATH`, which has as its value the absolute path to the pipeline properties file. Ziggy can then use the pipeline properties file to get all its configuration parameters.

For the sample pipeline, it was necessary to add a second environment variable: `ZIGGY_ROOT`, which is set to the absolute path to the top-level Ziggy directory. Why was this necessary?

Under normal circumstances, the user would set the values of the path properties in the properties file based on their own arrangement of the file system, the location of the Ziggy directory, etc. All these things are known to the user, so the user can put all that path information into the pipeline properties file.

In the case of the sample pipeline, we wanted to provide a properties file that would work for the end user, but we don't know anything about any end user's file system organization. We don't even know your username on your local system! So how do we make a properties file that will work for you without modification?

Answer: the sample properties file sets all its paths relative to the top-level Ziggy directory. Of course, we here at Ziggy World Headquarters don't know that, either; it's unknown for the same reason that the user's username, file system arrangement, etc., are unknown. Thus, the user has to provide that information in the form of a second environment variable. And here we are.

Anyway: set those properties now, before going any further.

#### What About the Ziggy Properties File?

The sample properties file contains a property that is the location of the Ziggy properties file. Thus there's no need to have a separate environment variable for that information. Like we said, to the extent possible we've put everything configuration related into the pipeline properties file.

### And That's It

Well, in fact we've covered quite a lot of material here! But once you've reached this point, you've covered everything that's needed to set up your own data analysis pipeline and connect it to Ziggy.

That said, if you're paying attention you've probably noticed that this article ignored some issues, or at least posed some mysteries:

- The shell script for the permuter module is `permuter.sh`, but the module name is `permuter`. Why isn't the module name `permuter.sh`?
- You activate a Python environment, but -- where did that environment come from? What's in it?

These questions will be discussed in the article on [Building a Pipeline](building-pipeline.md).

<!--
TODO Discuss how the properties ziggy.pipeline.data.importer.classname, ziggy.pipeline.uow.defaultIdentifier.classname, ziggy.remote.queuecommand.classname, ziggy.test.working.dir are used when rolling your own components.
See also customizing-ziggy.md.
-->

[[Previous]](downloading-and-building-ziggy.md)
[[Up]](user-manual.md)
[[Next]](module-parameters.md)
