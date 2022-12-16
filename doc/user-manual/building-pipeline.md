<!-- -*-visual-line-*- -->

[[Previous]](pipeline-definition.md)
[[Up]](user-manual.md)
[[Next]](rdbms.md)

## Building Your Pipeline

The question of whether your pipeline even needs a build system is one that only you can answer. If all the components of the pipeline are written in interpreted languages (Python, shell script, etc.), you might be able to get away without one!

In this article, we review the "build system" for the sample pipeline, and in the process offer arguments why some kind of build system is a good idea in general.

Before we move on, you should make sure that your environment variables `PIPELINE_CONFIG_PATH` and `ZIGGY_ROOT` are set correctly (as a reminder of what that means, take a look at the "Set up the Environment Variables" section of [the article on configuring a pipeline](configuring-pipeline.md)).

### The Sample Pipeline Directory

Just in case you haven't looked yet, here's what the sample pipeline directory should look like:

```console
sample-pipeline$ ls
build-env.sh    config    data    etc    multi-data    src
sample-pipeline$ 
```

As we've discussed, the `src` directory is the various bits of source code for the pipeline, `etc` is the location of the pipeline properties file, `config` is the location of the XML files that define the pipeline. The `data` is the initial source of the data files that will be used by the sample pipeline. 

At the top you can see `build-env.sh`, which is the "build system" for the sample pipeline. In this case, the sample pipeline is so simple that none of the grown-up build systems were seen as needed or even desirable; a shell script would do what was needed. 

If you run the shell script from the command line (`./build-env.sh`), you should quickly see something that looks like this:

```console
sample-pipeline$ /bin/bash ./build-env.sh 
Collecting h5py
  Using cached h5py-3.7.0-cp38-cp38-macosx_10_9_x86_64.whl (3.2 MB)
Collecting Pillow
  Using cached Pillow-9.2.0-cp38-cp38-macosx_10_10_x86_64.whl (3.1 MB)
Collecting numpy
  Using cached numpy-1.23.4-cp38-cp38-macosx_10_9_x86_64.whl (18.1 MB)
Installing collected packages: Pillow, numpy, h5py
Successfully installed Pillow-9.2.0 h5py-3.7.0 numpy-1.23.4
sample-pipeline$ 
```



Meanwhile, the directory now looks like this:

```console
sample-pipeline$ ls
build    build-env.sh    config    data    etc    multi-data    src
sample-pipeline$ ls build
bin    env    pipeline-results
sample-pipeline$ 
```

There's now a `build` directory that contains additional directories: `bin, data-receipt`, and `env`. 

#### The build-env.sh Shell Script

Let's go through the build-env.sh script in pieces. The first piece is the familiar code chunk that sets up some shell variables with paths:

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

Next:

```bash
# put the build directory next to the env directory in the directory tree
BUILD_DIR="$(dirname "$SAMPLE_PIPELINE_PYTHON_ENV")"
mkdir -p $SAMPLE_PIPELINE_PYTHON_ENV

# Create and populate the data receipt directory from the sample data
DATA_RECEIPT_DIR=$BUILD_DIR/data-receipt
mkdir -p $DATA_RECEIPT_DIR
cp $sample_home/data/* $DATA_RECEIPT_DIR
```

Here we create the `build` directory and its `env` and `data-receipt` directories. The contents of the data directory from the sample directory gets copied to `data-receipt`. 

```bash
# build the bin directory in build
BIN_DIR=$BUILD_DIR/bin
mkdir -p $BIN_DIR
BIN_SRC_DIR=$sample_home/src/main/sh

# Copy the shell scripts from src to build. There's probably some good shell script
# way to do this, but I'm too lazy.
cp $BIN_SRC_DIR/permuter.sh $BIN_DIR/permuter
cp $BIN_SRC_DIR/flip.sh $BIN_DIR/flip
cp $BIN_SRC_DIR/averaging.sh $BIN_DIR/averaging
chmod -R a+x $BIN_DIR
```

Here we construct `build/bin` and copy the shell scripts from `src/main/sh` to `build/bin`. In the process, we strip off the `.sh` suffixes. The shell script copies in `build/bin` now match what Ziggy expects to see. 

```bash
python3 -m venv $SAMPLE_PIPELINE_PYTHON_ENV

# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $SAMPLE_PIPELINE_PYTHON_ENV/bin/activate

# Build the environment with the needed packages.
pip3 install h5py Pillow numpy

# Get the location of the environment's site packages directory
SITE_PKGS=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

# Copy the pipeline major_tom package to the site-packages location.
cp -r $ZIGGY_HOME/sample-pipeline/src/main/python/major_tom $SITE_PKGS

# Copy the Ziggy components to the site-packages location.
cp -r $ZIGGY_HOME/src/main/python/hdf5mi $SITE_PKGS
cp -r $ZIGGY_HOME/src/main/python/zigutils $SITE_PKGS

exit 0
```

Here at last we build the Python environment that will be used for the sample pipeline. The environment is built in `build/env`, and the packages `h5py`, `Pillow`, and `numpy` are installed in the environment. Finally, the sample pipeline source code and the Ziggy utility modules are copied to the `site-packages` directory of the environment, so that they will be on Python's search path when the environment is activated. 

### Okay, but Why?

Setting up the `build` directory this way has a number of advantages. First and foremost, everything that Ziggy will eventually need to find is someplace in `build`. If your experience with computers is anything like mine, you know that 90% of what we spend our time doing is figuring out why various search paths aren't set correctly. Putting everything you need in one directory minimizes this issue. 

The second advantage is related: if you decide that you need to clean up and start over, you can simply delete the `build` directory. You don't need to go all over the place looking for directories and deleting them. Indeed, most grown up build systems will include a command that will automatically perform this deletion. 

### Why a Build System?

If you look at build-env.sh, you'll see that it's relatively simple: just 80 lines, including comments and whitespace lines. Nonetheless, you wouldn't want to have to type all this in every time you want to generate the `build` directory! Having a build system -- any build system -- allows you to ensure that the build is performed reproducibly -- every build is the same as all the ones before. It allows you to implement changes to the build in a systematic way. 

### Which Build System?

Good question! There are a lot of them out there. 

For simple systems, good old `make` is a solid choice. Support for `make` is nearly universal, and most computers ship with some version of it already loaded so you won't even need to install it yourself. 

For more complex systems, we're enamored of Gradle. The secret truth of build systems is that a build is actually a program: it describes the steps that need to be taken, their order, and provides conditionals of various kinds (like, "if the target is up-to-date, skip this step"). Gradle embraces this secret truth and runs with it, which makes it in many ways easier to use for bigger systems than `make`, with its frequently bizarre use of symbols and punctuation to represent relationships within the software.

### Postscript

Note that while going through all this exposition, we've also sneakily built the sample pipeline! This positions us for the next (exciting) step: [running the pipeline](running-pipeline.md)!

Unfortunately, before you get there, we'll need to talk some about [relational databases](rdbms.md).

[[Previous]](pipeline-definition.md)
[[Up]](user-manual.md)
[[Next]](rdbms.md)
