<!-- -*-visual-line-*- -->

[[Previous]](pipeline-definition.md)
[[Up]](user-manual.md)
[[Next]](running-pipeline.md)

## Building a Pipeline

The question of whether your pipeline even needs a build system is one that only you can answer. If all the components of the pipeline are written in interpreted languages (Python, shell script, etc.), you might be able to get away without one!

In this article, we review the "build system" for the sample pipeline, and in the process offer arguments why some kind of build system is a good idea in general.

Before we move on, you should make sure that your environment variables `PIPELINE_CONFIG_PATH` and `ZIGGY_ROOT` are set correctly (as a reminder of what that means, take a look at the "Set up the Environment Variables" section of [the article on configuring a pipeline](configuring-pipeline.md)).

### The Sample Pipeline Directory

Just in case you haven't looked yet, here's what the sample pipeline directory should look like:

```console
sample-pipeline$ ls
build-env.sh  clean-env.sh  config  config-extra  data  etc  multi-data  src
sample-pipeline$
```

As we've discussed, the `src` directory is the various bits of source code for the pipeline, `etc` is the location of the pipeline properties file, `config` is the location of the XML files that define the pipeline. The `data` is the initial source of the data files that will be used by the sample pipeline.

At the top you can see `build-env.sh`, which is the "build system" for the sample pipeline. In this case, the sample pipeline is so simple that none of the grown-up build systems were seen as needed or even desirable; a shell script would do what was needed.

If you run the shell script from the command line (`./build-env.sh`), you should quickly see something that looks like this:

```console
sample-pipeline$ ./build-env.sh
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
build  build-env.sh  clean-env.sh  config  config-extra  data  etc  multi-data  src
sample-pipeline$ ls build
bin    env    pipeline-results
sample-pipeline$
```

There's now a `build` directory that contains additional directories: `bin`, `env`, and `pipeline-results`.

#### The build-env.sh Shell Script

Let's go through the build-env.sh script in pieces. The first piece is the familiar code chunk that sets up some shell variables with paths:

```bash
etc_dir="$(dirname "$PIPELINE_CONFIG_PATH")"
sample_root="$(dirname "$etc_dir")"
sample_home="$sample_root/build"
python_env=$sample_home/env
ziggy_root="$(dirname "$sample_root")"
```

Next:

```bash
# Put the build directory next to the env directory in the directory tree.
mkdir -p $python_env

# Create and populate the data receipt directory from the sample data
data_receipt_dir=$sample_home/pipeline-results/data-receipt
mkdir -p $data_receipt_dir
cp -r $sample_root/data/* $data_receipt_dir
```

Here we create the `build` directory and its `env` and `pipeline-results` directories. The contents of the data directory from the sample directory gets copied to the `data-receipt` subdirectory.

```bash
# Build the bin directory in build.
bin_dir=$sample_home/bin
mkdir -p $bin_dir
bin_src_dir=$sample_root/src/main/sh

# Copy the shell scripts from src to build.
install -m a+rx  $bin_src_dir/permuter.sh $bin_dir/permuter
install -m a+rx  $bin_src_dir/flip.sh $bin_dir/flip
install -m a+rx  $bin_src_dir/averaging.sh $bin_dir/averaging
```

Here we construct `build/bin` and copy the shell scripts from `src/main/sh` to `build/bin`. In the process, we strip off the `.sh` suffixes. The shell script copies in `build/bin` now match what Ziggy expects to see.

```bash
python3 -m venv $python_env

# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $python_env/bin/activate

# Build the environment with the needed packages.
pip3 install h5py Pillow numpy

# Get the location of the environment's site packages directory
site_pkgs=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

# Copy the pipeline major_tom package to the site-packages location.
cp -r $ziggy_root/sample-pipeline/src/main/python/major_tom $site_pkgs

# Copy the Ziggy components to the site-packages location.
cp -r $ziggy_root/src/main/python/hdf5mi $site_pkgs
cp -r $ziggy_root/src/main/python/zigutils $site_pkgs

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

[[Previous]](pipeline-definition.md)
[[Up]](user-manual.md)
[[Next]](running-pipeline.md)
