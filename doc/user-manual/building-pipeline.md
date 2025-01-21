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
Processing ./sample-pipeline/build/src/main/python/sample_pipeline
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
Processing ./build/src/main/python/ziggy
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
Collecting Pillow (from sample_pipeline==0.7.0)
  Using cached pillow-11.0.0-cp312-cp312-macosx_11_0_arm64.whl.metadata (9.1 kB)
Collecting numpy (from sample_pipeline==0.7.0)
  Using cached numpy-2.1.3-cp312-cp312-macosx_14_0_arm64.whl.metadata (62 kB)
Collecting h5py (from ziggy==0.7.0)
  Using cached h5py-3.12.1-cp312-cp312-macosx_11_0_arm64.whl.metadata (2.5 kB)
Using cached h5py-3.12.1-cp312-cp312-macosx_11_0_arm64.whl (2.9 MB)
Using cached numpy-2.1.3-cp312-cp312-macosx_14_0_arm64.whl (5.1 MB)
Using cached pillow-11.0.0-cp312-cp312-macosx_11_0_arm64.whl (3.0 MB)
Building wheels for collected packages: sample_pipeline, ziggy
  Building wheel for sample_pipeline (pyproject.toml) ... done
  Created wheel for sample_pipeline: filename=sample_pipeline-0.7.0-py3-none-any.whl size=5660 sha256=5910f25cdaab267d2808b7a5e613fb9d8a3ecc38ad2e1901d7e6468ba77662cd
  Stored in directory: /private/var/folders/q5/0svn77vd25z40y9clkvktgq00000gp/T/pip-ephem-wheel-cache-3q146zcn/wheels/25/0c/64/f43bcc8b7f017d18d5c8be8fe8bfa73c57b13b9fe4800b778c
  Building wheel for ziggy (pyproject.toml) ... done
  Created wheel for ziggy: filename=ziggy-0.7.0-py3-none-any.whl size=16109 sha256=cfeced3656770e5aad897eedd5c0efb18a36e3add24c4e87415149b15314f887
  Stored in directory: /private/var/folders/q5/0svn77vd25z40y9clkvktgq00000gp/T/pip-ephem-wheel-cache-3q146zcn/wheels/f8/b5/e5/0f689b8d4a4f432a4927a6c94193925bc8ac3ac449df12eb08
Successfully built sample_pipeline ziggy
Installing collected packages: Pillow, numpy, sample_pipeline, h5py, ziggy
Successfully installed Pillow-11.0.0 h5py-3.12.1 numpy-2.1.3 sample_pipeline-0.7.0 ziggy-0.7.0
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
pip3 install $sample_home/src/main/python/sample_pipeline $ZIGGY_HOME/src/main/python/ziggy

# Generate version information.
$ZIGGY_HOME/bin/ziggy generate-build-info --home $sample_home

exit 0
```

Here at last we build the Python environment that will be used for the sample pipeline. The environment is built in `build/env`, and the packages from the `sample_pipeline` Python project are installed in the environment, along with their dependencies; the packages from the `ziggy` Python project are also installed. 

Finally, the version information for the sample pipeline is generated. This creates a metadata file in the `build/etc` directory of the sample pipeline. The file is named `pipeline-build.properties`, and the contents are as follows:

```
# This file is automatically generated by Ziggy.
# Do not edit.
pipeline.version = Ziggy-0.7.0-20241025-76-g513da3f544
pipeline.version.branch = release/ZIGGY-397-ziggy-0.8.0
pipeline.version.commit = 513da3f544
```

(Note that the contents of your file may vary.)

This file is generated from the Git branch and commit that are in use when the `build-env.sh` script runs. When the pipeline performs a processing activity, the version information is stored in the database so that there is a permanent record of which code version was used for each pipeline task. 

### Okay, but Why?

Setting up the `build` directory this way has a number of advantages. First and foremost, everything that Ziggy will eventually need to find is someplace in `build`. If your experience with computers is anything like mine, you know that 90% of what we spend our time doing is figuring out why various search paths aren't set correctly. Putting everything you need in one directory minimizes this issue.

The second advantage is related: if you decide that you need to clean up and start over, you can simply delete the `build` directory. You don't need to go all over the place looking for directories and deleting them. Indeed, most grown up build systems will include a command that will automatically perform this deletion.

### Why a Build System?

If you look at build-env.sh, you'll see that it's relatively simple: just 53 lines, including comments and whitespace lines. Nonetheless, you wouldn't want to have to type all this in every time you want to generate the `build` directory! Having a build system -- any build system -- allows you to ensure that the build is performed reproducibly -- every build is the same as all the ones before. It allows you to implement changes to the build in a systematic way.

### Which Build System?

Good question! There are a lot of them out there.

For simple systems, good old `make` is a solid choice. Support for `make` is nearly universal, and most computers ship with some version of it already loaded so you won't even need to install it yourself.

For more complex systems, we're enamored of Gradle. The secret truth of build systems is that a build is actually a program: it describes the steps that need to be taken, their order, and provides conditionals of various kinds (like, "if the target is up-to-date, skip this step"). Gradle embraces this secret truth and runs with it, which makes it in many ways easier to use for bigger systems than `make`, with its frequently bizarre use of symbols and punctuation to represent relationships within the software.

### Postscript

Note that while going through all this exposition, we've also sneakily built the sample pipeline! This positions us for the next (exciting) step: [running the pipeline](running-pipeline.md)!

[[Previous]](pipeline-definition.md)
[[Up]](user-manual.md)
[[Next]](running-pipeline.md)
