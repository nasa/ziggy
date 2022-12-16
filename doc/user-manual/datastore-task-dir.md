[[Previous]](intermediate-topics.md)
[[Up]](intermediate-topics.md)
[[Next]](task-configuration.md)

## The Datastore and the Task Directory

The datastore is Ziggy's organized, permanent file storage system. The task directory is temporary file storage used by processing algorithms. Let's take a look at these now.

### The Datastore

Before we can look at the datastore, we need to find it! Fortunately, we can refer to the [properties file](properties.md). Sure enough, we see this:

```
pipeline.root.dir = ${ziggy.root}/sample-pipeline
pipeline.home.dir = ${pipeline.root.dir}/build
pipeline.results.dir = ${pipeline.home.dir}/pipeline-results
datastore.root.dir = ${pipeline.results.dir}/datastore
```

Well, you don't see all of those lines laid out as conveniently as the above, but trust me, they're all there. Anyway, what this is telling us is that Ziggy's data directories are in `build/pipeline-results/datastore`. Looking at that location we see this:

```console
datastore$ tree
├── models
│   └── dummy model
│       └── 2022-10-31.0001-sample-model.txt
├── set-1
│   ├── L0
│   │   ├── nasa_logo-file-0.png
│   │   ├── nasa_logo-file-1.png
│   │   ├── nasa_logo-file-2.png
│   │   └── nasa_logo-file-3.png
│   ├── L1
│   │   ├── nasa_logo-file-0.png
│   │   ├── nasa_logo-file-1.png
│   │   ├── nasa_logo-file-2.png
│   │   └── nasa_logo-file-3.png
│   ├── L2A
│   │   ├── nasa_logo-file-0.png
│   │   ├── nasa_logo-file-1.png
│   │   ├── nasa_logo-file-2.png
│   │   └── nasa_logo-file-3.png
│   ├── L2B
│   │   ├── nasa_logo-file-0.png
│   │   ├── nasa_logo-file-1.png
│   │   ├── nasa_logo-file-2.png
│   │   └── nasa_logo-file-3.png
│   └── L3
│       └── averaged-image.png
└── set-2
 datastore$
```

Summarizing what we see:

- a `models` directory, with a `dummy model` subdirectory and within that a sample model.
- A `set-1` directory and a `set-2` directory. The `set-2` directory layout mirrors the layout of `set-1`; take a look if you don't believe me, I didn't bother to expand set-2 in the interest of not taking up too much space.
- Within `set-1` we see a directory `L0` with some PNG files in it, a directory `L1` with some PNG files, and then `L2A`, `L2B`, and `L3` directories which (again, trust me or look for yourself) contain additional PNG files.

Where did all this come from? Let's take a look again at part of the `pt-sample.xml` file:

```xml
<dataFileType name="raw data"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9]).png"
              fileNameWithSubstitutionsForDatastore="$2/L0/$1-$3.png"/>

<dataFileType name="permuted colors"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9])-perm.png"
              fileNameWithSubstitutionsForDatastore="$2/L1/$1-$3.png"/>
```

If you don't remember how data file type definitions worked, feel free to [go to the article on Data File Types](data-file-types.md) for a quick refresher course. In any event, you can probably now see what we meant when we said that the data file type definitions implicitly define the structure of the datastore. The `set-1/L0` and `set-2/L0` directories come from the `fileNameWithSubstitutionsForRegex` value for raw data; similarly the permuted colors data type defines the `set-1/L1` and `set-2/L2` directories.

#### Model Names in the Datastore

If you look at the figure above, you've probably noticed that the name of the dummy model has been mangled in some peculiar fashion: instead of `sample-model.txt`, the name is `2002-09-19.0001-sample-model.txt`. What's up with that?

Well -- models are different from mission data in that it's sometimes necessary to update models; but it's also necessary to keep every copy of every model, because we need to be able to work out the full provenance of all Ziggy's data products, which includes knowing which models were used for every processing activity. But the name "regex" for the dummy model is just `sample-model.txt`. That means that every version of the model has to have the same name, which means that ordinarily a new model file would overwrite an old one. And that's not acceptable.

So: when a model is imported, its "datastore name" includes the import date and a version number (starting at 1) that get prepended to the file name. Thus the name of the file we see in the datastore. Version numbers increase monotonically and are never re-used (thus are unique), but multiple models of a given type can have the same timestamp.

If you think you're going to have more than 9,999 versions of a model, let us know and we'll change the format to accommodate you.

Note that when the model gets copied to a task directory, any Ziggy-supplied version number or timestamp will be removed and the filename seen by the algorithm will match the original name of the model file.

#### Supplying Your Own Version Information for Models

Of course, it's also possible (indeed, likely) that any actual flight mission will include version and datestamp information in the name of its model files. You can imagine a model name regex that's something like "`calibration-constants-([0-9]{8}T[0-9]{6})-v([0-9]+).xml`", where the first group in the regex is a datestamp in ISO 8601 format (YYYYMMDDThhmmss), and the second group is a version number. In this case, you might want Ziggy to use the datestamp and version number from the filename and not append its own (if for no other reason than having 2 datestamps and 2 version numbers in the datastore filename would look dumb). You can make this happen by specifying in the XML the group number for the timestamp and the group number for the version:

```xml
<modelType type="calibration-constants"
           fileNameRegex="calibration-constants-([0-9]{8}T[0-9]{6})-v([0-9]+).xml"
           versionNumberGroup="2" timestampGroup="1"/>
```

You can use this mechanism to specify that filenames have version numbers in them; or timestamps, or both. If the filename has only one of the two, Ziggy will prepend its version of the other to the filename when storing in the datastore.

### The Task Directory

When Ziggy runs algorithm code, it doesn't the algorithm direct access to files in the datastore. Instead, each pipeline task gets its own directory, known as the "task directory" (clever!).

To find the task directory, look first to the pipeline results location in the properties file:

```
pipeline.results.dir=${pipeline.home.dir}/pipeline-results
```

The pipeline-results directory contains a number of subdirectories. First, let's look at task-data:

```
task-data$ ls
1-2-permuter    1-4-flip    1-6-averaging    2-10-flip    2-8-permuter
1-3-permuter    1-5-flip    1-7-averaging    2-11-flip    2-9-permuter
task-data$
```

Every pipeline task has its own directory. The name of a task's directory is the instance number, the task number, and the module name, separated by hyphens. If we drill down into `1-2-permuter`, we see this:

```console
1-2-permuter$ ls -R
ARRIVE_PFE.1667003280019        QUEUED_PBS.1667003279236    st-1
PBS_JOB_FINISH.1667003320029    permuter-inputs.h5          st-2
PBS_JOB_START.1667003280021     st-0                        st-3

1-2-permuter/st-0:
SUB_TASK_FINISH.1667003287519    nasa_logo-set-2-file-0-perm.png    permuter-inputs-0.h5     sample-model.txt
SUB_TASK_START.1667003280036     nasa_logo-set-2-file-0.png         permuter-stdout-0.log

1-2-permuter/st-1:
SUB_TASK_FINISH.1667003294982    nasa_logo-set-2-file-1-perm.png    permuter-inputs-0.h5     sample-model.txt
SUB_TASK_START.1667003287523     nasa_logo-set-2-file-1.png         permuter-stdout-0.log

1-2-permuter/st-2:
SUB_TASK_FINISH.1667003302619    nasa_logo-set-2-file-2-perm.png    permuter-inputs-0.h5     sample-model.txt
SUB_TASK_START.1667003294987     nasa_logo-set-2-file-2.png         permuter-stdout-0.log

1-2-permuter/st-3:
SUB_TASK_FINISH.1667003310303    nasa_logo-set-2-file-3-perm.png    permuter-inputs-0.h5     sample-model.txt
SUB_TASK_START.1667003302623     nasa_logo-set-2-file-3.png         permuter-stdout-0.log
1-2-permuter$
```

At the top level there's some stuff we're not going to talk about now. What's interesting is the contents of the subtask directory, st-0:

- The sample model is present with its original (non-datastore) name, `sample-model.txt`.
- The inputs file for this subtask is present, also with its original (non-datastore) name, `nasa-logo-set-2-file-0.png`.
- The outputs file for this subtask is present: `nasa-logo-set-2-file-0-perm.png`.
- The HDF5 file that contains filenames is present: `permuter-inputs-0.h5`.
- There's a file that contains all of the standard output (i.e., printing) from the algorithm: `permuter-stdout-0.log`.
- There are a couple of files that show the Linux time that the subtask started and completed processing.

### The Moral of this Story

So what's the takeaway from all this? Well, there's actually a couple:

- Ziggy maintains separate directories for its permanent storage in the datastore and temporary storage for algorithm use in the task directory.
- The task directory, in turn, contains one directory for each subtask.
- The subtask directory contains all of the content that the subtask needs to run. This is convenient if troubleshooting is needed: you can copy a subtask directory to a different computer to be worked on, rather than being forced to work on it on the production file system used by Ziggy.
- There's some name mangling between the datastore and the task directory.
- You can put anything you want into the subtask or task directory; Ziggy only pulls back the results it's been told to pull back. This means that, if you want to dump a lot of diagnostic information into each subtask directory, which you only use if something goes wrong in that subtask, feel free; Ziggy won't mind.

### Postscript: Copies vs. Symbolic Links

If you look closely at the figure that shows the task directory, you'll notice something curious: the input and output "files" aren't really files. They're symbolic links. Specifically, they're symbolic links to files in the datastore. Looking at an example:

```console
st-0$ ls -l
total 64
-rw-r--r--  1       0 Oct 31 16:01 SUB_TASK_FINISH.1667257285445
-rw-r--r--  1       0 Oct 31 16:01 SUB_TASK_START.1667257269376
lrwxr-xr-x  1     104 Oct 31 16:01 nasa_logo-set-2-file-0-perm.png -> ziggy/sample-pipeline/build/pipeline-results/datastore/set-2/L1/nasa_logo-file-0.png
lrwxr-xr-x  1     104 Oct 31 16:01 nasa_logo-set-2-file-0.png -> ziggy/sample-pipeline/build/pipeline-results/datastore/set-2/L0/nasa_logo-file-0.png
-rw-r--r--  1   25556 Oct 31 16:01 permuter-inputs-0.h5
-rw-r--r--  1     174 Oct 31 16:01 permuter-stdout-0.log
lrwxr-xr-x  1     126 Oct 31 16:01 sample-model.txt -> ziggy/sample-pipeline/build/pipeline-results/datastore/models/dummy model/2022-10-31.0001-sample-model.txt
st-0$
```

Ziggy allows the user to select whether to use actual copies of the files or symbolic links. This is configured in -- yeah, you got it -- the properties file:

```
moduleExe.useSymlinks = true
```

The way this works is obvious for the input files: Ziggy puts a symlink in the working directory, and that's all there is to it. For the outputs file, what happens is that the algorithm produces an actual file of results; when Ziggy goes to store the outputs file, it moves it to the datastore and replaces it in the working directory with a symlink. This is a lot of words to say that you can turn this feature on or off at will and your code doesn't need to do anything different either way.

The advantages of the symlinks are fairly obvious:

- Symbolic links take up approximately zero space on the file system. If you use symbolic links you avoid having multiple copies of every file around (one in the datastore, one in the subtask directory). For large data volumes, this can be valuable.
- Similarly, symbolic links take approximately zero time to instantiate. Copies take actual finite time. Again, for large data volumes, it can be a lot better to use symlinks than copies in terms of how much time your processing needs.

There are also situations in which the symlinks may not be a good idea:

- It may be the case that you're using one computer to run the worker and database, and a different one to run the algorithms. In this situation, the datastore can be on a file system that's mounted on the worker machine but not the compute machine, in which case the symlink solution won't work (the compute node can't see the datastore, so it can't follow the link).

[[Previous]](intermediate-topics.md)
[[Up]](intermediate-topics.md)
[[Next]](task-configuration.md)
