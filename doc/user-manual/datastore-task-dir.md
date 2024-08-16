<!-- -*-visual-line-*- -->

[[Previous]](intermediate-topics.md)
[[Up]](intermediate-topics.md)
[[Next]](rdbms.md)

## The Datastore and the Task Directory

The datastore is Ziggy's organized, permanent file storage system. The task directory is temporary file storage used by processing algorithms. Let's take a look at these now.

### The Datastore

Before we can look at the datastore, we need to find it! Fortunately, we can refer to the [properties file](properties.md). Sure enough, we see this:

```
pipeline.root.dir = ${ziggy.root}/sample-pipeline
ziggy.pipeline.home.dir = ${pipeline.root.dir}/build
ziggy.pipeline.results.dir = ${ziggy.pipeline.home.dir}/pipeline-results
ziggy.pipeline.datastore.dir = ${ziggy.pipeline.results.dir}/datastore
```

Well, you don't see all of those lines laid out as conveniently as the above, but trust me, they're all there. Anyway, what this is telling us is that Ziggy's data directories are in `build/pipeline-results/datastore`. Looking at that location we see this:

```console
datastore$ tree
├── models
│   └── dummy model
│       └── 2024-06-27.0001-sample-model.txt
├── set-1
│   ├── L0
│   │   ├── nasa-logo-file-0.png
│   │   ├── nasa-logo-file-1.png
│   │   ├── nasa-logo-file-2.png
│   │   └── nasa-logo-file-3.png
│   ├── L1
│   │   ├── nasa-logo-file-0.perm.png
│   │   ├── nasa-logo-file-1.perm.png
│   │   ├── nasa-logo-file-2.perm.png
│   │   └── nasa-logo-file-3.perm.png
│   ├── L2A
│   │   ├── nasa-logo-file-0.fliplr.png
│   │   ├── nasa-logo-file-1.fliplr.png
│   │   ├── nasa-logo-file-2.fliplr.png
│   │   └── nasa-logo-file-3.fliplr.png
│   ├── L2B
│   │   ├── nasa-logo-file-0.flipud.png
│   │   ├── nasa-logo-file-1.flipud.png
│   │   ├── nasa-logo-file-2.flipud.png
│   │   └── nasa-logo-file-3.flipud.png
│   └── L3
│       └── nasa-logo-averaged.png
└── set-2
datastore$
```

Summarizing what we see:

- a `models` directory, with a `dummy model` subdirectory and within that a sample model.
- A `set-1` directory and a `set-2` directory. The `set-2` directory layout mirrors the layout of `set-1`; take a look if you don't believe me, I didn't bother to expand set-2 in the interest of not taking up too much space.
- Within `set-1` we see a directory `L0` with some PNG files in it, a directory `L1` with some PNG files, and then `L2A`, `L2B`, and `L3` directories which contain additional PNG files.

Where did all this come from? Let's take a look again at part of the `pt-sample.xml` file:

```xml
  <!-- The raw data. this is in the L0 subdir of the dataset
       directory, with a file name regular expression of
       "(nasa-logo-file-[0-9]).png" -->
  <dataFileType name="raw data" location="dataset/L0"
                fileNameRegexp="(nasa-logo-file-[0-9])\.png"/>

  <!-- Results from the first processing step. This goes in the L1 subdir
       of the dataset directory, with a file name regular expression of
       "(nasa-logo-file-[0-9])\.perm\.png" -->
  <dataFileType name="permuted colors" location="dataset/L1"
                fileNameRegexp="(nasa-logo-file-[0-9])\.perm\.png"/>
```

If you don't remember how data file type definitions worked, feel free to [go to the article on The Datastore](datastore.md) for a quick refresher course. What you can see is that, as advertised, data file type `raw data` has a location that points to the `L0` directory in the datastore, and files with the name convention `"nasa-logo-file-[0-9]\.png"`. Similarly, the files in the `L1` directory have file names that match the `fileNameRegexp` for the `permuted colors` data file type.

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
ziggy.pipeline.results.dir=${ziggy.pipeline.home.dir}/pipeline-results
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
SUB_TASK_FINISH.1667003287519    nasa_logo-file-0.perm.png    permuter-inputs.h5
sample-model.txt
SUB_TASK_START.1667003280036     nasa_logo-file-0.png         permuter-stdout.log

1-2-permuter/st-1:
SUB_TASK_FINISH.1667003294982    nasa_logo-file-1.perm.png    permuter-inputs.h5
sample-model.txt
SUB_TASK_START.1667003287523     nasa_logo-file-1.png         permuter-stdout.log

1-2-permuter/st-2:
SUB_TASK_FINISH.1667003302619    nasa_logo-file-2.perm.png    permuter-inputs.h5
sample-model.txt
SUB_TASK_START.1667003294987     nasa_logo-file-2.png         permuter-stdout.log

1-2-permuter/st-3:
SUB_TASK_FINISH.1667003310303    nasa_logo-file-3.perm.png    permuter-inputs.h5
sample-model.txt
SUB_TASK_START.1667003302623     nasa_logo-file-3.png         permuter-stdout.log
1-2-permuter$
```

At the top level there's some stuff we're not going to talk about now. What's interesting is the contents of the subtask directory, st-0:

- The sample model is present with its original (non-datastore) name, `sample-model.txt`.
- The inputs file for this subtask is present: `nasa-logo-file-0.png`.
- The outputs file for this subtask is present: `nasa-logo-file-0.perm.png`.
- The HDF5 file that contains filenames is present: `permuter-inputs.h5`.
- There's a file that contains all of the standard output (i.e., printing) from the algorithm: `permuter-stdout.log`.
- There are a couple of files that show the Linux time that the subtask started and completed processing.

### The Moral of this Story

So what's the takeaway from all this? Well, there's actually a couple:

- Ziggy maintains separate directories for its permanent storage in the datastore and temporary storage for algorithm use in the task directory.
- The task directory, in turn, contains one directory for each subtask.
- The subtask directory contains all of the content that the subtask needs to run. This is convenient if troubleshooting is needed: you can copy a subtask directory to a different computer to be worked on, rather than being forced to work on it on the production file system used by Ziggy.
- There's some name mangling of models between the datastore and the task directory.
- You can put anything you want into the subtask or task directory; Ziggy only pulls back the results it's been told to pull back. This means that, if you want to dump a lot of diagnostic information into each subtask directory, which you only use if something goes wrong in that subtask, feel free; Ziggy won't mind.

### Postscript: Copies vs. Links

Are the files in the datastore and the task directory really copies of one another? Well, that depends.

Most modern file systems offer a facility known as a "link" or a "hard link." The way a link works is as follows: rather than copy a file from Directory A to Directory B, the file system creates a new entry in Directory B for the file, and points it at the spot in the file system that holds the file you care about in Directory A. The file has, in effect, two names: one in Directory A and one in Directory B; but that file still only takes up the space of one file on the file system (rather than two, which is what you get when you copy a file).

A great property of the link system is that, if we start with a file in Directory A, create a link to that file in Directory B, and then delete the file in Directory A, as far as Directory B is concerned that file is still there and can be accessed, modified, etc. In other words, as long as a file has multiple names (via the link system), "deleting the file" in one place only deletes that reference to the file, not the actual content of the file. The content of the file isn't deleted until the last such reference is removed. In other words, when you "delete" the file from Directory A, the file is still there on the file system, but the only way to find it now is via the name it has in Directory B. When you delete the file from Directory B, there are no longer any directories that have a reference to that file, so the actual content of the file is deleted.

There are two limitations to hard links as implemented on typical file systems:

- Only regular files can be linked; directories cannot.
- File links only work within a file system.

What does Ziggy do? By default, Ziggy always uses links if it can; that is to say, it does so if the file system in question supports links and if the requested link is on the same file system as the original file. If Ziggy is asked to "copy" a directory from one place on a file system to another, Ziggy will create a new directory at the destination and then fill it with links to the files in the source directory.

If the file system doesn't support links, or if the datastore and the task directory are on separate file systems, Ziggy will use ordinary file copying rather than linking.

Why would a person ever want to put the datastore and the task directory on separate systems, given all of the aforementioned advantages of co-locating them? Turns out that there are security benefits to putting the datastore on a file system that's not networked all over the place, but rather is directly connected to a single computer (i.e., the one that's running Ziggy for you). By putting the task files on networked file systems, you can use all the other computers that mount that file system for processing data; when you then copy results back to the datastore on the direct-mounted file system, you've eliminated a risk that some other computer is going to come along and mess up your datastore. On the other hand, actually copying files creates performance issues because copying is extremely slow compared to linking, and it means that, at least temporarily, you have two copies of all your files taking up space (the task directory copy and the datastore copy). We report, you decide.

#### Why not Symlinks?

The same file systems that provide links also provide a different way to avoid copying files within a file system: symbolic links, also known as "symlinks" or (somewhat harshly) "slimelinks." Symlinks are somewhat more versatile than hard links: you can symlink to a directory, and you can have symlinks from one file system to another. Meanwhile, they give the same advantages in speed and disk space as hard links. Why doesn't Ziggy use them?

There are a few disadvantages of symlinks that were decisive in our thinking on this issue. Specifically:

- A symlink can target a file on another file system, but it doesn't change the way that the file systems are mounted. Consider a system in which there's a datastore file system that's not networked and a task directory file system that is networked. The Ziggy server creates symlinks on the task directory file system that target files on the datastore file system, then hands execution over to another computer. That computer tries to open the file on the task directory, but it's not really there. It's really on the datastore file system, which the algorithm computer can't read from. Boom! Execution fails.
- Symlinks create a potential data-loss hazard. Imagine that you have a symlink that targets a directory in the datastore. Meanwhile, the actual files in that directory aren't symlinks; they're real files. Now imagine a user `cd`'s into the symlink directory. When that user accesses the files, they're accessing the files that are in the datastore, not files that are in some other directory. This means that if that user `cd`'s into the directory (which is a symlink), they can `rm` datastore files without realizing it!
- Because symlinks can target directories as well as regular files, you can wind up with an extremely complicated system in which you have a directory tree that contains a mixture of symlinks and real files / real directories, and in each and every case you need to decide how to handle them. This can quickly become a quagmire from which one will have a lot of trouble escaping.

For all these reasons we decided to stick with hard links and eschew symlinks.

[[Previous]](intermediate-topics.md)
[[Up]](intermediate-topics.md)
[[Next]](rdbms.md)
