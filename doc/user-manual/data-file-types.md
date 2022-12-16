<!-- -*-visual-line-*- -->

[[Previous]](module-parameters.md)
[[Up]](configuring-pipeline.md)
[[Next]](pipeline-definition.md)

## Data File Types

As the user, one of your jobs is to define, for Ziggy, the file naming patterns that are used for the inputs and outputs for each algorithm, and the file name patterns that are used for instrument models. The place for these definitions is in data file type XML files. These have names that start with "pt-" (for "Pipeline Data Type"); in the sample pipeline, the data file type definitions are in [config/pt-sample.xml](../../sample-pipeline/conf/pt-sample.xml).

Note that when we talk about data file types, we're not talking about data file formats (like HDF5 or geoTIFF). Ziggy doesn't care about data file formats; use whatever you like, as long as the algorithm software can read and write that format.

### The Datastore and the Task Directory

Before we get too deeply into the data file type definitions, we need to have a brief discussion about two directories that Ziggy uses: the datastore, on the one hand, and the task directories, on the other.

#### The Datastore

"Datastore" here is just a $10 word for an organized directory tree where Ziggy keeps the permanent copies of its various kinds of data files. Files from the datastore are provided as inputs to the algorithm modules; when the modules produce results, those outputs are transferred back to the datastore.

Who defines the organization of the datastore? You do! The organization is implicitly defined when you define the data file types that go into, and come out of, the datastore. This will become clear in a few paragraphs (at least I hope it's clear).

#### The Task Directory

Each processing activity has its own directory, known as the "task directory." The task directory is where the algorithm modules look to find the files they operate on, and it's where they put the files they produce as results. Unlike the datastore, these directories are transient; once processing is complete, you can feel free to delete them at some convenient time. In addition, there are some other uses that benefit from the task directory. First, troubleshooting. In the event that a processing activity fails, you have in one place all the inputs that the activity uses, so it's easy to inspect files, watch execution, etc. In fact, you can even copy the task directory to some other system (say, your laptop) if that is a more convenient place to do the troubleshooting! Second, and relatedly, the algorithm modules are allowed to write files to the task directory that aren't intended to be persisted in the datastore. This means that the task directory is a logical place to put files that are used for diagnostics or troubleshooting or some other purpose, but which you don't want to save for posterity in the datastore.

#### And My Point Is?

The key point is this: the datastore can be, and generally is, heirarchical; the task directory is flat. Files have to move back and forth between these two locations. The implications of this are twofold. First, **the filenames used in the datastore and the task directory generally can't be the same.** You can see why: because the datastore is heirarchical, two files that sit in different directories can have the same name. If those two files are both copied to the task directory, one of them will overwrite the other unless the names are changed when the files go to the task directory.

Second, and relatedly, **the user has to provide Ziggy with some means of mapping the two filenames to one another.** Sorry about that; but the organization of the datastore is a great power, and with great power comes great responsibility.

### Mission Data

Okay, with all that throat-clearing out of the way, let's take a look at some sample data file type definitions.

```xml
<dataFileType name="raw data"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9]).png"
              fileNameWithSubstitutionsForDatastore="$2/L0/$1-$3.png"/>

<dataFileType name="permuted colors"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9])-perm.png"
              fileNameWithSubstitutionsForDatastore="$2/L1/$1-$3.png"/>
```

Each data file type has a name, and that name can have whitespace in it. That much makes sense.

#### fileNameRegexForTaskDir

This is how we define the file name that's used in the task directory. This is a [Java regular expression](https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/regex/Pattern.html) (regex) that the file has to conform to. For `raw data`, for example, a name like `some-kinda-name-set-1-file-9.png` would conform to this regular expression, as would `another_NAME-set-4-file-3.png`, etc.

#### fileNameWithSubstitutionsForDatastore

Remember that the task directory is a flat directory, while the datastore can be heirarchical. This means that each part of the path to the file in the datastore has to be available somewhere in the task directory name, and vice-versa, so that the two can map to each other.

In the `fileNameWithSubstitutionsForDatastore`, we accomplish this mapping. The way that this is done is that each "group" (one of the things in parentheses) is represented with $ followed by the group number. Groups are numbered from left to right in the file name regex, starting from 1 (group 0 is the entire expression). In raw data, we see a value of `$2/L0/$1-$3.png`. This means that group 2 is used as the name of the directory under the datastore root; `L0` is the name of the next directory down; and groups 1 and 3 are used to form the filename. Thus,  `some-kinda-name-set-1-file-9.png` in the task directory would translate to `set-1/L0/some-kinda-name-file-9.png` in the datastore.

Looking at the example XML code above, you can (hopefully) see what we said about how you would be organizing the datastore. From the example, we see that the directories immediately under the datastore root will be `set-0, set-1`, etc. Each of those directories will then have, under it, an `L0` directory and an `L1` directory. Each of those directories will then contain PNG files.

Notice also that the filenames of `raw data` files and `permuted colors` files in the datastore can potentially be the same! This is allowed because the `fileNameWithSubstitutionsForDatastore` values show that the files are in different locations in the datastore, and the `fileNameRegexForTaskDir` values show that their names in the task directory will be different, even though their names in the datastore are the same.

### Instrument Model Types

Before we can get into this file type definition, we need to answer a question:

#### What is an Instrument Model, Anyway?

Instrument models are various kinds of information that are needed to process the data. These can be things like calibration constants; the location in space or on the ground that the instrument was looking at when the data was taken; the timestamp that goes with the data; etc.

Generally, instrument models aren't the data that the instrument acquired (that's the mission data, see above). This is information that is acquired in some other way that describes the instrument properties. Like mission data, instrument models can use any file format that the algorithm modules can read.

#### Instrument Model Type Definition

Here's our sample instrument model type definition:

​    `<modelType type="dummy model" fileNameRegex="sample-model.txt"/>`

As with the data file types, model types are identified by a string (in this case, the `type` attribute) that can contain whitespace, and provides a regular expression that can be used to determine whether any particular file is a model of the specified type. In this case, in a fit of no-imagination, the regex is simply a fixed name of `sample-model.txt`. Thus, any processing algorithm that needs the `dummy model` will expect to find a file named `sample-model.txt` in its task directory.

#### Wait, is That It?

Sadly, no. Let's talk about model names and how they fit into all of this.

##### Datastore Model Names

Ziggy permanently stores every model of every kind that is imported into it. This is necessary because someday you may need to figure out what model was used for a particular processing activity, but on the other hand it may be necessary to change the model as time passes -- either because the instrument itself changes with time, or because your knowledge of the instrument changes (hopefully it improves).

But -- in the example above, the file name "regex" is a fixed string! This means that the only file name that Ziggy can possibly recognize as an instance of `dummy model` is `sample-model.txt`. So when I import a new version of `sample-model.txt` into the datastore, what happens? To answer that, let's take a look at the `dummy model` subdirectory of the `models` directory in the datastore:

```console
models$ ls "dummy\ model"
2022-10-31.0001-sample-model.txt
models$
```

(Yes, I broke my own strongly-worded caution against using whitespace in names, and in a place where it matters a lot -- a directory name! Consistency, hobgoblins, etc.)

As you can see, the name of the model in the datastore isn't simply `sample-model.txt`. It's had the date of import prepended, along with a version number. By making these changes to the name, Ziggy can store as many versions of a model as it needs to, even if the versions all have the same name at the time of the import.

##### Task Directory Model Names

Ziggy also maintains a record of the name the model file had at the time of import. When the model is provided to the task directory so the algorithms can use it, this original name is restored. This way, the user never needs to worry about Ziggy's internal renaming conventions; the algorithms can use whatever naming conventions the mission uses for the model files, even if the mission reuses the same name over and over again.

##### Which Version is Sent to the Algorithms?

The most recent version of each model is the one provided to the algorithms at runtime. If there were 9 different models in `dummy model`, the one with version number `0009` would be the one that is copied to the task directories. If, some time later, a tenth version was imported, then all subsequent processing would use version `0010`.

##### What Happens if the Actual Model Changes?

Excellent question! Imagine that, at some point in time, one or more models change -- not your knowledge of them, the actual, physical properties of your instrument change. Obviously you need to put a new model into the system to represent the new properties of the instrument. But equally obviously, if you ever go back and reprocess data taken prior to the change, you need to use the model that was valid at that time. How does Ziggy handle that?

Answer: Ziggy always, *always* provides the most recent version of the model file. If you go and reprocess, the new processing will get the latest model. In order to properly represent a model that changes with time, **the changes across time must be reflected in the most recent model file!** Also, and relatedly, **the algorithm code must be able to pull model for the correct era out of the model file!**

In practice, that might mean that your model file contains multiple sets of information, each of which has a datestamp; the algorithm would then go through the file contents to find the set of information with the correct datestamp, and use it. Or, it might mean that the "model" is values measured at discrete times that need to be interpolated by the algorithm. How the time-varying information is provided in the model file is up to you, but if you want to have a model that does change in time, this is how you have to do it.

##### Model Names with Version Information

The above example is kind of unrealistic because in real life, a mission that provides models that get updated will want to put version information into the file name; if for no other reason than so that when there's a problem and we need to talk about a particular model version, we can refer to the one we're concerned about without any confusion ("Is there a problem with sample model?" "Uh, which version of sample model?" "Dunno, it's just called sample model."). Thus, the file name might contain a timestamp, a version number, or both.

If the model name already has this information, it would be silly for Ziggy to prepend its own versioning; it should use whatever the mission provides. Fortunately, this capability is provided:

```xml
<modelType type="versioned-model"
           fileNameRegex="([0-9]{4}-[0-9]+)_eo1hyp_metadata_updates([0-9]+).xlsx"
           versionNumberGroup="2" timestampGroup="1"/>
```

In this case, the XML attribute `versionNumberGroup` tells Ziggy which regex group it should use as the version number, and the attribute `timestampGroup` tells it which to use as the file's timestamp. When Ziggy stores this model in the `versioned-model` directory, it won't rename the file; it will keep the original file name, because the original name already has a timestamp and a version number.

In general, the user can include in the filename a version number; a timestamp; or both; or neither. Whatever the user leaves out, Ziggy will add to the filename for internal storage, and then remove again when providing the file to the algorithms.

##### Models Never Get Overwritten in the Datastore

One thing about supplying timestamp and version information in the filename is that it gives some additional protection against accidents. **Specifically: Ziggy will never import a model that has the same timestamp and version number as one already in the datastore.** Thus, you can never accidentally overwrite an existing model with a new one that's been accidentally given the same timestamp and version information.

For models that don't provide that information in the filename, there's no protection against such an accident because there can't be any such protection. If you accidentally re-import an old version of `sample-model.txt`, Ziggy will assume it's a new version and store it with a new timestamp and version number. When Ziggy goes to process data, this version will be provided to the algorithms.

[[Previous]](module-parameters.md)
[[Up]](configuring-pipeline.md)
[[Next]](pipeline-definition.md)
