<!-- -*-visual-line-*- -->

[[Previous]](module-parameters.md)
[[Up]](configuring-pipeline.md)
[[Next]](pipeline-definition.md)

## The Datastore

"The Datastore" is a $10 word for an organized directory tree where Ziggy keeps the permanent copies of its various kinds of data files. These include the actual files of mission data, data product files, and a particular kind of metadata known as "instrument model files."

As the user, one of your jobs is to define the following for Ziggy:

- The layout of the datastore directory tree.
- The datastore locations and file name conventions for all of the data files used as inputs or outputs for your algorithms.
- The types of model files that your algorithms need, and the file name conventions for each.

The place for these definitions is in data file type XML files. These have names that start with "pt-" (for "Pipeline Data Type"); in the sample pipeline, the data file type definitions are in [config/pt-sample.xml](../../sample-pipeline/conf/pt-sample.xml).

Note that when we talk about data file types, we're not talking about data file formats (like HDF5 or geoTIFF). Ziggy doesn't care about data file formats; use whatever you like, as long as the algorithm software can read and write that format.

### The Datastore Directory Tree

Once you've spent a bit of time thinking about your algorithms and their inputs and outputs, you've probably got some sense of how you want to organize the directory tree for all those files. It's probably a bit intuitive and hard to put into words, but it's likely that you have some directory levels where there's just one directory with a fixed name, and others where you can have several directories with different names. If you have a directory "foo" that has subdirectories "bar" and "baz", the "foo" directory is an example of a fixed-name, all-by-itself-at-a-directory-level directory, while "bar" and "baz" are examples of a directory level where the directories can have one of a variety of different names.

The way that Ziggy puts these into words (and code) is that every level of a directory is a `DatastoreNode`, and `DatastoreNodes` can use another kind of object, a `DatastoreRegexp`, to define different names that a `DatastoreNode` can take on.

To make this more concrete (it could hardly be less concrete so far), let's consider the section of pt-sample.xml that defines the datastore directory tree:

```xml
  <!-- Datastore regular expressions. -->
  <datastoreRegexp name="dataset" value="set-[0-9]"/>

  <!-- Datastore node definitions. -->
  <datastoreNode name="dataset" isRegexp="true" nodes="L0, L1, L2A, L2B, L3">
    <datastoreNode name="L0"/>
    <datastoreNode name="L1"/>
    <datastoreNode name="L2A"/>
    <datastoreNode name="L2B"/>
    <datastoreNode name="L3"/>
  </datastoreNode>
```

The first thing you see is an example of a `DatastoreRegexp`. It has a `name` (`"dataset"`) and a `value` (`"set-[0-9]"`). The value is a *Java [regular expression](https://xkcd.com/208/)* (`"regexp"`), which is [defined by the Pattern class](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html). In this case, the regular expression will match "`set-0"`, `"set-1"`, etc. -- anything that's a combination of `"set-"` and a digit.

The next thing you see is a `DatastoreNode`, also named `"dataset".` It has an attribute, `isRegexp`, which is true. What does this mean? It means that there's a top-level directory under the datastore root directory which can have as its name anything that matches the value of the `"dataset"` `DatastoreRegexp`. More generally, it means that any directory under the datastore root that matches that value is a valid directory in the datastore! Thus, the "`dataset"` `DatastoreNode` means, "put as many directories as you like here, as long as they match the `dataset` regular expression, and I'll know how to access them when the time comes."

The `"dataset"` `DatastoreNode` also has another attribute: `nodes`, which has a value of `"L0, L1, L2A, L2B, L3"`. This tells Ziggy, "You should expect that any of these `dataset` directories will have subdirectories given by the `L0, L1, L2A, L2B`, and `L3` `DatastoreNode` instances." The `"dataset"` `DatastoreNode` then has elements that are themselves `DatastoreNode` instances, specifically the `"L0"`, `"L1"`, `"L2A"`, `"L2B"`, and `"L3"` nodes.

None of these 5 `DatastoreNode` instances has an `isRegexp` attribute. That means that none of them references any `DatastoreRegexp` instances; which in turn means that each of them represents a plain old directory with a fixed name.

Anyway, the point of this is that, at the top level of the datastore, we can have directories `set-1`, `set-2`, etc.; and each of those can have in it subdirectories named `L0`, `L1`, etc.

#### A More Complicated Example: Deeper Nesting

Let's consider our datastore layout again, but instead of putting all of the L* directories under the "dataset" directory, let's nest the directories, so that you wind up with directories like `set-0/L0`, `set-0/L0/L1`, etc. The obvious way to do that is like this:

```xml
  <!-- Datastore regular expressions. -->
  <datastoreRegexp name="dataset" value="set-[0-9]"/>

  <!-- Datastore node definitions. -->
  <datastoreNode name="dataset" isRegexp="true" nodes="L0">
    <datastoreNode name="L0" nodes="L1">
      <datastoreNode name="L1" nodes="L2A">
        <datastoreNode name="L2A"nodes="L2B">
          <datastoreNode name="L2B" nodes="L3">
            <datastoreNode name="L3"/>
          </datastoreNode>
        </datastoreNode>
      </datastoreNode>
    </datastoreNode>
  </datastoreNode>
```

This is a perfectly valid way to set up the datastore, but it's kind of a mess. There's a lot of nesting and a lot of `datastoreNode` closing tags, and between them it makes the layout hard to read and understand. For that reason, a better way to do it is like this:

```xml
  <!-- Datastore regular expressions. -->
  <datastoreRegexp name="dataset" value="set-[0-9]"/>

  <!-- Datastore node definitions. -->
  <datastoreNode name="dataset" isRegexp="true" nodes="L0">
    <datastoreNode name="L0" nodes="L1"/>
    <datastoreNode name="L1" nodes="L2A"/>
    <datastoreNode name="L2A" nodes="L2B"/>
    <datastoreNode name="L2B" nodes="L3"/>
    <datastoreNode name="L3"/>
  </datastoreNode>
```

Better, right?

#### An Even More Complicated Example

Now let's do something even more perverse: let's say that we want another L0 level under L2A but above L2B. That is to say, we want to make a directory like `set-0/L0/L1/L2A/L0` part of the datastore. Based on the example above, you might think that you could do this:

```xml
  <!-- Datastore regular expressions. -->
  <datastoreRegexp name="dataset" value="set-[0-9]"/>

  <!-- Datastore node definitions. -->
  <datastoreNode name="dataset" isRegexp="true" nodes="L0">
    <datastoreNode name="L0" nodes="L1"/>
    <datastoreNode name="L1" nodes="L2A"/>
    <datastoreNode name="L2A" nodes="L0"/>
    <datastoreNode name="L0" nodes="L2B"/>
    <datastoreNode name="L2B" nodes="L3"/>
    <datastoreNode name="L3"/>
  </datastoreNode>
```

In this case, though, you would be wrong! This won't work.

Why not?

The reason is that **every `DatastoreNode` within a parent `DatastoreNode` must have a unique name.** In this case, the `"dataset"` node contains two `"L0"` nodes, which is not allowed. If you wanted to do something like this, here's how you'd assemble the XML:

```xml
  <!-- Datastore regular expressions. -->
  <datastoreRegexp name="dataset" value="set-[0-9]"/>

  <!-- Datastore node definitions. -->
  <datastoreNode name="dataset" isRegexp="true" nodes="L0">
    <datastoreNode name="L0" nodes="L1"/>
    <datastoreNode name="L1" nodes="L2A">
      <datastoreNode name="L2A" nodes="L0"/>
      <datastoreNode name="L0" nodes="L2B"/>
    </datastoreNode>
    <datastoreNode name="L2B" nodes="L3"/>
    <datastoreNode name="L3"/>
  </datastoreNode>
```

This works because, although there are two nodes named `"L0"`, they are sub-nodes of different parents: one is under `"dataset"`, the other is under `"L1"`. The first one is the only `"L0"` that has `"dataset"` as its parent; the second one is the only `"L0"` that has `"L1"` as its parent.

Although the sample pipeline uses a pretty simple datastore layout, it's possible to implement extremely sophisticated layouts with the use of additional `DatastoreRegexp` instances, and so on.

### Mission Data

Now that we have the datastore layout defined, let's look at the next thing in the pt-sample file: data file type definitions. We'll just look at the first two:

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

A data file type declaration has three pieces of information: a `name,` a `location`, and a `fileNameRegexp`.

The `name` is hopefully self-explanatory.

What is a `location`? It's a valid, er, location in the datastore, as defined by the `DatastoreNode` instances. In the case of `raw data`, the `location` is `dataset/L0`. This means that `raw data` files can be found in directories `set-1/L0`, `set-2/L0`, etc. Note that the separator used in `location` instances is always the slash character. This is true even when the local file system uses some other character as its file separator in file path definitions.

The `fileNameRegexp` uses a Java regular expression to define the naming convention for files of the `raw data` type. For raw-data, the regular expression is `"(nasa-logo-file-[0-9])\.png"`. This means that `nasa-logo-file-0.png`, `nasa-logo-file-1.png`, etc., are valid names for `raw data` files. Note the backslash character before the "." character: this is necessary because "." has a special meaning in Java regular expressions. If you don't want it to have that meaning, but instead just want it to be a regular old period, you put the backslash character before the period.

Anyway, if you put it all together, this `DataFileType` is telling you that `raw data` files are things like `set-1/L0/nasa-logo-file-0.png`, `set-2/L0/nasa-logo-file-1.png`, and so on.

### Instrument Model Types

Before we can get into this file type definition, we need to answer a question:

#### What is an Instrument Model, Anyway?

We've given a lot of thought to how to define an instrument model. Here's the formal definition:

**Instrument models are various kinds of information that are needed to process the data. These can be things like calibration constants; the location in space or on the ground that the instrument was looking at when the data was taken; the timestamp that goes with the data; etc.**

The foregoing is not very intuitive. Here's a more colloquial definition:

**Instrument models are any kinds of mission information that you're tempted to hard-code into your algorithms.**

Think about it: when you write code to process data from an experiment, there's always a bunch of constants, coefficients, etc., that you need in order to perform your analysis. Unlike the data, these values don't change very often, so your first thought would be to just put them right into the code (or at least to hard-code the name and directory of the file that has the information). Anything that you'd treat that way is a model.

Our opinion is that model files are a better way to handle this type of information, rather than hard-coding. For one thing, Ziggy provides explicit tracking of model versions and supports model updates in a way that's superior to receiving a new file and then either copying and pasting its contents into your source code or putting the file into version control and changing a hard-coded file name in the source. It also supports models that can't easily be put into a repository, either because they're too big, because they're in a non-text format, or both.

#### Instrument Model Type Definition

Behold our sample instrument model type definition:

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

Note also that model type definitions don't require a defined `location`. Ziggy creates a subdirectory to the datastore root, `models`, and puts under that a subdirectory for every model type. So that's one set of decisions you don't need to make.

When a model is provided to an algorithm that needs it, the models infrastructure does the following:

First, it finds the most recent model of the specified type (which has the highest model number and also the most recent date stamp); then, it copies the file to the algorithm's working directory, but in the process it renames the file from the name it uses for storage (in this example, `2022-10-31.0001-sample-model.txt`) to the name it had when it was imported (in this example, `sample-model.txt`). In this way, Ziggy uses a name-mangling scheme to keep multiple model versions in a common directory, but then un-mangles the name for the algorithm, so the algorithm developers don't need to know anything about name-mangling; the name you expect the file to have is the name it actually will have.

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
