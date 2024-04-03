<!-- -*-visual-line-*- -->

[[Previous]](data-receipt.md)
[[Up]](data-receipt.md)
[[Next]](data-receipt-display.md)

## Data Receipt Execution Flow

At the highest level, the purpose of data receipt is to take files delivered from someplace outside the pipeline and pull them into the pipeline's datastore. This seems it would be pretty straightforward! What makes it interesting is that there are additional, implicit, demands put on the system in the context of a flight mission:

- Most crucially, the integrity of the delivery has to be ensured. In a (largish) nutshell, this entails the following:
  - All of the files that you expected to receive actually showed up.
  - No files showed up that are not expected.
  - The files that showed up were not somehow corrupted in transit.
- Whoever it was that delivered the files may require a notification that there were no problems with the delivery, so data receipt needs to produce something that can function as the notification.
- The data receipt process needs to clean up after itself. This means that there is no chance that a future data receipt operation fails because of some debris left from a prior data receipt operation, and that there is no chance that a future data receipt operation will inadvertently re-import files that were already imported.

The integrity of the delivery is supported by an XML file, the *manifest*, that lists all of the delivered files and contains size and checksum information for each one. After a successful import, Ziggy produces an XML file, the *acknowledgement*, that can be used as a notification to the source of the files that the files were delivered and imported without incident. The cleanup is managed algorithmically by Ziggy.

With that, let's dive in!

### Data Files and Manifest

The sample pipeline's data receipt directory uses a copy of the files from the `data` subdirectory in the `sample-pipeline` main directory. Let's take a look at that directory now:

```console
sample-pipeline$ ls data
models
sample-pipeline-manifest.xml
set-1
set-2
sample-pipeline$
```

Look more closesly and you'll see that only sample-pipeline-manifest.xml is a regular file. The other files are all directories. Let's look into them and see what's what:

```bash
sample-pipeline$ ls data/set-1/L0
nasa-logo-file-0.png
nasa-logo-file-1.png
nasa-logo-file-2.png
nasa-logo-file-3.png
sample-pipeline$
```

If we look at the `set-2` directory, we'll see something analogous. Meanwhile, the `models` directory looks like this:

```bash
sample-pipeline$ ls data/models
sample-model.txt
sample-pipeline$
```

From looking at this, you've probably already deduced the two rules of data receipt layout:

1. The mission data must be in a directory tree that matches the datastore, such that each file's location in the data receipt directory tree matches its destination in the datastore.
2. All model files must be in a `models` directory within the data receipt directory.

Now let's look at the manifest file:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<manifest datasetId="0" checksumType="SHA1" fileCount="9">
    <file name="set-1/L0/nasa-logo-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-2/L0/nasa-logo-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-1/L0/nasa-logo-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-2/L0/nasa-logo-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-1/L0/nasa-logo-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-2/L0/nasa-logo-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-1/L0/nasa-logo-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="set-2/L0/nasa-logo-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
    <file name="models/sample-model.txt" size="2477" checksum="694cd3668fd1ec7e0e826bb1b211f4d2a5459628"/>
</manifest>
```

Some parts of this are obvious: the number of files in the delivery, the fact that every file has an entry in the manifest, every file's size is listed in the manifest.

The `datasetId` is a unique identifier for a data delivery. This serves two purposes:

1. It prevents you from re-importing the same files multiple times, as the `datasetId` from each successful import is saved and new imports are checked against them (exception: `datasetId` 0 can be reused).
2. When there's a problem with the delivery and you need to work the issue with whoever sent it to you, it lets you refer to exactly which one failed: "Yeah, uh, looks like dataset 12345 won't import. Got a minute?"

The `checksumType` is the name of the algorithm that's used to generate a checksum for each file in the manifest. In this example we're using SHA1, which is a reasonable balance between calculation speed, checksum size, and checksum quality (anyway, we're not using these SHA1 hashes for secure communication, we're just using them to make sure the files didn't get corrupted).

Each file has a `checksum` that's computed using the specified `checksumType`.

### The Data Receipt Directory

Data receipt needs to have a directory that's used as the source for files that get pulled into the datastore. There's a [property in the properties file](properties.md) that specifies this, namely `ziggy.pipeline.data.receipt.dir`. Ziggy allows this directory to be used in either of two ways.

#### Files in the Data Receipt Directory

Option 1 is for all the files, and the manifest, to be in the data receipt directory. In this case, the data receipt pipeline node will produce 1 task.

#### Files in Data Receipt Subdirectories

Option 2 is that there are no data files or manifests in the top-level data receipt directory. Instead, there are subdirectories within data receipt, each of which contains files for import and a manifest. In this case, data receipt will create a pipeline task per subdirectory.

### What Data Receipt Does

Here's the steps data receipt takes, in order.

#### Validate the Manifest

In this step, Ziggy checks that every file that's present in the manifest is also present in the directory, and that the size and checksum of every file is correct. Ziggy produces an acknowledgement file that lists each file with its transfer status (was the file there?) and its validation status (were the size and checksum correct?), plus an overall status for the transfer (did any file fail either of its validations?).

#### Look for Files Not Listed in the Manifest

The step above ensures that every file in the manifest was transferred, but it doesn't rule out that there were extra files transferred that aren't in the manifest. Ziggy now checks to make sure that there aren't any such extra files, and throws an exception if any are found. The idea here is that Ziggy can't tell whether an extra file is supposed to be imported or not, and if it is supposed to be imported there's no size or checksum information to validate it with. So to be on the safe side, better to stop and ask for help.

#### Do the Imports

At this point Ziggy loops through the directory and imports all the files into the datastore. As each file is imported into the datastore it's removed from the data receipt directory.

#### Clean Up the Data Receipt Directory

All manifest and acknowledgement files are transferred to the `manifests` sub-directory of the `logs` directory.

Empty subdirectories of the data receipt directory are removed. If the data receipt directory has any remaining content other than the .manifests directory, an exception is thrown. An exception at this point due to non-empty directories means that files that were supposed to be imported weren't.

### The Acknowledgement XML File

In the interest of completeness, here's the content of the acknowledgement file for the sample pipeline data delivery:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<acknowledgement datasetId="0" checksumType="SHA1" fileCount="9" transferStatus="valid">
    <file name="set-1/L0/nasa-logo-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-2/L0/nasa-logo-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-1/L0/nasa-logo-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-2/L0/nasa-logo-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-1/L0/nasa-logo-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-2/L0/nasa-logo-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-1/L0/nasa-logo-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="set-2/L0/nasa-logo-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
    <file name="models/sample-model.txt" size="2477" checksum="694cd3668fd1ec7e0e826bb1b211f4d2a5459628" transferStatus="present" validationStatus="valid"/>
</acknowledgement>
```

Note that the manifest file must end with "`-manifest.xml`", and the acknowledgement file will end in "`-manifest-ack.xml`", with the filename prior to these suffixes being the same for the two files.

### Generating Manifests

Ziggy also comes with a utility to generate manifests from the contents of a directory. Use `ziggy generate-manifest`. This utility takes 3 command-line arguments:

1. Manifest name, required.
2. Dataset ID, required.
3. Path to directory with files to be put into the manifest, optional (default: working directory).

[[Previous]](data-receipt.md)
[[Up]](data-receipt.md)
[[Next]](data-receipt-display.md)
