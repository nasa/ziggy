## Data Receipt Execution Flow

Remember data receipt? Here's where we get into how it works. You'll want to know this when you're setting up data transfers in your own mission.

### Data Files and Manifest

The sample pipeline's data receipt directory uses a copy of the files from the `data` subdirectory in the `sample-pipeline` main directory. Let's take a look at that directory now:

```console
sample-pipeline$ ls data
nasa_logo-set-1-file-0.png
nasa_logo-set-1-file-3.png
nasa_logo-set-2-file-2.png
sample-pipeline-manifest.xml
nasa_logo-set-1-file-1.png
nasa_logo-set-2-file-0.png
nasa_logo-set-2-file-3.png
nasa_logo-set-1-file-2.png
nasa_logo-set-2-file-1.png
sample-model.txt
sample-pipeline$
```

Most of these files are obviously the files that get imported. But what about the manifest? Here's the contents of the manifest:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<manifest datasetId="0" checksumType="SHA1" fileCount="9">
  <file name="nasa_logo-set-2-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-1-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-1-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-1-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-1-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-2-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-2-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="nasa_logo-set-2-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed"/>
  <file name="sample-model.txt" size="2271" checksum="c99ad366e36c84edeacf72769dc7ad6dc0465ac0"/>
</manifest>
```

Some parts of this are obvious: the number of files in the delivery, the fact that every file has an entry in the manifest, every file's size is listed in the manifest.

The `datasetId` is a unique identifier for a data delivery. This serves two purposes:

1. It prevents you from re-importing the same files multiple times, as the `datasetId` from each successful import is saved and new imports are checked against them (exception: `datasetId` 0 can be reused).
2. When there's a problem with the delivery and you need to work the issue with whoever sent it to you, it lets you refer to exactly which one failed: "Yeah, uh, looks like dataset 12345 won't import. Got a minute?"

The `checksumType` is the name of the algorithm that's used to generate a checksum for each file in the manifest. In this example we're using SHA1, which is a reasonable balance between calculation speed, checksum size, and checksum quality (anyway, we're not using these SHA1 hashes for secure communication, we're just using them to make sure the files didn't get corrupted).

Each file has a `checksum` that's computed using the specified `checksumType`.

### The Data Receipt Directory

Data receipt needs to have a directory that's used as the source for files that get pulled into the datastore. There's a [property in the properties file](properties.md) that specifies this, namely `data.receipt.dir`. Ziggy allows this directory to be used in either of two ways.

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
  <file name="nasa_logo-set-2-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-1-file-3.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-1-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-1-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-1-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-2-file-0.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-2-file-1.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="nasa_logo-set-2-file-2.png" size="72407" checksum="0b30f7f0028f2cee5686d3169fff11e76a96fbed" transferStatus="present" validationStatus="valid"/>
  <file name="sample-model.txt" size="2271" checksum="c99ad366e36c84edeacf72769dc7ad6dc0465ac0" transferStatus="present" validationStatus="valid"/>
</acknowledgement>
```

Note that the manifest file must end with "`-manifest.xml`", and the acknowledgement file will end in "`-manifest-ack.xml`", with the filename prior to these suffixes being the same for the two files.

### Systems that Treat Directories as Data Files

There may be circumstances in which it's convenient to put several files into a directory, and then to use a collection of directories of that form as "data files" for the purposes of data processing. For example, consider a system where there's a data file with an image, and then several files that are used to background-subtract the data file. Rather than storing each of those files separately, you might put the image file and its background files into a directory; import that directory, as a whole, into the datastore; then supply that directory, as a whole, as an input for a subtask.

In that case, the manifest still needs to have an entry for each regular file, but in this case the name of the file includes the directory it sits in. Here's what that looks like in this example:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<manifest datasetId="0" checksumType="SHA1" fileCount="10">
  <file name="data-00001/image.png" size="100" checksum="..."/>
  <file name="data-00001/background-0.png" size="100" checksum="..."/>
  <file name="data-00001/background-1.png" size="100" checksum="..."/>
  <file name="data-00002/image.png" size="100" checksum="..."/>
  <file name="data-00002/background-0.png" size="100" checksum="..."/>
  <file name="data-00002/background-1.png" size="100" checksum="..."/>
  <file name="data-00002/background-2.png" size="100" checksum="..."/>
  <file name="data-00003/image.png" size="100" checksum="..."/>
  <file name="data-00003/background-0.png" size="100" checksum="..."/>
  <file name="data-00003/background-1.png" size="100" checksum="..."/>
</manifest>
```

Now the only remaining issue is how to tell Ziggy to import the files in such a way that each of the `data-#####` directories is imported and stored as a unit. To understand how that's accomplished, let's look back at the data receipt node in `pd-sample.xml`:

```xml
<node moduleName="data-receipt" childNodeNames="permuter">
  <inputDataFileType name="raw data"/>
  <moduleParameter name="Data receipt configuration"/>
</node>
```

Meanwhile, the definition of the raw data type is in `pt-sample.xml`:

```xml
<dataFileType name="raw data"
              fileNameRegexForTaskDir="(\\S+)-(set-[0-9])-(file-[0-9]).png"
              fileNameWithSubstitutionsForDatastore="$2/L0/$1-$3.png"/>
```

Taken together, these two XML snippets tell us that data receipt's import is going to import files that match the file name convention for the `raw data` file type. We can do the same thing when the "file" to import is actually a directory. If you define a data file type that has `fileNameRegexForTaskDir` set to `data-[0-9]{5}`, Ziggy will import directory `data-00001` and all of its contents as a unit and store that unit in the datastore, and so on.

Note that the manifest ignores the fact that import of data is going to treat the `data-#####` directories as the "files" it imports, and the importer ignores that the manifest validates the individual files even if they are in these subdirectories.

### Generating Manifests

Ziggy also comes with a utility to generate manifests from the contents of a directory. Use `runjava generate-manifest`. This utility takes 3 command-line arguments:

1. Manifest name, required.
2. Dataset ID, required.
3. Path to directory with files to be put into the manifest, optional (default: working directory).

