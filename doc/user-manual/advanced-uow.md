<!-- -*-visual-line-*- -->

[[Previous]](nicknames.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)

## Advanced Unit of Work Configurations

In the [article on the datastore](datastore.md) and the [article on the instances panel](instances-panel.md), we described the way in which a data file type is defined, the way that a unit of work is defined from the data file types, and the way that data files are obtained from the datastore based on the unit of work information. 

Just to review: if you have the following data file type definitions:

```xml
<dataFileType name="input1" location="foo/bar/baz/input1"
    fileNameRegexp="input1-([0-9]+)\.nc"/>
<dataFileType name="input2" location="foo/bar/baz/input2"
    fileNameRegexp="input2-([0-9]+)\.nc"/>
<dataFileType name="output" location="foo/bar/baz/output"
    fileNameRegexp="output-([0-9]+)\.nc"/>

<datastoreRegexp name="baz" value="(dog|cat|primate)"/>
```

This will generate units of work `[dog]`, `[cat]`, and `[primate]`, because the only regular expression in the locations of the data file types is `baz`, which can be "dog," "cat," or "primate." Data files from directory `foo/bar/dog/input1` and `foo/bar/dog/input2` will wind up in the task with unit of work `[dog]`, and so on.

This is fine as far as it goes, but it's a bit limited. In particular, `foo/bar/dog/input1`, `foo/bar/cat/input1`, and `foo/bar/primate/input1` each has to be a flat directory with all the input1 data files in it. In situations where you acquire a lot of data files, this may not be a desirable arrangement! Imagine, for example, that we acquire 1 file per hour for file type input1, and all those files want to go in the `foo/bar/dog` directory. After one year, there will be almost 9,000 files in `foo/bar/dog/input1`!

Imagine that we decide to organize the data for each data file type by year and by DOY (day of year, an integer that runs from 1 to 366). If we do that, the data file type and regexp definitions are as follows:

```xml
<dataFileType name="input1" location="foo/bar/baz/input1/year/doy"
    fileNameRegexp="input1-([0-9]+)\.nc"/>
<dataFileType name="input2" location="foo/bar/baz/input2/year/doy"
    fileNameRegexp="input2-([0-9]+)\.nc"/>
<dataFileType name="output" location="foo/bar/baz/output/year/doy"
    fileNameRegexp="output-([0-9]+)\.nc"/>

<datastoreRegexp name="baz" value="(dog|cat|primate)"/>
<datastoreRegexp name="year" value="[0-9]{4}"/>
<datastoreRegexp name="doy" value="[0-9]{3}"/>
```

That's better! Except that now, a unit of work will be a given DOY in a given year for one of "`dog, cat, primate`" values of baz. The UOW display will have things like `[dog;2019;322]`. Changing the layout of the datastore into something more convenient has forced us to change the unit of work definition, which we may not wish to do. We may have wanted the UOW to be defined by the value of baz only, but for the task to pull data from a user-selected set of year and DOY values. 

It turns out that there's a way to do this in Ziggy, and it's exactly what you (most likely) have already guessed. You can set up the datastore as follows:

```xml
<dataFileType name="input1" location="foo/bar/baz"
    fileNameRegexp="input1/year/doy/input1-([0-9]+)\.nc"/>
<dataFileType name="input2" location="foo/bar/baz"
    fileNameRegexp="input2/year/doy/input2-([0-9]+)\.nc"/>
<dataFileType name="output" location="foo/bar/baz"
    fileNameRegexp="output/year/doy/output-([0-9]+)\.nc"/>

<datastoreRegexp name="baz" value="(dog|cat|primate)"/>
<datastoreRegexp name="year" value="[0-9]{4}"/>
<datastoreRegexp name="doy" value="[0-9]{3}"/>
```

What happens now is as follows:

- The UOW is generated from the data file type location attributes. Thus we are once again back to `[dog], [cat], [primate]`.
- Within a UOW, Ziggy will search all the input1 subdirectories, across all values of year and DOY, to obtain data for the task. It will do likewise for input2. You'll wind up with a lot of subtasks, each of which has an input1 and an input2. For a given subtask, the input1 and input2 will come from the same year and DOY directories (i.e., you'll have a subtask with input1 from 2019/100, and it will have input2 fro 2019/100 as well).
- When the output file is persisted to the datastore, it will take into account the datastore directories that provided the inputs. For example, in the subtask with a file from `input1/2019/100` and one from `input2/2019/100`, the output file will go to directory `output/2019/100`.

That's better, but we've now apparently traded a huge number of extremely small UOWs for a small number of gigantic ones. If we have, say, 2 years of data, then the task with UOW `[dog]` will have almost 18,000 subtasks! That seems ... excessive.

Fortunately, the `year` and `doy` parts of the `fileNameRegexp` will respect the include and exclude restrictions you put on these `DatastoreRegexp` instances. As described in the [article on the datastore control panel](datastore-regexp.md), you can set the value of the `year` regexp to, for example, `2019`, and the `doy` regexp to, for example, `(100|101|102)`. When you now start the pipeline, the task with UOW `[dog]` will process data only from DOYs 100, 101, and 102 of year 2019. 

[[Previous]](nicknames.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)
