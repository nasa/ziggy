<!-- -*-visual-line-*- -->

[[Previous]](version-tracking.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)

## HDF5

In the course of pipeline execution, it's often convenient to have a binary data format (i.e., something other than a text file) that can be written and read by the software. It's also in the interest of the beleagured developers of the pipeline software to minimize the number of different binary formats that need to be supported. 

Ziggy has chosen [HDF5](https://www.hdfgroup.org/solutions/hdf5/) as its primary means of storing data in a binary format. 

### What is HDF5?

HDF5 stands for "Hierarchal Data Format, version 5". As the name implies, it allows data to be stored in a tree (or "hierarchy") in which a data object can contain data, metadata, or even additional data objects. In this way an HDF5 file represents something like a data struct (for you older programmers) or a data class with additional embedded classes (for you younger programmers). If you're a Python programmer, you can think of it as a dictionary, in which the dictionary entries can themselves be dictionaries. 

This is a standard data format that provides the flexibility to represent structs, is widely used, and is maintained by somebody other than us. For all these reasons, we've selected HDF5 as our go-to binary format.

### Where Does Ziggy use HDF5?

In any use-case where we have a data file that (a) would be painful to represent as a text file, and (b) might be of interest to the user, we use HDF5. This includes all of the following:

- Input files (see [The Datastore and the Task Dir](datastore-task-dir.md))
- Stack traces from failed subtasks
- Task configuration files
- Time series of subtask memory consumption
- Subtask wall times.

Okay, if we're being honest, the stack traces and the task configuration files probably aren't really of much interest to users, which is why we don't talk about them in this manual. These files are HDF5 because (a) the data in the files is easy to store as HDF5, and (b) the Ziggy development team occasionally finds it useful to open these files up and see what they can tell us. 

### How Does a User Read HDF5?

Most programming languages provide some form of HDF5 API. Unfortunately, most of them are pretty bad. There are various issues, but the big issue is this: the typical HDF5 API doesn't provide a tool that walks through an HDF5 file's hierarchy and convert the contents of the file into a struct or dictionary or class instance. That is to say: you, the user, need to hand-code support for the "hierarchy" part of HDF5! On top of that, HDF5 is extremely flexible and provides a lot of features for structuring data or attaching metadata. This means that writing code that uses an HDF5 API requires a lot of understanding of the inner workings of HDF5, which most people cannot and should not be expected to have or to develop. 

In the interest of sanity, Ziggy provides extremely high level HDF5 APIs that perform all of that hierarchical angle-bargle for you. All you need to do is point the API at a file (for reading), or provide it with a data object (for writing). The API does all the rest. 

The current design provides this API for Python, MATLAB, C++, and Java.

#### Python HDF5 API

The Python HDF5 API is located in the Ziggy code tree, at src/main/python/ziggy/ziggytools/hdf5.py. This module defines a class, Hdf5AlgorithmInterface, which provides the top-level methods read_file and write_file. 

An HDF5 file that's read into Python by Hdf5AlgorithmInterface is represented as a dictionary, which in turn can have other dictionaries or lists of dictionaries as entries (thus the "hierarchy" part of HDF5). 

Data values are represented in the dictionary as either primitive values or as tuples, depending on whether there is just one value (a "scalar") or multiple values. For multi-dimensional arrays, the data is represented using nested tuples (curse the day the Python architects decided not to include a built-in array data type). 

#### MATLAB HDF5 API

For MATLAB, Ziggy provides the hdf5ConverterClass, which is a MATLAB class as the name implies. This can be found in src/main/matlab/@hdf5ConverterClass . This provides the same read_file and write_file methods as the Python API. 

HDF5 files are converted to MATLAB structs. Struct fields that represent data (i.e., fields that are not themselves structs) use standard MATLAB arrays.

#### C++ HDF5 API

The C++ API for HDF5 is the Hdf5AlgorithmInterface class, located at src/main/cpp/libziggymi . 

Here's the bad news about the C++ API: remember how we said that our APIs handle the chore of walking through the hierarchy? Unfortunately, that's not possible with C++. 

Why not?

Python and MATLAB (above) do not have strong typing: you don't have to tell them in advance, "the next thing I'm going to read is a one-dimensional array of floats," or whatever. This means that those APIs can figure out what kind of data they're reading and they can just -- stick it into the dictionary or struct, no matter what they are, no fuss no muss. C++, by contrast, is strongly-typed, which means that you can only set a variable equal to, say, an array of floats if that variable is defined to be an array of floats. Put another way, you need to know in advance how the data you're going to read in is organized. 

Java (see below) is also strongly-typed, so you need to know what kind of data you're expecting for each field in a given class instance. Fortunately, Java has introspection, which means that you can take a Java Object and figure out what data types are required for each of the fields. C++ does not have introspection: if you pass a generic object to a C++ program, that program can't figure out what the fields are in the object. 

Put these together and you find that there's no general-purpose way to populate a generic object with data from HDF5. The user winds up back needing to write dedicated code for any given C++ class that they want to populate from HDF5. The Ziggy API does help in this regard: it provides readers and writers for each supported data type, and those readers and writers handle all of the additional overhead of setting metadata, etc. 

#### Java HDF5 API

The Java API is the Hdf5AlgorithmInterface class, located in package src/main/java/gov/nasa/ziggy/pipeline/step/hdf5 . It provides readFile and writeFile methods. 

Like C++ (above), Java is strongly-typed. This means that there's no way to define a Java class in a totally generic way in which it doesn't need to know anything about the data it's reading from an HDF5 file into a Java object. You have to define a Java class that matches the data you're going to read into the class instances from HDF5. 

The good news is that Java permits introspection. What this means is that, if you're given a generic Object, it's possible to determine the fields of that object and even its actual class. 

Put these together and what it means is that, to read content from HDF5, you need an instance of a Java class where the fields of the instance match the arrangement of data you want to read; but it's possible to have a generic "read this file into this object" method that will work for any instance of any object. 

### Limitations of the Ziggy HDF5 APIs

In order to make it possible to have general, top-level read and write functions for HDF5 files, it was necessary to limit the potential contents of the files that Ziggy has to read and write. 

#### Top-Level

The data in the file represents a single struct / dictionary / object. It can't be a primitive data value, an array of primitives, or an array of structs / dictionaries / objects. 

#### Data Types

The HDF5 APIs can only manage the following data types:

- Boolean / logical
- Integers: 1, 2, 4, or 8 bytes (or more generally byte, short, int, long)
- Single and double precision floating point numbers (4 byte and 8 byte, respectively)
- Text strings
- Structs / dictionaries / objects.

The Java API can also store and retrieve enumerations; these are stored in HDF5 as Strings, where the stored String is the name of the enumeration.

#### Collections

The Ziggy HDF5 APIs can also store and retrieve single- or multi-dimensional arrays of any of the accepted data types. This includes structs / dictionaries / objects. The only exception to this is that the top-level struct / dictionary / object has to be a singleton, as described above.

In MATLAB, C++, and Java, arrays are represented using the native array data type of the language. For Python, as mentioned above, there is no native array data type; here, arrays are represented as tuples or as tuples of tuples (curse the day the Python architects decided not to include a built-in array data type). 

[[Previous]](version-tracking.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)

