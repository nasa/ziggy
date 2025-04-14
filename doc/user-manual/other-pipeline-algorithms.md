<!-- -*-visual-line-*- -->

[[Previous]](python-pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](algorithm-parameters.md)

## Other Pipeline Algorithms

If you're not writing your algorithms in Python, you're writing them in some other language (nice tautology). In this case, your step definitions look something like this:

```xml
<step name="permuter" description="Color Permuter"/>
<step name="flip" file="flipper" description="Flip Up-Down and Left-Right"/>
<step name="averaging" description="Average Images Together"/>
```

### Attributes of a Pipeline Step XML Element

As you can see, the step XML element has three attributes:

#### The `description`

Optional. Just a plain-text description of what the step does.

#### The `file`

Optional. The name of the file Ziggy executes when it reaches this processing step.

#### The `name`

Required. A string with no whitespace that is unique in your pipeline. The name is how the steps will be referenced when the time comes to put the processing steps together into a complete pipeline. Also, if the `file` attribute is omitted, Ziggy will use the step name as the file it will execute when your pipeline reaches the step in question. 

You may wonder why Ziggy has an optional `file` attribute when you can always use the `name` attribute to specify the name of your algorithm. This is done so that, if you have one executable that is used by multiple pipeline steps, you don't need to keep multiple copies of the same file, with different names, in order to support that use-case. You can have multiple steps, each with a unique name, and all of which point at the same executable. 

### Where are the Pipeline Step Executables?

In the section above, we asserted that Ziggy will try to run the program named by the `file` attribute, and if there's no such attribute it will run the program named by the `name` attribute. Fair enough, but that's not really enough information to allow Ziggy to figure out what it needs to run! The executables have to be someplace that Ziggy can find them. 

There are two ways to specify the location of the executable files (remember, these are the files you wrote, so Ziggy can't find them without your cooperation).

- By default, Ziggy will look for the executables in the `bin` subdirectory of your pipeline's home directory. The home directory, in turn, is specified by the `ziggy.pipeline.home.dir` property of the properties file (see the article [The Properties File](properties.md)). If we consider the sample pipeline's properties file ([etc/ziggy.properties](../../etc/ziggy.properties)), the value of `ziggy.pipeline.home.dir` translates to `sample-pipeline/build`, which means that the first place Ziggy will look for your executables is in `sample-pipeline/build/bin`.
- If for whatever reason you don't want to put them in the `bin` subdirectory of your pipeline's home directory, there's another property you can use. It's the `ziggy.pipeline.binPath` property. This is a typical Linux collection of paths separated by colons (so `foo:bar:baz` would be the directories `foo`, `bar`, and `baz`). These are the directories that Ziggy will search after the `bin` subdirectory of pipeline home.
- The `ziggy.pipeline.binPath` can also be used for executables that aren't your handiwork, and which you can't put into the `bin` subdirectory of the home directory. Consider the following absurd example: you have a pipeline step in the usual place, but at some point during execution it needs to call the Linux file list program, `ls`. You obviously don't want to copy ls to your pipeline home directory tree, and moving it is out of the question! The alternative is to put the location of ls on your system into the `ziggy.pipeline.binPath` property.
- If your executables make use of shared libraries, you'll need to use the `ziggy.pipeline.libPath` property to make those libraries available to your pipeline executables.

### What are the Rules for Pipeline Steps?

Given that you have to write the pipeline steps, you'd probably like to know the design rules for steps. There are two:

- The step has to be a program that can be executed at a command line without any additional parameters. 
- The step must return an exit code of zero in the event of successful completion, and a nonzero exit code in the event of failure. 

That's obviously fine if your pipeline step is a compiled program or a shell script, but that generally won't be the case. Most algorithm code is written in languages that require additional information or resources in order to run. 

#### Implementing "Glue" Code

The generic approach if your program needs help is to write a layer of "glue" code that can meet the requirement of running independently and returning an exit value. The "glue" code can be the file in the `bin` subdirectory of the pipeline home directory. In turn, it can call your algorithm code. 

To make this concrete, consider algorithms written in Java. Java classes can't be run directly at the command line. They require the `java` program itself, which invokes the Java Virtual Machine (JVM), which is the thing that can actually run the program. You may also want to provide it with a class path, some parameters, etc.

Given all that, let's imagine that your algorithm code is in the Java class `Foo.java`, which has a fully-qualified name of `org.phony.Foo`. Let's also imagine that you want to supply some classpath information for the JVM, and you want to request a large heap size allocation. The command you would type at the command line would be something like:

```bash
java -cp here/*:there/* -Xmx500G org.phony.Foo
```

Ziggy obviously can't do much with that. However, what you would do is to write a shell script, `foo`, which contains the following:

```bash
#! /bin/bash

java -cp here/*:there/* -Xmx500G org.phony.foo
exit $?
```

Ziggy will run `foo`, `foo` will run the `org.phony.Foo` Java class, and the last line -- `exit $?` -- will take the exit code from the java program and use it as the exit code for the shell script, so that if your Java class crashes, Java will return a nonzero exit code, then foo will return that same code, and Ziggy will know that something went wrong.

### Using Libraries

Your step may need additional libraries in order to run. A program written in C++ may require shared libraries (normally supplied via `LD_LIBRARY_PATH`), and a Java program may require native libraries (normally supplied via `JNA_PATH`). Ziggy can supply the locations of these libraries with the `ziggy.pipeline.libPath` and `ziggy.pipeline.environment` properties. The latter is used to pass the `JNA_PATH` variable For example, the TESS project specifies the following:

```
ziggy.pipeline.binPath=${ziggy.pipeline.home.dir}/bin
ziggy.pipeline.environment = ZIGGY_HOME=${ziggy.home.dir}, MATLAB_JAVA=${matlab.java.home}, JNA_PATH=${ziggy.pipeline.home.dir}/lib
ziggy.pipeline.libPath=${ziggy.home.dir}/lib:${ziggy.pipeline.home.dir}/lib
```

### Deployed MATLAB Support

Ziggy really got its start running algorithms that are written in MATLAB, then converted to a deployed executable (via the MATLAB Compiler). The upside of a deployed MATLAB executable is that it can be run without a MATLAB license, which you'll definitely want to be able to do when you're running several thousand processes in parallel. The downside is that you still need to provide it with the locations of MATLAB's runtime libraries (which can be either in the directory of your MATLAB installation, or can be obtained as the MATLAB Compiler Runtime if you're running deployed MATLAB but don't have a MATLAB installation yourself). 

Ziggy can detect that a pipeline step is a deployed MATLAB algorithm. It does this by looking for an additional file produced by the compiler, the required MCR products file. If your deployed application is named `foo`, it will be accompanied by a file named `foo-requiredMCRProducts.txt`. When Ziggy sees this, it will attempt to put the runtime libraries on the library search path. The way that you tell Ziggy where those files are located is via the `ziggy.pipeline.mcrRoot` property in the properties file. Ziggy will reach down into the directory identified by that property, locate the subdirectories of runtime libraries, and put them on the library path that the deployed application searches for library files.

[[Previous]](python-pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](algorithm-parameters.md)