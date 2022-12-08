<!-- -*-visual-line-*- -->

## Downloading and Building Ziggy

Before you start, you should check out the [system requirements](system-requirements.md) article. This will ensure that you have the necessary hardware and software to follow the steps in this article. 

### Downloading Ziggy

Ziggy's source code is stored on GitHub, which you probably know already since you're reading this document, which is also stored on GitHub along with Ziggy. From Ziggy's [home page](https://github.com/nasa/ziggy), you can download the code in your favorite fashion. You can press the green Code button to obtain the URL to clone the repository with, for example, `git clone https://github.com/nasa/ziggy.git`. You can view the latest release in the [Releases](https://github.com/nasa/ziggy/releases), download the tarball using the link at the bottom of the release notes, and extract it with, for example, `tar -xf ziggy-0.2.0.tar.gz`).

Once you've done that, you should see something like this in your Ziggy folder:

```console
ziggy$ ls
LICENSE.pdf     doc                  gradlew            script-plugins
README.md       etc                  ide                settings.gradle
build.gradle    gradle               licenses           src
buildSrc        gradle.properties    sample-pipeline    test
ziggy$ 
```

Let's go through these items:

- The files `build.gradle`, `settings.gradle`, `gradle.properties`, and `gradlew` are used by our build system. Hopefully you won't need to know anything more about them than that.
- Likewise, directories `gradle` and `script-plugins` are used in the build.
- The `buildSrc` directory contains some Java and Groovy classes that are part of Ziggy but are used by the build. The idea here is that Gradle allows users to extend it by defining new kinds of build tasks; those new kinds of build tasks are implemented as Java or Groovy classes and by convention are put into a "buildSrc" folder. This is something else you probably won't need to worry about; certainly not any time soon.
- The `doc` directory contains this user manual, plus a bunch of other, more technical documentation. 
- The `etc` directory contains files that are used as configuration inputs to various programs. This is things like: the file that tells the logger how to format text lines, and so on. Two of these are going to be particularly useful and important to you: the ziggy.properties file, and the pipeline.properties.EXAMPLE file. These files provide all of the configuration that Ziggy needs to locate executables, working directories, data storage, and etc. We'll go into a lot of detail on this at the appropriate time.
- The `ide` directory contains auxiliary files that are useful if you want to develop Ziggy in the Eclipse IDE. 
- The `licenses` directory contains information about both the Ziggy license and the licenses of third-party applications and libraries that Ziggy uses. 
- The `src` directory contains the directory tree of Ziggy source code, both main classes and test classes. 
- The `test` directory contains test data for Ziggy's unit tests. 
- The `sample-pipeline` directory contains the source and such for the sample pipeline. 

### Building Ziggy

Before you build Ziggy, you'll need to set up 4 environment variables:

- The `JAVA_HOME` environment variable should contain the location of the Java Development Kit (JDK) you want to use for Java compilation.
- The `CC` environment variable should contain the location of the C compiler you want to use for C compilation.
- The `CXX` environment variable should contain the location of the C++ compiler you want to use for C++ compilation.
- When Ziggy goes to build its copy of the HDF5 libraries, for some reason the HDF5 build system doesn't always find the necessary include files for building the Java API. If this happens to you, create the environment variable JNIFLAGS: `export JNIFLAGS="-I<java-home>/include -I<java-home>/include/<os-specific>"`. Here `<java-home>`is the location of your JDK (so it should be identical to the contents of `JAVA_HOME`, above). The `<java-home>/include` directory will have an additional subdirectory of include files that are specific to a particular OS (for example, on my Macbook this is `$JAVA_HOME/include/darwin`). With a properly-set `JNIFLAGS` environment variable, you should be able to build HDF5 without difficulty (famous last words...).

Once you've got that all set up, do the following:

1. Open a terminal window.
2. Change directory to the Ziggy main directory (the one that looks like the figure above).
3. At the command line, type `./gradlew`.

The first time you do this, it will take a long time to run and there will be a lot of C and C++ compiling. This is because Gradle is building the HDF5 libraries, which can take a long time. Gradle will also download all the third party libraries and Jarfiles Ziggy relies upon. Barring the unforeseen, eventually you should see something like this in your terminal:

```console
ziggy$ ./gradlew
ar: creating archive ziggy/build/lib/libziggy.a
ar: creating archive ziggy/build/lib/libziggymi.a

> Task :compileJava 
Note: Some input files use or override a deprecated API.
Note: Recompile with -Xlint:deprecation for details.


BUILD SUCCESSFUL in 14s
19 actionable tasks: 19 executed
ziggy$ 
```

At this point, it's probably worthwhile to run Ziggy's unit tests to make sure nothing has gone wrong. To do this, at the command line type `./gradlew test`. The system will run through a large number of tests (around 700) over the course of about a minute, and hopefully you'll get another "BUILD SUCCESSFUL" message at the end. 

If you look at the Ziggy folder now, you'll see the following:

```console
ziggy$ ls
LICENSE.pdf     buildSrc    gradle.properties    sample-pipeline    test
README.md       doc         gradlew              script-plugins
build           etc         ide                  settings.gradle
build.gradle    gradle      licenses             src
ziggy$ 
```

Pretty much the same, except that now there's a `build` folder. What's in the `build` folder?

```console
build$ ls
bin        etc        lib     obj          schema    tmp
classes    include    libs    resources    src
build$ 
```

The main folders of interest here are:

- The `bin` folder, which has all the executables. 
- The `lib` folder, which has shared object libraries (i.e., compiled C++)
- The `libs` folder, which has Jarfiles (so why not name it "jars"? I don't know).
- The `etc` folder, which is a copy of the main `ziggy/etc` folder. 

Everything that Ziggy uses in execution comes from the subfolders of `build`. That's why there's a copy of `etc` in `build`: the `etc` in the main directory isn't used, the one in `build` is, so that everything that Ziggy needs is in one place. 

Before we move on, a few useful details about building Ziggy:

- Ziggy makes use of a number of dependencies. Most of them are Jarfiles, which means that they can be downloaded and used without further ado, but at least one, the [HDF5 library](https://www.hdfgroup.org/solutions/hdf5/), requires compilation via the C++ compiler. This is the most time-consuming part of the build.
- The first time you run `./gradlew`, the dependencies will be automatically downloaded. On subsequent builds with `./gradlew`, the dependencies will mostly not be downloaded, but instead cached copies will be used. This means that the subsequent uses are much faster. 
- Why "mostly not ... downloaded?" Well, the build system checks the dependencies to see whether any new versions have come out. New versions of the third party libraries are automatically downloaded in order to ensure that Ziggy remains up-to-date with security patches. So on any given invocation of `./gradlew`, there might be a library or two that gets updated. 
- Gradle has lots of commands (known in the lingo as "tasks") other than `test`. Most notably, the `./gradlew clean` command will delete the build directory so you can start the build over from the beginning. The `./gradlew build` command will first build the code and then run the unit tests. 

If all is going well, you're now ready to move on to [defining your own pipeline](configuring-pipeline.md)!
