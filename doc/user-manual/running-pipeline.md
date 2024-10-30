<!-- -*-visual-line-*- -->

[[Previous]](building-pipeline.md)
[[Up]](user-manual.md)
[[Next]](ziggy-gui.md)

## Running the Pipeline

At last, we are ready to start up the pipeline! First, though, we need to have a minor digression about the `ziggy` program.

### Ziggy

Running a piece of Java code should be simple: you use the `java` command at the command line, followed by the class name. Awesome!

Well, almost. In reality, there are often issues with the `java` command: specifying the correct class path, giving the fully-qualified name of the class (without typos), etc.

For this reason, Ziggy provides a program that inserts a layer of abstraction over the `java` command. This layer of abstraction allows the program to use the Ziggy configuration system (like the properties file) to specify the things that the user would otherwise need to put at the command line. It also allows for nicknames: rather than the fully-qualified names, shorter names are provided for some classes. It's like having a version of the `java` command that's purpose-built to work with Ziggy.

The name of this software is `ziggy`, and it's a Perl program that's in Ziggy's `build/bin` directory (the Ziggy one, not the sample pipeline one). The `ziggy` program is so useful that you should probably create an alias for it, or else add Ziggy's `build/bin` directory to your search path. Just so you don't have to type the whole path to `ziggy` every time you want to use it.

You can see the full set of nicknames by running the ziggy command with no arguments:

```console
$ ziggy
NICKNAME                 CLASS NAME
cluster                  gov.nasa.ziggy.ui.ClusterController
compute-node-master      gov.nasa.ziggy.module.ComputeNodeMaster
console                  gov.nasa.ziggy.ui.ZiggyConsole
dump-system-properties   gov.nasa.ziggy.services.config.DumpSystemProperties
execsql                  gov.nasa.ziggy.services.database.SqlRunner
export-parameters        gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli
export-pipelines         gov.nasa.ziggy.pipeline.definition.PipelineDefinitionCli
generate-build-info      gov.nasa.ziggy.util.BuildInfo
generate-manifest        gov.nasa.ziggy.data.management.Manifest
hsqlgui                  org.hsqldb.util.DatabaseManagerSwing
import-datastore-config  gov.nasa.ziggy.data.management.DatastoreConfigurationImporter
import-events            gov.nasa.ziggy.services.events.ZiggyEventHandlerDefinitionImporter
import-parameters        gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli
import-pipelines         gov.nasa.ziggy.pipeline.definition.PipelineDefinitionCli
metrics                  gov.nasa.ziggy.metrics.report.MetricsCli
perf-report              gov.nasa.ziggy.metrics.report.PerformanceReport
update-pipelines         gov.nasa.ziggy.pipeline.definition.PipelineDefinitionCli
$ 
```

You can view more help with `ziggy --help` and even more help with `perldoc ziggy`.

Since there are a lot of commands, sub-commands, and options, we've created a bash completions file for the `ziggy` program so you can press the `TAB` key while entering the `ziggy` program to display the available commands and options. If you want to use it, run `. $ZIGGY_ROOT/etc/ziggy.bash-completion`. That's a dot at the front; it's the same mechanism that you would use to re-read your `.bashrc` file.

If you should happen to write some Java to manage your pipeline and want to use the `ziggy` program to run it, please refer to the article on [Creating Ziggy Nicknames](nicknames.md).

### Ziggy Cluster Commands

For coarse, on-off control for the cluster, the fundamental `ziggy` command is `ziggy cluster`. As described [elsewhere](pipeline-architecture.md),  **cluster** is the word we use to refer to a particular instance of a pipeline, its data storage, etc.

To see the options for this command, do `ziggy cluster`with no additional arguments:

```console
$ ziggy cluster
usage: ClusterController [options] command...

Commands:
init                    Initialize the cluster
start                   Start the cluster
stop                    Stop the cluster
status                  Check cluster status
console                 Start pipeline console GUI
version                 Display the version (as a Git tag)

Options:
 -f,--force                  Force initialization if cluster is already
                             initialized
 -h,--help                   Show this help
    --workerCount <arg>      Number of worker threads or 0 to use all
                             cores
    --workerHeapSize <arg>   Total heap size used by all workers (MB)
$
```

The first thing you need to do is to initialize the cluster (hence the name).

#### Cluster Initialization

Cluster initialization is something you only do once per cluster. It populates the database with Ziggy's standard tables; sets up the datastore directories; and reads in the pipeline configuration from the XML files.

The command `ziggy cluster init` causes the initialization to occur. When you do this, after a few seconds you'll either have a nice spew of logging that ends with the message:

`Cluster initialized`

or else you'll see a Java stack trace that tells you that something went wrong. The most common problem that causes initialization to fail is a problem with the XML files that describe the pipeline, so that's the place to look first. Hopefully the exception provides informative messages that will help you track down the problem (they help me, but YMMV).

Once you've initialized the cluster, Ziggy will prevent you from doing so again:

```
$ ziggy cluster init
[ClusterController.usageAndExit] Failed to initialize cluster
gov.nasa.ziggy.module.PipelineException: Cannot re-initialize an initialized cluster without --force option
        at gov.nasa.ziggy.ui.ClusterController.initializeCluster(ClusterController.java:317) [ziggy-0.5.0.jar:?]
        at gov.nasa.ziggy.ui.ClusterController.main(ClusterController.java:277) [ziggy-0.5.0.jar:?]
$
```

Like it says: if you want to do this, you can do it; you need to use the `--force` (or `-f`) option.  Be aware, though: **if you reinitialize the cluster, you will delete all of the existing contents!** That's why Ziggy tries to keep you from doing it by accident. Reinitialize and all the database content, all the data files that have been generated as results -- all gone.

That said: if your cluster initialization fails because of a problem in the XML, you will need to use the `--force` option. This is because the database was successfully created with all its tables. Thus, even though you failed to put actual information in the tables, you'd need to use the `--force` option to try again.

##### What if I don't want to reinitialize?

If the failure was in the import of the contents of the pipeline-defining XML files, there's an alternative to using `ziggy cluster init`. Specifically, you can use other ziggy commands that import the XML files without performing initialization.

If you look at the list of ziggy nicknames in the top screen shot, there are 3 that will be helpful here: `import-parameters`, `import-datastore-config`, and `import-pipelines`. These do what they say: import the parameter library, data type definition, and pipeline definition files, respectively.

Important note: if you decide to manually import the XML files, **you must do so in the order shown above:** parameters, then data types, then the pipeline definitions. This is because some items can't import correctly unless other items that they depend upon have already been pulled in.

#### Cluster Start and Cluster Status

To start the cluster, you use -- wait for it -- `ziggy cluster start`. If you're using a private (non-system) database, this command starts it. The supervisor process is then started. The supervisor, as the name suggests, manages all of Ziggy's actual work: generates tasks, starts them running, monitors them, persists outputs, records instances of failures, and much, much more. It's an exaggeration to say that the supervisor **is** Ziggy, but not much of one.

Once you've done this, both the supervisor and the database software run as what Linux folks like to call "daemons". This means that they're running even though they gave you back the command prompt in the shell where you started them. To see this, use `ziggy cluster status`:

```console
Cluster is initialized
Supervisor is running
Database is available
Cluster is running
```

The status command shows that the cluster is running, which is great! But so far it's not obvious how you're supposed to do anything with it. To make use of the cluster, you'll need Ziggy's graphical user interface.

##### Workers and Heap Size

If you look at the command options, there are two options that are relevant to starting the cluster. These are `workerHeapSize` and `workerCount`.

The `workerHeapSize` is the total number of MB of Java "heap" (aka memory) that the supervisor is permitted to request for its workers.  If you ever encounter an error message complaining that a worker is out of memory or out of "GC Overhead" (i.e., additional RAM needed by the Java garbage collector), you can stop the cluster, then restart it with a larger heap size specified via this option.

Ziggy's supervisor can run and manage multiple worker processes, which means that the supervisor can marshal, persist, and execute multiple tasks simultaneously. The maximum number of tasks that can execute in parallel is set by the `workerCount` option. 

Note that these options are, well, optional. The pipeline properties file specifies a default value for the heap size and a default for the worker count. If you don't use these options at the command line, Ziggy will use the values in the properties file. For more information, see the article on [The Properties File](properties.md).

How should you decide how to set these values? Here are a few rough rules of thumb:

1. Your pipeline will have a maximum number of tasks that it will ever need to execute for a given node in the pipeline. This is determined by your design of the pipeline. In the sample pipeline, there are 2 datasets that get processed in parallel, `set-1` and `set-2`. Thus the sample pipeline can benefit from having 2 workers, but wouldn't be any faster if it had more.
2. The system that runs the supervisor will have a fixed number of CPUs ("cores"). As a general rule, there is no benefit to selecting a worker count that is larger than the number of CPUs; this will simply result in too many processes competing for too few CPUs.
3. The more processes you have, the larger the heap will need to be in order to support them. That said, you can probably get by with 1-2 GB per worker.
4. The supervisor always takes the total heap size and divides it equally among all the workers. Thus, if you know that each worker needs, say, 8 GB, and you specify 4 workers, you should set the heap size to 32 GB. 
5. When running locally, each algorithm process is managed by a worker process. This means that the total number of algorithm processes that run locally will never exceed the total number of workers. Thus, one reason to limit the number of worker processes is that you have algorithms that run locally and you don't want your local system to run out of cores and/or RAM when it's running them. Note that this doesn't apply to algorithms that are run remotely, on a High Performance Computing (HPC) or cloud computing system. In those cases, the worker hands execution over to a remote compute node for the algorithm and is then free to find some other work to do. For more information on this, see [the section on High Performance Computing](select-hpc.md). 

One last detail on this: the worker heap size controls the amount of RAM available to the worker processes, and thus the amount of RAM available for marshaling and persisting. It doesn't control the amount of RAM available to the algorithms. The reason for this is technical and probably not all that interesting, but the point is that even if you know that your algorithm needs 10 GB per task, you still can give each worker 1-2 GB per thread and everything will run just fine.

##### What if My Algorithms Need Different Numbers of Workers?

Excellent question! To answer it, we first need to understand why this sort of issue would arise in the first place. 

The most likely way for this to happen is if you have algorithms that have significantly different resource requirements, in particular RAM requirements. Imagine that you have a server with 256 GB of RAM, and on this you need to run an algorithm that needs 16 GB per process and another that needs 10 GB per process. This means that you can run 25 processes at a time for the algorithm that needs 10 GB each, but only 16 processes at a time for the one that needs 16 GB each. If you set the number of workers to 16, then you're not taking full advantage of your server's capacity when running the leaner algorithm (i.e., you're forcing it to take longer than it should); if you set it to 25, the more memory-intensive algorithm will run out of RAM and will start to use "virtual memory" (i.e., it will get extremely slow). 

If this is your situation, you're in luck, because Ziggy allows you to assign a different max worker value for each module in a pipeline. To see how this is done, take a look at [the article on the Edit Pipeline Dialog](edit-pipeline.md). If you decide to use this per-module worker setting, then the value you set via the methods described above acts as a default: it's used by any pipeline module that doesn't have its own max worker setting.

#### Cluster console

To get the console to show up use the command `ziggy cluster console` or simply `ziggy console`. After a few seconds and a few lines of logging, you'll see this appear:

<img src="images/gui.png" style="width:32cm;"/>

Now you're ready to do some stuff!

##### Ziggy cluster start console &

Personally, I find that when I start the pipeline I generally want a console window immediately. For this reason, you can put the two commands together: `ziggy cluster start console & `. That final `&` sign causes the command to run in the background so you get your command prompt back in the window where you issued the command. So that you don't go around filling your display with terminal windows that are just there to keep the console running.

##### Important Note: The Cluster and the Console

It is important to remember that the console is not the cluster! Most importantly, when you close a console window, the cluster continues to run even though the console is gone (so you have no way to control it, but it's still doing whatever you set it working on before you closed the console). To stop the cluster, you use:

#### Cluster stop

When you're done with the cluster and want to shut it down, the command is `ziggy cluster stop`. This will stop the worker and stop the database (for a non-system database). It will also cause any console windows to shut down.

That said: don't do it now! Because you'll want the cluster running and the console up when we ...

### What's Next?

... [use the console](ziggy-gui.md) to do some cool stuff!

[[Previous]](building-pipeline.md)
[[Up]](user-manual.md)
[[Next]](ziggy-gui.md)
