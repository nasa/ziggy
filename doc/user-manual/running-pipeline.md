<a href="rdbms.md">[Previous]</a> <a href="user-manual.md">[Up]</a> <a href="ziggy-gui.md">[Next]</a>

## Running the Cluster

At last, we are ready to start up the pipeline! First, though, we need to have a minor digression about the `runjava` program.

### Runjava

Running a piece of Java code should be simple: you use the `java` command at the command line, followed by the class name. Awesome!

Well, almost. In reality, there are often issues with the `java` command: specifying the correct class path, giving the fully-qualified name of the class (without typos), etc. 

For this reason, Ziggy provides a program that inserts a layer of abstraction over the `java` command. This layer of abstraction allows the program to use the Ziggy configuration system (like the properties file) to specify the things that the user would otherwise need to put at the command line. It also allows for nicknames: rather than the fully-qualified names, shorter names are provided for some classes. It's like having a version of the `java` command that's purpose-built to work with Ziggy.

The name of this software is `runjava`, and it's a Perl program that's in Ziggy's `build/bin` directory (the Ziggy one, not the sample pipeline one). The `runjava` program is so useful that you should probably create an alias for it, or else add Ziggy's `build/bin` directory to your search path. Just so you don't have to type the whole path to `runjava` every time you want to use it. 

You can see the full set of nicknames by running the runjava command with no arguments:

```console
$ runjava
              NICKNAME      CLASSNAME
               cluster      gov.nasa.ziggy.ui.ClusterController
   compute-node-master      gov.nasa.ziggy.module.ComputeNodeMaster
               console      gov.nasa.ziggy.ui.ZiggyConsole
              dump-err      gov.nasa.ziggy.module.io.DumpMatlabErrCli
            dump-props      gov.nasa.ziggy.services.configuration.DumpSystemProperties
dump-system-properties      gov.nasa.ziggy.services.config.DumpSystemProperties
               execsql      gov.nasa.ziggy.dbservice.SqlRunner
     generate-manifest      gov.nasa.ziggy.data.management.Manifest
               metrics      gov.nasa.ziggy.metrics.report.MetricsCli
             pd-export      gov.nasa.ziggy.pipeline.definition.PipelineDefinitionCli
             pd-import      gov.nasa.ziggy.pipeline.definition.PipelineDefinitionCli
             pe-import      gov.nasa.ziggy.services.events.ZiggyEventHandlerDefinitionFileImporter
           perf-report      gov.nasa.ziggy.metrics.report.PerformanceReport
             pl-export      gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli
             pl-import      gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli
           pl-override      gov.nasa.ziggy.parameters.ParameterLibraryOverrideCli
             pt-import      gov.nasa.ziggy.data.management.DataFileTypeImporter
         seed-security      gov.nasa.ziggy.services.security.SecuritySeedData
          spoc-version      gov.nasa.ziggy.common.version.ZiggyVersionCli
       status-listener      gov.nasa.ziggy.services.process.StatusMessageLogger
$ 
```

### Runjava Cluster Commands

For coarse, on-off control for the cluster, the fundamental `runjava` command is `runjava cluster`. As described [elsewhere](pipeline-architecture.md),  **cluster** is the word we use to refer to a particular instance of a pipeline, its data storage, etc. 

To see the options for this command, do `runjava cluster`with no additional arguments:

```console
$ runjava cluster
usage: ClusterController [options] command...

Commands:
init                    Initialize the cluster
start                   Start the cluster
stop                    Stop the cluster
status                  Check cluster status
console                 Start pipeline console GUI

Options:
 -f,--force                     Force initialization if cluster is already
                                initialized
    --workerHeapSize <arg>      Total heap size used by all workers (MB)
    --workerThreadCount <arg>   Number of worker threads or 0 to use all
                                cores
$ 
```

The first thing you need to do is to initialize the cluster (hence the name).

#### Cluster Initialization

Cluster initialization is something you only do once per cluster. It populates the database with Ziggy's standard tables; sets up the datastore directories; and reads in the pipeline configuration from the XML files. 

The command `runjava cluster init` causes the initialization to occur. When you do this, after a few seconds you'll either have a nice spew of logging that ends with the message: 

`[ClusterController.initializeCluster] INIT: database initialization and creation complete`

or else you'll see a Java stack trace that tells you that something went wrong. The most common problem that causes initialization to fail is a problem with the XML files that describe the pipeline, so that's the place to look first. Hopefully the exception provides informative messages that will help you track down the problem (they help me, but YMMV).

Once you've initialized the cluster, Ziggy will prevent you from doing so again:

```
system:sample-pipeline user$ runjava cluster init
[ZiggyConfiguration.getConfiguration] Loading configuration from: /xxx/yyy/zzz/ziggy/sample-pipeline/etc/sample.properties
[ZiggyConfiguration.getConfiguration] Loading configuration from: /xxx/yyy/zzz/ziggy/sample-pipeline/etc/../../build/etc/ziggy.properties
Cannot re-initialize an initialized cluster without --force option
system3465:sample-pipeline user$ 
```

Like it says: if you want to do this, you can do it; you need to use the `--force` (or `-f`) option.  Be aware, though: **if you reinitialize the cluster, you will delete all of the existing contents!** That's why Ziggy tries to keep you from doing it by accident. Reinitialize and all the database content, all the data files that have been generated as results -- all gone. 

That said: if your cluster initialization fails because of a problem in the XML, you will need to use the `--force` option. This is because the database was successfully created with all its tables. Thus, even though you failed to put actual information in the tables, you'd need to use the `--force` option to try again.

##### What if I don't want to reinitialize?

If the failure was in the import of the contents of the pipeline-defining XML files, there's an alternative to using `runjava cluster init`. Specifically, you can use other runjava commands that import the XML files without performing initialization.

If you look at the list of runjava nicknames in the top screen shot, there are 3 that will be helpful here: `pl-import`, `pt-import`, and `pd-import`. These do what they say: import the parameter library, data type definition, and pipeline definition files, respectively. 

Important note: if you decide to manually import the XML files, **you must do so in the order shown above:** parameters, then data types, then the pipeline definitions. This is because some items can't import correctly unless other items that they depend upon have already been pulled in. 

#### Cluster Start and Cluster Status

To start the cluster, you use -- wait for it -- `runjava cluster start`. If you're using a private (non-system) database, this command starts it. The worker process is then started. The worker, as the name suggests, does all of Ziggy's actual work: generates tasks, starts them running, monitors them, persists outputs, records instances of failures, and much, much more. It's an exaggeration to say that the worker **is** Ziggy, but not much of one. 

Once you've done this, both the worker and the database software run as what Linux folks like to call "daemons". This means that they're running even though they gave you back the command prompt in the shell where you started them. To see this, use runjava cluster status:

```console
[ZiggyConfiguration.getConfiguration] Loading configuration from: /xxx/yyy/zzz/ziggy/sample-pipeline/etc/../../build/etc/ziggy.properties
[ClusterController.status] Cluster initialized
[ClusterController.status] Worker running
[ClusterController.status] Database running
[ClusterController.status] Cluster running
sample-pipeline$ 
```

The status command shows that the cluster is running, which is great! But so far it's not obvious how you're supposed to do anything with it. To make use of the cluster, you'll need Ziggy's graphical user interface.

##### Threads and Heap Size

If you look at the command options, there are two options that are relevant to starting the cluster. These are `workerHeapSize` and `workerThreadCount`. 

The `workerHeapSize` is the number of MB that the worker process is allowed to use. If you ever encounter an error message complaining that the worker is out of memory or out of "GC Overhead" (i.e., additional RAM needed by the Java garbage collector), you can stop the cluster, then restart it with a larger heap size specified via this option.

Ziggy's worker is a multi-threaded application. This means that the worker can marshal, persist, and execute multiple tasks simultaneously. The maximum number of tasks that can execute in parallel is set by the `workerThreadCount` option. 

Note that these options are, well, optional. The pipeline properties file specifies a default value for the heap space and a default for the thread count. If you don't use these options at the command line, Ziggy will use the values in the properties file. For more information, see the article on [The Properties File](properties.md).

How should you decide how to set these values? Here are a few rough rules of thumb:

1. Your pipeline will have a maximum number of tasks that it will ever need to execute for a given node in the pipeline. This is determined by your design of the pipeline. In the sample pipeline, there are 2 datasets that get processed in parallel, `set-1` and `set-2`. Thus the sample pipeline can benefit from having 2 worker threads, but wouldn't be any faster if it had more. 
2. The system that runs the worker process will have a fixed number of CPUs ("cores"). As a general rule, there is no benefit to selecting a worker thread count that is larger than the number of CPUs; this will simply result in too many threads competing for too few CPUs. 
3. The more threads you have, the larger the heap will need to be in order to support them. That said, you can probably get by with 1-2 GB per worker thread. 

One last detail on this: the worker heap size controls the amount of RAM available to the worker threads, and thus the amount of RAM available for marshaling and persisting. It doesn't control the amount of RAM available to the algorithms. The reason for this is technical and probably not all that interesting, but the point is that even if you know that your algorithm needs 10 GB per task, you still can give the worker 1-2 GB per thread and everything will run just fine. 

#### Cluster console

To get the console to show up use the command `runjava cluster console`. After a few seconds and a few lines of logging, you'll see this appear:

![](images/gui.png)

Now you're ready to do some stuff!

##### Runjava cluster start console &

Personally, I find that when I start the pipeline I generally want a console window immediately. For this reason, you can put the two commands together: `runjava cluster start console & `. That final `&` sign causes the command to run in the background so you get your command prompt back in the window where you issued the command. So that you don't go around filling your display with terminal windows that are just there to keep the console running.

##### Important Note: The Cluster and the Console

It is important to remember that the console is not the cluster! Most importantly, when you close a console window, the cluster continues to run even though the console is gone (so you have no way to control it, but it's still doing whatever you set it working on before you closed the console). To stop the cluster, you use:

#### Cluster stop

When you're done with the cluster and want to shut it down, the command is `runjava cluster stop`. This will stop the worker and stop the database (for a non-system database). It will also cause any console windows to shut down.

That said: don't do it now! Because you'll want the cluster running and the console up when we ...

### What's Next?

... [use the console](ziggy-gui.md) to do some cool stuff!

<a href="rdbms.md">[Previous]</a> <a href="user-manual.md">[Up]</a> <a href="ziggy-gui.md">[Next]</a>
