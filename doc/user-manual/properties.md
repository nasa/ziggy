<!-- -*-visual-line-*- -->

[[Previous]](contact-us.md)
[[Up]](user-manual.md)

## Appendix A. The Properties File

When we discussed [configuring the pipeline](configuring-pipeline.md), we mentioned the properties file, which provides Ziggy with all of its global configuration. Let's get into detail on this now.

### Syntax

The properties file is a flat text file that consists of name-value pairs separated by an equals sign (=). Both the name and the value are text strings.

#### Property Expansion

Properties can be defined in terms of the values of other properties by use of the `${...}` syntax. For example, in the [sample.properties](../../sample-pipeline/etc/sample.properties) file, we see these definition:

```
ziggy.pipeline.results.dir = ${ziggy.pipeline.home.dir}/pipeline-results
```

When Ziggy needs the value of `ziggy.pipeline.results.dir`, it first finds the value of `build.dir`, and then appends `/pipeline-results` onto the end of that string. Note that for some reason this is sometimes referred to as "interpolation."

#### Use of Environment Variables

The properties manager can also reach out to the environment and pull in the value of an environment variable for use in a property. This is done with the `${env:...}` syntax. For example, the sample properties file defines the `ziggy.root` property in terms of the `ZIGGY_ROOT` environment variable:

```
hibernate.connection.username = ${env:USER}
```

See also the `ziggy.pipeline.environment` property.

### Pipeline Properties vs Ziggy Properties

Ziggy actually uses two properties files.

The pipeline properties file contains the properties that you, the pipeline user, are more likely to want to edit. This is the file that `PIPELINE_CONFIG_PATH` needs to point to. You likely seeded your file with the copy in `ZIGGY_ROOT/etc/pipeline.properties.EXAMPLE`.

The other properties file is the Ziggy properties file, which is stored with the main Ziggy code, at `etc/ziggy.properties`. These are properties that you, the pipeline user, are unlikely to ever want to mess with.

In a real, normal pipeline, our recommendation is to put the pipeline properties file someplace outside of the directories that are under version control. This way you can modify them to your heart's content without fear that you'll accidentally commit your changes back to the Ziggy repository. The Ziggy properties file can stay in the version-controlled directories unless you decide to modify it, in which case it, too, needs to be copied out to avoid corrupting the repository.

### The Properties

Without further ado, here's the list of properties that Ziggy gets from the properties files. Note that these are the properties that Ziggy itself uses. You can define any other properties you like, and define the ones in this section in terms of the new ones you've defined, if that makes your properties file easier to use or maintain.

The default value is either defined by code or by `ziggy.properties`. If the default value is None, you must define the property in your `PIPELINE_CONFIG_PATH` file.

| Name | Description | Default |
| ---- | ----------- | -------- |
| hibernate.connection.driver_class | Java class used by Hibernate to manage connections. Only needed if using a database application not supported by Ziggy. | Supported database default |
| hibernate.connection.password | Password for database connections | None |
| hibernate.connection.url | URL for database connections | jdbc:${ziggy.database.software.name}:${ziggy.database.protocol}//${ziggy.database.host}:${ziggy.database.port}/${ziggy.database.name} |
| hibernate.connection.username | Username for use when connecting to database | None |
| hibernate.dialect | Database dialect used by Hibernate. Only needed if using a database application not supported by Ziggy. | Supported database default |
| hibernate.format_sql | "Pretty-print" generated queries written to log file | true |
| hibernate.jdbc.batch_size | Size of batches used in updates | 30 |
| hibernate.show_sql | Write generated queries to log file | false |
| hibernate.use_sql_comments | Generate comments in generated queries | false |
| java.home | Location of Java used by the ziggy program to override the Java on your search path (see the article on [running the cluster](running-pipeline.md) for more information on the ziggy program) | $JAVA_HOME |
| java.rmi.server.hostname | Hostname of RMI server | localhost |
| ziggy.database.bin.dir | Location of the RDBMS executables | $PATH |
| ziggy.database.conf.file | Location of the database configuration file; not used with a system database | "" |
| ziggy.database.connections | Number of connections database will accept; not used with a system database | None |
| ziggy.database.dir | Directory used by RDBMS; if empty, the system database is used | "" |
| ziggy.database.host | Hostname for RDBMS (usually localhost) | None |
| ziggy.database.name | Name of the database (you're going to be stuck with this forever once you pick it, so choose wisely) | None |
| ziggy.database.port | Connection port for RDBMS. Each cluster on a given system must have its own database port. More generally, each cluster must have a port that is not already in use on the system, so you need to avoid trying to use a port that some other joker has already taken. | None |
| ziggy.database.protocol | Protocol for RDBMS if applicable plus any punctuation used before the // in the URL such as colon (:) as in `hsql:`. | "" |
| ziggy.database.software.name | The flavor of database in use (postgresql, hsqldb). | "" |
| ziggy.home.dir | Location of the `build` directory for Ziggy | None |
| ziggy.logoFile | Location and name of the image logo file to be used as an icon (supported formats: PNG, JPEG, BMP, GIF, WBMP) | "" |
| ziggy.pipeline.binPath | Colon-separated list of directories to search for algorithm executables (PATH is ignored by Ziggy) | ${ziggy.pipeline.home.dir}/bin |
| ziggy.pipeline.classpath | Java classpath for pipeline-side Java classes | "" |
| ziggy.pipeline.data.importer.classname | Implementation class of DataImporter used by data receipt | gov.nasa.ziggy.data.management.DefaultDataImporter |
| ziggy.pipeline.data.receipt.dir | Directory used by data receipt | None |
| ziggy.pipeline.data.receipt.validation.maxFailurePercentage | Maximum percentage of files that can fail validation before DR throws an exception | 100 |
| ziggy.pipeline.datastore.dir | Root directory for datastore | None |
| ziggy.pipeline.definition.dir | Location for XML files that define the pipeline | None |
| ziggy.pipeline.environment | Comma-separated list of name-value pairs of environment variables that should be provided to the algorithm at runtime. Note that whitespace around the commands is not allowed. | "" |
| ziggy.pipeline.home.dir | Top-level directory for the pipeline code. | None |
| ziggy.pipeline.libPath | Colon-separated list of directories to search for shared libraries such as files with .so or .dylib suffix (LD_LIBRARY_PATH is ignored by Ziggy) | "" |
| ziggy.pipeline.mcrRoot | Location of the MATLAB Compiler Runtime (MCR), including the version, if MATLAB algorithm executables are used | "" |
| ziggy.pipeline.memdrone.enabled | Enable/disable memory consumption tracker | false |
| ziggy.pipeline.memdrone.sleepSeconds | Sample interval for memory consumption tracker | 60 |
| ziggy.pipeline.processing.halt.step | Automatically halt pipeline after a given processing step (marshaling, submitting, etc.). Mainly for debugging. See the article on [The Instances Panel](instances-panel.md) for more about processing steps. | complete |
| ziggy.pipeline.results.dir | Location for working directories, log files, etc. | None |
| ziggy.pipeline.uow.defaultIdentifier.classname | Class used to identify default UOWs that are defined in the pipeline (not Ziggy) | DatastoreDirectoryUnitOfWorkGenerator.class or DataReceiptUnitOfWorkGenerator.class as appropriate |
| ziggy.pipeline.useSymlinks | Use symbolic links rather than copies when staging files to working directory | false |
| ziggy.remote.cluster.name | Flavor of remote system used. Supported values are "NAS" (i.e., the HPC facility at NASA Ames Research Center), "AWS" (i.e., Amazon Web Services). | NAS |
| ziggy.remote.group | Group ID to be used when submitting jobs to batch system | "" |
| ziggy.remote.host | Colon-separated list of remote host names, in order from most- to least-desired | "" |
| ziggy.remote.nasa.directorate | NASA directorate to be used for calculating likely NAS queue wait times | SMD |
| ziggy.remote.queuecommand.classname | Implementation class of QueueCommandManager for use by Ziggy | gov.nasa.ziggy.module.remote.QueueLocalCommandManager |
| ziggy.remote.user | Username to be used when submitting jobs to batch system | "" |
| ziggy.test.working.dir | Allows the user to specify a working directory other than user.dir. For testing only. | ${user.dir} |
| ziggy.worker.allowPartialTasks | Allow persisting to continue although one or more subtasks failed | true |
| ziggy.worker.heapSize | Maximum cumulative size of the Java heap for all worker processes, in MB (can be overridden by the `--workerHeapSize` option in `ziggy cluster start`) | 16,000 |
| ziggy.supervisor.heartbeat.interval.millis | Interval between messages from the supervisor to RMI clients to ensure that connections remain intact | 15,000 |
| ziggy.supervisor.port | Port used for connections between supervisor, worker, and UI. Same conditions as for the database port (i.e., each cluster must have a port that's unique and not in use by some other joker). | 1099 |
| ziggy.worker.count | Maximum number of workers (can be overridden by the `--workerCount` option in `ziggy cluster start`); set to zero to have 1 worker per CPU "core" | 1 |

[[Previous]](contact-us.md)
[[Up]](user-manual.md)
