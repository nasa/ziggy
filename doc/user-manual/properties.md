<!-- -*-visual-line-*- -->

[[Previous]](task-configuration.md)
[[Up]](intermediate-topics.md)
[[Next]](troubleshooting.md)

## The Properties File

When we discussed [configuring the pipeline](configuring-pipeline.md), we mentioned the properties file, which provides Ziggy with all of its global configuration. Let's get into detail on this now.

### Syntax

The properties file is a flat text file that consists of name-value pairs separated by an equals sign (=). Both the name and the value are text strings.

#### Property Expansion

Properties can be defined in terms of the values of other properties by use of the `${...}` syntax. For example, in the [sample.properties](../../sample-pipeline/etc/sample.properties) file, we see these definition:

```
pipeline.results.dir = ${pipeline.home.dir}/pipeline-results
```

When Ziggy needs the value of `pipeline.results.dir`, it first finds the value of `build.dir`, and then appends `/pipeline-results` onto the end of that string. Note that for some reason this is sometimes referred to as "interpolation."

#### Use of Environment Variables

The properties manager can also reach out to the environment and pull in the value of an environment variable for use in a property. This is done with the `${env:...}` syntax. For example, the sample properties file defines the `ziggy.root` property in terms of the `ZIGGY_ROOT` environment variable:

```
hibernate.connection.username = ${env:USER}
```

### Pipeline Properties vs Ziggy Properties

Ziggy actually uses two properties files.

The pipeline properties file contains the properties that you, the pipeline user, are more likely to want to edit. This is the file that `PIPELINE_CONFIG_PATH` needs to point to.

The other properties file is the Ziggy properties file, which is stored with the main Ziggy code, at `etc/ziggy.properties`. These are properties that you, the pipeline user, are unlikely to ever want to mess with. Ziggy finds the latter via -- yeah, you got it -- a property in the former, specifically `ziggy.config.path`.

In a real, normal pipeline, our recommendation is to put the pipeline properties file someplace outside of the directories that are under version control. This way you can modify them to your heart's content without fear that you'll accidentally commit your changes back to the Ziggy repository. The Ziggy properties file can stay in the version-controlled directories unless you decide to modify it, in which case it, too, needs to be copied out to avoid corrupting the repository.

### The Properties

Without further ado, here's the list of properties that Ziggy gets from the properties files. Note that these are the properties that Ziggy itself uses. You can define any other properties you like, and define the ones in this section in terms of the new ones you've defined, if that makes your properties file easier to use or maintain.

#### General Path-Setting

| Property Name           | Description                                                  | File     |
| ----------------------- | ------------------------------------------------------------ | -------- |
| pipeline.home.dir       | Top-level directory for the pipeline code.                   | Pipeline |
| pipeline.results.dir    | Location for working directories, log files, etc.            | Pipeline |
| pipeline.definition.dir | Location for XML files that define the pipeline              | Pipeline |
| pipeline.classpath      | Java classpath for pipeline-side Java classes                | Pipeline |
| pipeline.logoFile       | Location and name of the image logo file to be used as an icon (supported formats: PNG, JPEG, BMP, GIF, WBMP) | Pipeline |
| data.receipt.dir        | Directory used by data receipt                               | Pipeline |
| database.dir            | Directory used by RDBMS; if empty, the system database is used | Pipeline |
| datastore.root.dir      | Root directory for datastore                                 | Pipeline |
| java.home               | Location of Java to be used by runjava (see the article on [running the cluster](running-pipeline.md) for more information on runjava) | Pipeline |

#### Paths to Ziggy

| Property Name     | Description                                 | File     |
| ----------------- | ------------------------------------------- | -------- |
| ziggy.home.dir    | Location of the `build` directory for Ziggy | Pipeline |
| ziggy.config.path | Location of Ziggy's configuration file      | Pipeline |

#### Algorithm Module Paths

In order to avoid conflicts with the rest of the user's environment configuration, when Ziggy runs an algorithm module it creates an environment in which the executable and library paths are replaced by values specified in the pipeline configuration file. This means that the user isn't obligated to put any pipeline-related paths into their `PATH` or `LD_LIBRARY_PATH` environment variables.

| Property Name               | Description                                                  | File     |
| --------------------------- | ------------------------------------------------------------ | -------- |
| pi.worker.moduleExe.binPath | Colon-separated list of directories to search for algorithm executables | Pipeline |
| pi.worker.moduleExe.libPath | Colon-separated list of directories to search for shared libraries (i.e., files with .so or .dylib suffix) | Pipeline |

#### Algorithm Environment

| Property Name            | Description                                                  | File     |
| ------------------------ | ------------------------------------------------------------ | -------- |
| pi.moduleExe.environment | Comma-separated list of name-value pairs of environment variables that should be provided to the algorithm at runtime. | Pipeline |

#### Database Properties

| Property Name          | Description                                                  | File     |
| ---------------------- | ------------------------------------------------------------ | -------- |
| database.software.name | The flavor of database in use, also used in the database URI. Currently, only postgresql is supported. | Pipeline |
| database.name          | Name of the database (you're going to be stuck with this forever once you pick it, so choose wisely) | Pipeline |
| database.port          | Connection port for RDBMS. Each cluster on a given system must have its own database port. More generally, each cluster must have a port that is not already in use on the system, so you need to avoid trying to use a port that some other joker has already taken. | Pipeline |
| database.host          | Hostname for RDBMS (usually localhost)                       | Ziggy    |
| database.connections   | Number of connections database will accept. Might not be used  if the database is a system database. | Pipeline |

#### Worker Resources

| Property Name         | Description                                                  | File     |
| --------------------- | ------------------------------------------------------------ | -------- |
| rmi.registry.port     | Port used for connections between worker and UI. Same conditions as for the database port (i.e., each cluster must have a port that's unique and not in use by some other joker). | Pipeline |
| pi.worker.heapSize    | Size of Java heap for worker process, in MB (can be overridden by the `--workerHeapSize` option in `runjava cluster start`) | Pipeline |
| pi.worker.threadCount | Number of worker threads (can be overridden by the `--workerThreadCount` option in `runjava cluster start`); set to zero to have 1 thread per CPU "core" | Pipeline |

#### Remote Execution Properties

| Property Name       | Description                                                  | File     |
| ------------------- | ------------------------------------------------------------ | -------- |
| remote.user         | Username to be used when submitting jobs to batch system     | Pipeline |
| remote.group        | Group ID to be used when submitting jobs to batch system     | Pipeline |
| remote.cluster.name | Flavor of remote system used. Supported values are "NAS" (i.e., the HPC facility at NASA Ames Research Center), "AWS" (i.e., Amazon Web Services). | Pipeline |

#### Behavior Properties

| Property Name                   | Description                                                  | File     |
| ------------------------------- | ------------------------------------------------------------ | -------- |
| moduleExe.useSymlinks           | Use symbolic links rather than copies when staging files to working directory | Pipeline |
| moduleExe.memdrone.enabled      | Enable/disable memory consumption tracker                    | Pipeline |
| moduleExe.memdrone.sleepSeconds | Sample interval for memory consumption tracker               | Pipeline |
| pi.processing.halt.step         | Automatically halt pipeline after a given processing step (marshaling, submitting, etc.). Mainly for debugging. See the article on [The Instances Panel](instances-panel.md) for more about processing steps. | Pipeline |

#### Hibernate Properties

These properties control the behavior of Hibernate, the API that links Ziggy to the database.

| Property Name                       | Description                                                  | File     |
| ----------------------------------- | ------------------------------------------------------------ | -------- |
| hibernate.connection.username       | Username for use when connecting to database                 | Pipeline |
| hibernate.show_sql                  | Write generated queries to log file                          | Ziggy    |
| hibernate.format_sql                | "Pretty-print" generated queries written to log file         | Ziggy    |
| hibernate.use_sql_comments          | Generate comments in generated queries                       | Ziggy    |
| hibernate.jdbc.batch_size           | Size of batches used in updates                              | Ziggy    |
| hibernate.id.new_generator_mappings | Allow sequence generators to have initial values             | Ziggy    |
| hibernate.connection.url            | URL for database connections (default: jdbc:${database.software.name}://${database.host}:${database.port}/${database.name})                                 | Ziggy    |
| hibernate.connection.password       | Password for database connections                            | Ziggy    |
| hibernate.controller.driver_class   | Java class used by Hibernate to manage connections. Only needed if using a database application not supported by Ziggy. | Ziggy    |
| hibernate.dialect                   | Database dialect used by Hibernate. Only needed if using a database application not supported by Ziggy. | Ziggy    |

[[Previous]](task-configuration.md)
[[Up]](intermediate-topics.md)
[[Next]](troubleshooting.md)
