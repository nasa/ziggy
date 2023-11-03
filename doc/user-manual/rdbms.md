<!-- -*-visual-line-*- -->

[[Previous]](task-configuration.md)
[[Up]](user-manual.md)
[[Next]](troubleshooting.md)

## Setting up a Relational Database Management System (RDBMS)

Ziggy uses commercial off-the-shelf (COTS) database applications to keep track of a lot of the details of pipeline operations and generally to provide permanent records of lots and lots of information. For this purpose, pretty much any RDBMS will suffice, but Ziggy "ships" with support for HSQLDB and PostgreSQL.

### HSQLDB

The sample pipeline properties file is configured to use the included HSQLDB database so you can be productive immediately. HSQLDB is an in-memory database, which is both a benefit as it makes it fast, and a limitation, as it limits the size of your database<sup>1</sup>.

### PostgreSQL

As your system grows, you may consider switching to PostgreSQL, which is free and open-source, has great support and documentation, and performs well.

You may already have a PostgreSQL database that is set up and maintained by your system administrator (the "system database"). If not, you'll need to download and install PostgreSQL yourself. The following sections walk you through these two options.

#### Your own private PostgreSQL

This is in many ways the easiest solution in that it doesn't require a sysadmin to set anything up for you, and it's integrated into the rest of Ziggy's systems.

Anyway, the steps look like this:

##### Install PostgreSQL

On a Macbook you can use Homebrew or Macports to install PostgreSQL in your home directory, assuming you have the permissions for that. On Linux you can use the system's package manager if you have permissions for that. Users of any OS can go to [the PostgreSQL website](https://www.postgresql.org/download/) and download executables or source code. We've currently developing with the older PostgreSQL 12, but testing indicates that any version up to 14 will work. Not surprising, as our demands on the database are relatively simple.

##### Put the PostgreSQL Executables on your Search Path

In other words, "make sure that the postgresql bin directory is part of your PATH environment variable." Alternately, you can fill in `ziggy.database.bin.dir` in [the properties file](properties.md).

##### Update the Properties File

If you're on a system where other people are using PostgreSQL for Ziggy or other programs, you'll need to change the property `ziggy.database.port` in your properties file and set it to a free port. Try the `ss -tulw` command to see what ports are not free. Otherwise, you can use the value in pipeline.properties.EXAMPLE, which is the default PostgreSQL port (5432).

#### System PostgreSQL

This case is a bit more involved for the database and/or system administrators. On the other hand, it's less work for the user in that it delegates installation, care and feeding of the database application to the system administrators. Anyway:

##### Create the Database

The system database is a system-level instance of PostgreSQL that can run multiple relational databases simultaneously. This means that PostgreSQL needs to be installed by your system administrator, if it isn't already installed, and your Ziggy database needs to be created for you by the database administrator (DBA).

##### Update the Properties File

In order to use the system PostgreSQL executable, you'll need to set some values in [the properties file](properties.md). Specifically:

- The property `ziggy.database.port` needs to be set to whatever port the system database uses for connections. The value in `pipeline.properties.EXAMPLE` is the default PostgreSQL port (5432), so you might not need to change it.
- The property `ziggy.database.dir` needs to be removed. This tells Ziggy where it should put the files that are the content of the database, and in this case you don't need that because Ziggy isn't making that decision, your sysadmin is.
- The property `ziggy.database.conf.file` needs to be removed. This tells PostgreSQL how to configure itself; again, in this case, your sysadmin has made all those decisions for you.
- Other database properties need to be set according to instructions from your system administrator.

##### One Last Thing

If Ziggy's database runs in the context of the system's RDBMS, then Ziggy won't attempt to start or stop the database when it starts and stops the rest of its systems (again, see [the article on running the cluster](running-pipeline.md)). In essence, the database is "running" all the time.

However, this also means that if the database goes down (for example when the system is shut down), it may need a system administrator to bring it back to life.

### Some Other RDBMS

This is the most complex option to use, and I recommend that you not go this route right from the start. But if your favorite RDBMS isn't HSQLDB or PostgreSQL, read on.

The best way to accomplish this is to contact the Ziggy team and ask us whether we'd be willing and able to add support for another RDBMS application.

The main reason we only support HSQLDB and PostgreSQL is that we haven't had any requests yet to add another RDBMS, and in the absence of such a request we wouldn't know which ones to add and don't want to needlessly make work for ourselves. That said, in principle Ziggy is written in a way that should make it straightforward to add support for additional databases. If somebody actually wants to use one, then by definition it's not needless work, so we'll do it if we can!

In the event of impatience or bravery on your part, you can make a different RDBMS work for you without any help from us. Here's what you need to do:

#### Create the Database

Either you (in the case of a private database) or your DBA (in the case of a system database) has to create a database for your use.

#### Get the Appropriate Database JAR File

A database JAR file is the glue that connects Ziggy's Java code to the database application. Ziggy downloads the JAR files for PostgreSQL and HSQLDB, but no other RDBMS applications. For other applications, you'll need to get the JAR file.

In order to tell Ziggy where to find the JAR file, you'll want to put the file system path to the JAR file into the `ziggy.pipeline.classpath` property of the properties file. Alternately, you could put it into `ziggy/build/libs`, but if you do that then whenever you run `./gradlew clean`, the JAR file will get deleted and won't get replaced.

This JAR file will contain the database driver class definition; you'll need that classname for the next couple of steps.

#### Generate The Database Schema

Ziggy generates the schema for its database tables in PostgreSQL and HSQLDB format. You can generate the tables for some other database using a utility that Ziggy provides:

```console
$ ziggy -Dhibernate.connection.driver_class=<classname> -Dhibernate.dialect=<dialect> gov.nasa.ziggy.services.database.ZiggySchemaExport --create --output=<output-file>
```

You'll need to substitute in the name of the driver class for `<classname>`, the name of the dialect for `<dialect>` , and the output file name and location for `<output-file>`. For the dialect, use the fully-qualified name of one of the classes in the `org.hibernate.dialect` package as a string.

#### Create the Database Tables

Using the files created in the previous step, create the necessary tables in the database. There will be some command you can use to read in the files and generate the necessary tables and cross-references.

#### Update the Properties File

You'll need to make several changes:

- The property `ziggy.database.port` needs to be set to whatever port the database uses for connections.
- The property `database.software.name` must be removed.
- The properties `hibernate.dialect` and `hibernate.connection.driver_class` need to be set to the values you used a couple steps ago when generating the schema.

#### Populate the Database Tables

This amounts to manually importing into the database the XML files that define the pipeline, parameters, and data types. Fortunately, there are ziggy commands you can use for all of these actions:

- The command `ziggy import-parameters` allows you to read in the parameter library files.
- The command `ziggy import-types` allows you to read in the data file type definitions.
- The command `ziggy import-pipelines` allows you to read in the pipeline definitions.

All of the commands listed above will allow you to get help to know the exact syntax, order of arguments, etc. For more information on the ziggy program, take a look at [the article on running the cluster](running-pipeline.md). Most importantly: **Be sure to run the commands in the order shown above**, and specifically **be sure to run import-pipelines last!**

#### One Last Thing

As with a PostgreSQL system database, a database that uses some other RDBMS application will be controlled by you, or your system administrator, and not Ziggy. Ziggy will not attempt to stop or start the database.

<sup>1</sup> HSQLDB can be configured to run from on-disk catalogs and can theoretically handle 64 TB of data but that is beyond the scope of this document.

[[Previous]](task-configuration.md)
[[Up]](user-manual.md)
[[Next]](troubleshooting.md)
