<!-- -*-visual-line-*- -->

[[Previous]](dusty-corners.md)
[[Up]](dusty-corners.md)
[[Next]](redefine-parameter-set.md)

## More on the Relational Database

Let's start with an existential question:

### Why does Ziggy Use a Relational Database?

The relational database is the system Ziggy uses to store its permanent records of its activities. Every pipeline task, every parameter set, every event -- every everything is stored in the database, along with all the necessary cross-referencing (so, for example, there are cross-references between a pipeline instance and the definition of the pipeline, the tasks that run as part of the pipeline instance, etc.).

### Is the Database Used for Anything Relevant to Algorithm Execution?

Experience with prior missions has given us a sense that Ziggy's records are well-suited to be stored in a relational database, while the algorithms mainly need data that's not well-suited to such storage. This is why Ziggy stores models and data files in the datastore and never attempts to open those files up and manipulate the contents. 

The limited amount of database information that's potentially needed for algorithm execution is the stuff that's provided in the algorithm inputs files: module parameters, names of model and data files, data file type definitions, etc. 

### What if We Need to Change Pipeline Definitions?

Ziggy's storage of things like parameter sets and pipeline definitions is done in a way that supports versioning, and more importantly allows the user to store new versions of these definitions for future use without losing the old versions that were used in the past. We'll see more about how this works in the articles on [redefining a parameter set](redefine-parameter-set.md) and [redefining a pipeline](redefine-pipeline.md).

### Is it Possible to use the Database for Mission-Related Data?

Short answer: yes. 

Longer answer:

There are definitely cases in which some portion of the mission's data belongs in a relational database. As an example: on both the Kepler and TESS missions, the sky catalog was stored in the database. A sky catalog is a collection of all the stars in the mission's field of view, down to some limiting magnitude, with parameters like location in the sky, magnitude, temperature, etc. The reason this information was stored in that way and not as a model file in the datastore goes like this:

1. The amount of information in the catalog is large.
2. The portion of the catalog needed by any particular processing task is tiny. Hence:
3. It makes sense to query the database for the part of the catalog that a given task needs, and provide the results of that query to the task. 

#### The Bad News

In order to give the algorithms access to information in the database, one of two things needs to happen. Either:

1. Somebody will need to write specialized Java code that goes to the database, performs a query, and puts the results into the inputs for the task that goes with the query. Or:
2. The algorithm code will need to have its own software to talk to the database, perform the necessary query, and interpret the results of that query.

Put another way, either the database access has to happen before algorithm execution, or during algorithm execution. Either of these options requires skills that the typical subject matter expert is unlikely to have. In particular, we've made a diligent effort to keep the users from needing to write Java code that interfaces with Ziggy, and the first option breaks that desirement.

#### The Good News

Team Ziggy has a lot of experience with this sort of thing, and if you discover that your mission needs to store something in the database, and then the algorithms need the results of a database query in order to function, we can work with you to make it happen. [Drop us a line](contact-us.md).

[[Previous]](dusty-corners.md)
[[Up]](dusty-corners.md)
[[Next]](redefine-parameter-set.md)