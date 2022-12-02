package gov.nasa.ziggy.services.config;

/**
 * Container for definitions of all property names.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PropertyNames {

    // This file is sorted by property name (not constant) to help organize properties.

    /** Implementation class of DataImporter used by data receipt. */
    public static final String DATA_RECEIPT_CLASS_PROP_NAME = "data.importer.classname";

    /** Directory from which data receipt imports files. */
    public static final String DATA_RECEIPT_DIR_PROP_NAME = "data.receipt.dir";

    /** Maximum percentage of files that can fail validation before DR throws an exception. */
    public static final String MAX_FAILURE_PERCENTAGE_PROP_NAME = "dataReceipt.validation.maxFailurePercentage";

    /** Maximum connections supported by database connection pooler. */
    public static final String DATABASE_CONNECTIONS_PROP_NAME = "database.connections";

    /** Location of the database executables. */
    public static final String DATABASE_BIN_DIR_PROP_NAME = "database.bin.dir";

    /** Location of the database configuration file. */
    public static final String DATABASE_CONF_FILE_PROP_NAME = "database.conf.file";

    /** Directory used by the relational database. */
    public static final String DATABASE_DIR_PROP_NAME = "database.dir";

    /** Host used by the relational database. */
    public static final String DATABASE_HOST_PROP_NAME = "database.host";

    /** Name of the database. */
    public static final String DATABASE_NAME_PROP_NAME = "database.name";

    /** Port used by the relational database. */
    public static final String DATABASE_PORT_PROP_NAME = "database.port";

    /** Name of relational database software package. */
    public static final String DATABASE_SOFTWARE_PROP_NAME = "database.software.name";

    /** Top-level dictionary of the datastore. */
    public static final String DATASTORE_ROOT_DIR_PROP_NAME = "datastore.root.dir";

    /** Class used by Hibernate to manage the database connection. */
    public static final String HIBERNATE_DRIVER_PROP_NAME = "hibernate.connection.driver_class";

    /** Password used by Hibernate for database connection. */
    public static final String HIBERNATE_PASSWD_PROP_NAME = "hibernate.connection.password";

    /** URL used by Hibernate for database connection. */
    public static final String HIBERNATE_URL_PROP_NAME = "hibernate.connection.url";

    /** Username used by Hibernate for database connection. */
    public static final String HIBERNATE_USERNAME_PROP_NAME = "hibernate.connection.username";

    /** Database dialect used by Hibernate. */
    public static final String HIBERNATE_DIALECT_PROP_NAME = "hibernate.dialect";

    /**
     * Directs how how identity or sequence columns are generated when using @GeneratedValue. The
     * {@code SequenceHiLoGenerator} is used when false, which will have that multiply behavior. The
     * The {@code SequenceStyleGenerator} is used when false that is more JPA and Oracle friendly
     * and generates identifier values based on an sequence-style database structure. Variations
     * range from actually using a sequence to using a table to mimic a sequence. The default value
     * switches from false to true in Hibernate 5. The Ziggy default is true.
     */
    public static final String HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_PROP_NAME = "hibernate.id.new_generator_mappings";

    /** Determines whether memdrone is enabled. */
    public static final String MEMDRONE_ENABLED_PROP_NAME = "moduleExe.memdrone.enabled";

    /** Determines the interval between memdrone samples. */
    public static final String MEMDRONE_SLEEP_PROP_NAME = "moduleExe.memdrone.sleepSeconds";

    /** Indicates whether to copy files to/from the working directory, or use symlinks. */
    public static final String USE_SYMLINKS_PROP_NAME = "moduleExe.useSymlinks";

    // NASA directorate to be used for calculating likely NAS queue wait times.
    public final static String NASA_DIRECTORATE_PROP_NAME = "nasa.directorate";

    /** Name for the operating system. */
    public static final String OPERATING_SYSTEM_PROPERTY_NAME = "os.name";

    /** Environment definition used by pipeline modules. */
    public static final String RUNTIME_ENVIRONMENT_PROPERTY_NAME = "pi.moduleExe.environment";

    // Allows execution to stop after completion of a specified step (marshaling,
    // executing, etc.).
    public static final String PIPELINE_HALT_PROP_NAME = "pi.processing.halt.step";

    // Heap size for the worker process. This is converted to the wrapper heap size by
    // ClusterController.
    public static final String WORKER_HEAP_SIZE_PROP_NAME = "pi.worker.heapSize";

    /** Search path for executables used by pipeline modules. */
    public static final String MODULE_EXE_BINPATH_PROPERTY_NAME = "pi.worker.moduleExe.binPath";

    /** Shared object library path for pipeline modules. */
    public static final String MODULE_EXE_LIBPATH_PROPERTY_NAME = "pi.worker.moduleExe.libPath";

    /** Location of the MATLAB Compiler Runtime (MCR). */
    public static final String MODULE_EXE_MCRROOT_PROPERTY_NAME = "pi.worker.moduleExe.mcrRoot";

    /** Number of worker threads. */
    public static final String WORKER_THREAD_COUNT_PROP_NAME = "pi.worker.threadCount";

    /** Classpath for Java classes that are part of the pipeline (not part of Ziggy). */
    public static final String PIPELINE_CLASSPATH_PROP_NAME = "pipeline.classpath";

    /** Location of XML files that define the pipeline. */
    public static final String PIPELINE_DEFS_DIR_PROP_NAME = "pipeline.definition.dir";

    /** Pipeline home directory. */
    public static final String PIPELINE_HOME_DIR_PROP_NAME = "pipeline.home.dir";

    /** Location and name of the logo file for the pipeline (not the Ziggy logo). */
    public static final String PIPELINE_LOGO_FILE_PROP_NAME = "pipeline.logoFile";

    /** Location of pipeline results. */
    public static final String RESULTS_DIR_PROP_NAME = "pipeline.results.dir";

    /** Class used to identify default UOWs that are defined in the pipeline (not Ziggy). */
    public static final String PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS_PROP_NAME = "pipeline.uow.defaultIdentifier";

    /** Implementation class of QueueCommandManager for use by Ziggy. */
    public static final String QUEUE_COMMAND_CLASS_PROP_NAME = "pleiades.queuecommand.classname";

    /** Remote cluster to use when submitting batch jobs. */
    public static final String CLUSTER_PROPERTY_NAME = "remote.cluster.name";

    /** System property name for architecture data model. */
    public static final String ARCH_DATA_MODEL_PROPERTY_NAME = "sun.arch.data.model";

    /** Working directory. */
    public static final String CURRENT_DIR_PROP_NAME = "user.dir";

    /**
     * Interval between messages from the worker to RMI clients to ensure that connections remain
     * intact.
     */
    public static final String HEARTBEAT_INTERVAL_PROP_NAME = "worker.heartbeat.interval.millis";

    /** Classpath elements for the wrapper executable. */
    public static final String WRAPPER_CLASSPATH_PROP_NAME = "wrapper.java.classpath.";

    /** Heap size for the wrapper executable. */
    public static final String WRAPPER_HEAP_SIZE_PROP_NAME = "wrapper.java.maxmemory";

    /** Log file for the wrapper executable. */
    public static final String WRAPPER_LOG_FILE_PROP_NAME = "wrapper.logfile";

    /** Location of the configuration properties for Ziggy. */
    public static final String ZIGGY_CONFIG_PROP_NAME = "ziggy.config.path";

    /** Ziggy home directory (the build directory in the top-level Ziggy directory). */
    public static final String ZIGGY_HOME_DIR_PROP_NAME = "ziggy.home.dir";

    // Allows the user to specify a working directory other than user.dir. For testing only.
    public static final String ZIGGY_TEST_WORKING_DIR_PROP_NAME = "ziggy.test.working.dir";

    /**
     * If this property is true, require login even when running on trunk code. On non-trunk code,
     * login is always required
     */
    public static final String REQUIRE_LOGIN_OVERRIDE = "console.dev.require.login";
}
