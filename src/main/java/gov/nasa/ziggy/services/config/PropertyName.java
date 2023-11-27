package gov.nasa.ziggy.services.config;

/**
 * Container for definitions of all property names.
 *
 * @author PT
 * @author Bill Wohler
 */
public enum PropertyName {

    // This file is sorted by property name (not constant) to help organize properties.

    /**
     * If this property is true, require login even when running on trunk code. On non-trunk code,
     * login is always required. TODO Review in future and document if preserved
     */
    REQUIRE_LOGIN_OVERRIDE("console.dev.require.login"),

    /** Class used by Hibernate to manage the database connection. */
    HIBERNATE_DRIVER("hibernate.connection.driver_class"),

    /** Password used by Hibernate for database connection. */
    HIBERNATE_PASSWORD("hibernate.connection.password"),

    /** URL used by Hibernate for database connection. */
    HIBERNATE_URL("hibernate.connection.url"),

    /** Username used by Hibernate for database connection. */
    HIBERNATE_USERNAME("hibernate.connection.username"),

    /** Database dialect used by Hibernate. */
    HIBERNATE_DIALECT("hibernate.dialect"),

    /** "Pretty-print" generated queries written to log file. */
    HIBERNATE_FORMAT_SQL("hibernate.format_sql"),

    /** JDBC batch sized used by Hibernate. Can be tuned to improve performance. */
    HIBERNATE_JDBC_BATCH_SIZE("hibernate.jdbc.batch_size"),

    /** Write generated queries to log file. */
    HIBERNATE_SHOW_SQL("hibernate.show_sql"),

    /** Generate comments in generated queries in log. */
    HIBERNATE_USE_SQL_COMMENTS("hibernate.use_sql_comments"),

    /**
     * The {@code java.class.path} system property. As it is not user-modifiable, this property
     * should not be documented in the manual.
     */
    JAVA_CLASS_PATH("java.class.path"),

    /**
     * Location of the Java package to be used by the ziggy program when executing programs.
     */
    JAVA_HOME("java.home"),

    /**
     * The {@code java.runtime.name} system property. As it is not user-modifiable, this property
     * should not be documented in the manual.
     */
    JAVA_RUNTIME_NAME("java.runtime.name"),

    /**
     * The {@code java.vm.version} system property. As it is not user-modifiable, this property
     * should not be documented in the manual.
     */
    JAVA_VM_VERSION("java.vm.version"),

    /**
     * The log4j2 configuration property. As it is not user-modifiable, this property should not be
     * documented in the manual.
     */

    LOG4J2_CONFIGURATION_FILE("log4j2.configurationFile"),

    /**
     * Property in the config service that points to the log4j.xml file used by Java code called
     * from MATLAB
     */
    MATLAB_LOG4J_CONFIG_FILE("matlab.log4j.config"),

    /** Set to true to initialize log4j when starting MATLAB. */
    MATLAB_LOG4J_CONFIG_INITIALIZE("matlab.log4j.initialize"),

    /**
     * Name for the operating system. As it is not user-modifiable, this property should not be
     * documented in the manual.
     */
    OPERATING_SYSTEM("os.name"),

    /**
     * System property name for architecture data model. As it is not user-modifiable, this property
     * should not be documented in the manual.
     */
    SUN_ARCH_DATA_MODEL("sun.arch.data.model"),

    /**
     * The {@code sun.boot.library.path} system property. As it is not user-modifiable, this
     * property should not be documented in the manual.
     */
    SUN_BOOT_LIBRARY_PATH("sun.boot.library.path"),

    /**
     * Working directory. As it is not user-modifiable, this property should not be documented in
     * the manual.
     */
    WORKING_DIR("user.dir"),

    /**
     * Current user. As it is not user-modifiable, this property should not be documented in the
     * manual.
     */
    USER_NAME("user.name"),

    /** Location of the configuration properties for Ziggy. */
    ZIGGY_CONFIG_PATH("ziggy.config.path"),

    /** Location of the database executables. */
    DATABASE_BIN_DIR("ziggy.database.bin.dir"),

    /** Location of the database configuration file. */
    DATABASE_CONF_FILE("ziggy.database.conf.file"),

    /** Maximum connections supported by database connection pooler. */
    DATABASE_CONNECTIONS("ziggy.database.connections"),

    /** Directory used by the relational database. */
    DATABASE_DIR("ziggy.database.dir"),

    /** Host used by the relational database. */
    DATABASE_HOST("ziggy.database.host"),

    /** Name of the database. */
    DATABASE_NAME("ziggy.database.name"),

    /** Port used by the relational database. */
    DATABASE_PORT("ziggy.database.port"),

    /** Protocol used by the relational database. */
    DATABASE_PROTOCOL("ziggy.database.protocol"),

    /** Location of the database schema for the pipeline + Ziggy */
    DATABASE_SCHEMA_DIR("ziggy.database.schema.dir"),

    /** Name of relational database software package. */
    DATABASE_SOFTWARE("ziggy.database.software.name"),

    /** Ziggy home directory (the build directory in the top-level Ziggy directory). */
    ZIGGY_HOME_DIR("ziggy.home.dir"),

    /** Location and name of the logo file for the pipeline (not the Ziggy logo). */
    PIPELINE_LOGO_FILE("ziggy.logoFile"),

    /** Search path for executables used by pipeline modules. */
    BINPATH("ziggy.pipeline.binPath"),

    /**
     * Whether alerts should always be broadcast. This property does not need to be documented in
     * the manual.
     */
    BROADCAST_ALERTS_ENABLED("ziggy.pipeline.broadcast.alerts.enabled"),

    /** Classpath for Java classes that are part of the pipeline (not part of Ziggy). */
    PIPELINE_CLASSPATH("ziggy.pipeline.classpath"),

    /** Implementation class of DataImporter used by data receipt. */
    DATA_IMPORTER_CLASS("ziggy.pipeline.data.importer.classname"),

    /** Directory from which data receipt imports files. */
    DATA_RECEIPT_DIR("ziggy.pipeline.data.receipt.dir"),

    /** Maximum percentage of files that can fail validation before DR throws an exception. */
    MAX_FAILURE_PERCENTAGE("ziggy.pipeline.data.receipt.validation.maxFailurePercentage"),

    /** Top-level dictionary of the datastore. */
    DATASTORE_ROOT_DIR("ziggy.pipeline.datastore.dir"),

    /** Location of XML files that define the pipeline. */
    PIPELINE_DEFS_DIR("ziggy.pipeline.definition.dir"),

    /** Environment definition used by pipeline modules. */
    RUNTIME_ENVIRONMENT("ziggy.pipeline.environment"),

    /** Pipeline home directory. */
    PIPELINE_HOME_DIR("ziggy.pipeline.home.dir"),

    /** Shared object library path for pipeline modules. */
    LIBPATH("ziggy.pipeline.libPath"),

    /** Location of the MATLAB Compiler Runtime (MCR). */
    MCRROOT("ziggy.pipeline.mcrRoot"),

    /** Determines whether memdrone is enabled. */
    MEMDRONE_ENABLED("ziggy.pipeline.memdrone.enabled"),

    /** Determines the interval between memdrone samples. */
    MEMDRONE_SLEEP("ziggy.pipeline.memdrone.sleepSeconds"),

    /**
     * Allows execution to stop after completion of a specified step (marshaling, executing, etc.).
     */
    PIPELINE_HALT("ziggy.pipeline.processing.halt.step"),

    /** Location of pipeline results. */
    RESULTS_DIR("ziggy.pipeline.results.dir"),

    /**
     * Whether pipeline status should be broadcast. This property does not need to be documented in
     * the manual.
     */
    STATUS_BROADCAST_ENABLED("ziggy.pipeline.status.broadcast.enabled"),

    /**
     * Interval for posting worker status (milliseconds) This property does not need to be
     * documented in the manual.
     */
    WORKER_STATUS_REPORT_INTERVAL_MILLIS("ziggy.pipeline.status.worker.reportIntervalMillis"),

    /** Class used to identify default UOWs that are defined in the pipeline (not Ziggy). */
    PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS("ziggy.pipeline.uow.defaultIdentifier.classname"),

    /** Indicates whether to copy files to/from the working directory, or use symlinks. */
    USE_SYMLINKS("ziggy.pipeline.useSymlinks"),

    /** Remote cluster to use when submitting batch jobs. */
    REMOTE_CLUSTER("ziggy.remote.cluster.name"),

    /** The user group to use on Pleiades. */
    REMOTE_GROUP("ziggy.remote.group"),

    /**
     * Names of Pleiades hosts, in order from most- to least-desired to use separated by
     * semi-colons.
     */
    REMOTE_HOST("ziggy.remote.host"),

    /** NASA directorate to be used for calculating likely NAS queue wait times. */
    REMOTE_NASA_DIRECTORATE("ziggy.remote.nasa.directorate"),

    /** Implementation class of QueueCommandManager for use by Ziggy. */
    REMOTE_QUEUE_COMMAND_CLASS("ziggy.remote.queuecommand.classname"),

    /** The username to use on Pleiades. */
    REMOTE_USER("ziggy.remote.user"),

    /**
     * Interval between messages from the supervisor to RMI clients to ensure that connections
     * remain intact.
     */
    HEARTBEAT_INTERVAL("ziggy.supervisor.heartbeat.interval.millis"),

    /**
     * Port used for communications between supervisor and console. Each cluster must have a port
     * that is not already in use on the system.
     */
    SUPERVISOR_PORT("ziggy.supervisor.port"),

    /**
     * Allows Ziggy unit test classes to inform the configuration system that it is, in fact, a test
     * environment, hence to not load the pipeline properties. Usually this is accomplished
     * automatically, but there are some corner cases where it's necessary to manually inform the
     * configuration system. In this case, it's necessary to set this as a system property so that
     * it survives resets of the configuration.
     */
    TEST_ENVIRONMENT("ziggy.test.environment"),

    /**
     * Property used by tests to ensure that ziggy.properties can be read. Its value is expected to
     * be {@code from.default.location}. This property should not be documented in the manual.
     */
    TEST_FILE("ziggy.test.file.property"),

    /** Allows the user to specify a working directory other than user.dir. For testing only. */
    ZIGGY_TEST_WORKING_DIR("ziggy.test.working.dir"),

    /** The current Ziggy version as determined by {@code git describe}. */
    ZIGGY_VERSION("ziggy.version"),

    /** Allow persisting to continue although one or more subtasks failed. */
    ALLOW_PARTIAL_TASKS("ziggy.worker.allowPartialTasks"),

    /** Number of workers. */
    WORKER_COUNT("ziggy.worker.count"),

    /**
     * Heap size for the worker process. This is converted to the wrapper heap size by
     * ClusterController.
     */
    WORKER_HEAP_SIZE("ziggy.worker.heapSize");

    private String property;

    PropertyName(String property) {
        this.property = property;
    }

    public String property() {
        return property;
    }

    @Override
    /** The {@link #property} method is favored. */
    public String toString() {
        return property();
    }
}
