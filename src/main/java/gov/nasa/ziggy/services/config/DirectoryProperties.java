package gov.nasa.ziggy.services.config;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration2.ImmutableConfiguration;

/**
 * Provides one-stop shopping for various directory locations based on properties.
 *
 * @author PT
 */
public class DirectoryProperties {

    // Relative paths for various kinds of files.
    private static final String TASK_DATA_RELATIVE_PATH = "task-data";
    private static final String LOG_FILES_RELATIVE_PATH = "logs";
    private static final String TASK_LOG_FILES_RELATIVE_PATH = "ziggy";
    private static final String CLI_LOG_FILES_RELATIVE_PATH = "cli";
    private static final String PBS_LOG_FILES_RELATIVE_PATH = "pbs";
    private static final String ALGORITHM_LOG_FILES_RELATIVE_PATH = "algorithms";
    private static final String DATABASE_LOG_FILES_RELATIVE_PATH = "db";
    private static final String SUPERVISOR_LOG_FILES_RELATIVE_PATH = "supervisor";
    private static final String PI_LOG_FILES_RELATIVE_PATH = "pi";
    private static final String SCHEMA_DIR_RELATIVE_PATH = "schema";
    private static final String CONFIG_DIR_RELATIVE_PATH = "config";
    private static final String MANIFESTS_RELATIVE_PATH = "manifests";
    private static final String MEMDRONE_RELATIVE_PATH = "memdrone";
    private static final String REPORTS_DIR_RELATIVE_PATH = "reports";

    /**
     * Location of the bin directory relative to either {@link PropertyName#PIPELINE_HOME_DIR} or
     * {@link PropertyName#ZIGGY_HOME_DIR}.
     */
    private static final String BIN_DIR_RELATIVE_PATH = "bin";

    /**
     * Location of the lib directory relative to either {@link PropertyName#PIPELINE_HOME_DIR} or
     * {@link PropertyName#ZIGGY_HOME_DIR}.
     */
    private static final String LIB_DIR_RELATIVE_PATH = "lib";

    private static final String BUILD = "build";

    private static final String TMP = "tmp";

    public static Path pipelineResultsDir() {
        return Paths
            .get(ZiggyConfiguration.getInstance().getString(PropertyName.RESULTS_DIR.property()));
    }

    public static Path taskDataDir() {
        return pipelineResultsDir().resolve(TASK_DATA_RELATIVE_PATH);
    }

    public static Path logDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH);
    }

    public static Path taskLogDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(TASK_LOG_FILES_RELATIVE_PATH);
    }

    public static Path cliLogDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(CLI_LOG_FILES_RELATIVE_PATH);
    }

    public static Path pbsLogDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(PBS_LOG_FILES_RELATIVE_PATH);
    }

    public static Path algorithmLogsDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(ALGORITHM_LOG_FILES_RELATIVE_PATH);
    }

    public static Path piLogsDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(PI_LOG_FILES_RELATIVE_PATH);
    }

    public static Path pipelineHomeDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.PIPELINE_HOME_DIR.property()));
    }

    public static Path pipelineBinDir() {
        return pipelineHomeDir().resolve(BIN_DIR_RELATIVE_PATH);
    }

    public static Path pipelineConfigDir() {
        return pipelineHomeDir().resolve(CONFIG_DIR_RELATIVE_PATH);
    }

    public static Path ziggyHomeDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyName.ZIGGY_HOME_DIR.property(),
                System.getenv(ZiggyConfiguration.ZIGGY_HOME_ENV)));
    }

    public static Path ziggyBinDir() {
        return ziggyHomeDir().resolve(BIN_DIR_RELATIVE_PATH);
    }

    public static Path ziggyLibDir() {
        return ziggyHomeDir().resolve(LIB_DIR_RELATIVE_PATH);
    }

    public static Path ziggySchemaDir() {
        return ziggyHomeDir().resolve(SCHEMA_DIR_RELATIVE_PATH);
    }

    public static Path databaseSchemaDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_SCHEMA_DIR.property()));
    }

    public static Path ziggyCodeBuildDir() {
        return Paths.get(BUILD);
    }

    public static Path ziggySchemaBuildDir() {
        return ziggyCodeBuildDir().resolve(SCHEMA_DIR_RELATIVE_PATH);
    }

    public static Path tmpDir() {
        return ziggyCodeBuildDir().resolve(TMP);
    }

    public static Path databaseLogDir() {
        return pipelineResultsDir()
            .resolve(Paths.get(LOG_FILES_RELATIVE_PATH, DATABASE_LOG_FILES_RELATIVE_PATH));
    }

    public static Path supervisorLogDir() {
        return pipelineResultsDir()
            .resolve(Paths.get(LOG_FILES_RELATIVE_PATH, SUPERVISOR_LOG_FILES_RELATIVE_PATH));
    }

    public static Path reportsDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(REPORTS_DIR_RELATIVE_PATH);
    }

    public static Path pipelineDefinitionDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.PIPELINE_DEFS_DIR.property()));
    }

    public static Path ziggyLogoDir() {
        return ziggyHomeDir().resolve(Paths.get("resources", "main", "images"));
    }

    public static Path manifestsDir() {
        return pipelineResultsDir()
            .resolve(Paths.get(LOG_FILES_RELATIVE_PATH, MANIFESTS_RELATIVE_PATH));
    }

    public static Path memdroneDir() {
        return logDir().resolve(MEMDRONE_RELATIVE_PATH);
    }

    /**
     * Returns the root of the database directory ({@link PropertyName#DATABASE_DIR}).
     *
     * @return the database root directory, or null if this property is empty, which can be the case
     * if a system database is in use
     */
    public static Path databaseDir() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_DIR.property(), null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * Returns the absolute path to the database executables, which is in the properties file as the
     * value of the {@link PropertyName#DATABASE_BIN_DIR} property. If no such property is set, null
     * is returned. This last is necessary because the user may decide to put the database
     * executables on their search path rather than specifying the path in the properties file.
     */
    public static Path databaseBinDir() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_BIN_DIR.property(), null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * Returns the absolute path to the database executables, which is in the properties file as the
     * value of the {@link PropertyName#DATABASE_CONF_FILE} property. If no such property is set,
     * null is returned. This last is necessary because the specifying such a file is optional.
     */
    public static Path databaseConfFile() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_CONF_FILE.property(), null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * During normal activities, returns the working directory specified by the
     * {@link PropertyName#WORKING_DIR} system property. During testing, setting the
     * {@link PropertyName#ZIGGY_TEST_WORKING_DIR} property allows this to return a different path
     * to be used as the working directory for test purposes.
     */
    public static Path workingDir() {
        ImmutableConfiguration configuration = ZiggyConfiguration.getInstance();

        return Paths.get(configuration.getString(PropertyName.ZIGGY_TEST_WORKING_DIR.property(),
            configuration.getString(PropertyName.WORKING_DIR.property())));
    }

    public static Path datastoreRootDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.DATASTORE_ROOT_DIR.property()));
    }

    public static Path dataReceiptDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.DATA_RECEIPT_DIR.property()));
    }
}
