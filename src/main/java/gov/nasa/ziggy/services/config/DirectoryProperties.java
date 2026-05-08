package gov.nasa.ziggy.services.config;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration2.ImmutableConfiguration;

/**
 * Provides one-stop shopping for various directory locations based on properties.
 *
 * @author PT
 * @author Bill Wohler
 */
public class DirectoryProperties {

    // Relative paths for various kinds of files.
    private static final String TASK_DATA_RELATIVE_PATH = "task-data";
    private static final String LOG_FILES_RELATIVE_PATH = "log";
    private static final String TASK_LOG_FILES_RELATIVE_PATH = "ziggy";
    private static final String CLI_LOG_FILES_RELATIVE_PATH = "cli";
    private static final String PBS_LOG_FILES_RELATIVE_PATH = "pbs";
    private static final String ALGORITHM_LOG_FILES_RELATIVE_PATH = "algorithms";
    private static final String DATABASE_LOG_FILES_RELATIVE_PATH = "db";
    private static final String SUPERVISOR_LOG_FILES_RELATIVE_PATH = "supervisor";
    private static final String SCHEMA_DIR_RELATIVE_PATH = "schema";
    private static final String DEFINITION_DIR_RELATIVE_PATH = "ziggy.d";
    private static final String MANIFESTS_RELATIVE_PATH = "manifests";
    private static final String REPORTS_DIR_RELATIVE_PATH = "reports";
    private static final String PYTHON_VENV_RELATIVE_PATH = "env";
    private static final String RUN_DIR_RELATIVE_PATH = "run";

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

    /**
     * Location of the etc directory relative to either {@link PropertyName#PIPELINE_HOME_DIR} or
     * {@link PropertyName#ZIGGY_HOME_DIR}.
     */
    private static final String ETC_DIR_RELATIVE_PATH = "etc";

    private static final String BUILD = "build";

    // Methods are generally grouped by parent directory and sorted within each group.

    public static Path pipelineResultsDir() {
        return Paths
            .get(ZiggyConfiguration.getInstance().getString(PropertyName.RESULTS_DIR.property()));
    }

    public static Path logDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH);
    }

    public static Path algorithmLogsDir() {
        return logDir().resolve(ALGORITHM_LOG_FILES_RELATIVE_PATH);
    }

    // TODO Why isn't this used more?
    public static Path cliLogDir() {
        return logDir().resolve(CLI_LOG_FILES_RELATIVE_PATH);
    }

    public static Path databaseLogDir() {
        return logDir().resolve(DATABASE_LOG_FILES_RELATIVE_PATH);
    }

    // TODO Unused?
    public static Path pbsLogDir() {
        return logDir().resolve(PBS_LOG_FILES_RELATIVE_PATH);
    }

    public static Path supervisorLogDir() {
        return logDir().resolve(SUPERVISOR_LOG_FILES_RELATIVE_PATH);
    }

    public static Path taskLogDir() {
        return logDir().resolve(TASK_LOG_FILES_RELATIVE_PATH);
    }

    public static Path manifestsDir() {
        return pipelineResultsDir().resolve(MANIFESTS_RELATIVE_PATH);
    }

    public static Path reportsDir() {
        return pipelineResultsDir().resolve(REPORTS_DIR_RELATIVE_PATH);
    }

    public static Path runDir() {
        return pipelineResultsDir().resolve(RUN_DIR_RELATIVE_PATH);
    }

    public static Path taskDataDir() {
        return pipelineResultsDir().resolve(TASK_DATA_RELATIVE_PATH);
    }

    public static Path ziggyHomeDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyName.ZIGGY_HOME_DIR.property(),
                System.getenv(ZiggyConfiguration.ZIGGY_HOME_ENV)));
    }

    public static Path ziggyBinDir() {
        return ziggyHomeDir().resolve(BIN_DIR_RELATIVE_PATH);
    }

    public static Path ziggyEtcDir() {
        return ziggyHomeDir().resolve(ETC_DIR_RELATIVE_PATH);
    }

    public static Path ziggyDefinitionDir() {
        return ziggyEtcDir().resolve(DEFINITION_DIR_RELATIVE_PATH);
    }

    public static Path ziggyLibDir() {
        return ziggyHomeDir().resolve(LIB_DIR_RELATIVE_PATH);
    }

    // TODO Currently unused, but ZiggyGuiConsole should use it
    public static Path ziggyLogoDir() {
        return ziggyHomeDir().resolve(Paths.get("resources", "main", "images"));
    }

    public static Path ziggySchemaDir() {
        return ziggyHomeDir().resolve(SCHEMA_DIR_RELATIVE_PATH);
    }

    public static Path pipelineHomeDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.PIPELINE_HOME_DIR.property()));
    }

    public static Path pipelineBinDir() {
        return pipelineHomeDir().resolve(BIN_DIR_RELATIVE_PATH);
    }

    /** Location where Ziggy looks for Python virtual environments. */
    public static Path pythonEnvDir() {
        return pipelineHomeDir().resolve(PYTHON_VENV_RELATIVE_PATH);
    }

    public static Path pipelineDefinitionDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.PIPELINE_DEFS_DIR.property()));
    }

    public static Path databaseSchemaDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_SCHEMA_DIR.property()));
    }

    public static Path ziggyCodeBuildDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyName.ZIGGY_BUILD_DIR.property(), BUILD));
    }

    public static Path ziggySchemaBuildDir() {
        return ziggyCodeBuildDir().resolve(SCHEMA_DIR_RELATIVE_PATH);
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

    // TODO DataReceiptPipelineStepExecutor in Zowie should use this
    public static Path dataReceiptDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyName.DATA_RECEIPT_DIR.property()));
    }
}
