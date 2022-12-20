package gov.nasa.ziggy.services.config;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.SystemUtils;

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
    private static final String STATE_FILES_RELATIVE_PATH = "state";
    private static final String ALGORITHM_LOG_FILES_RELATIVE_PATH = "algorithms";
    private static final String DATABASE_LOG_FILES_RELATIVE_PATH = "db";
    private static final String WORKER_LOG_FILES_RELATIVE_PATH = "worker";
    private static final String PI_LOG_FILES_RELATIVE_PATH = "pi";
    private static final String SCHEMA_DIR_RELATIVE_PATH = "schema";
    private static final String CONFIG_DIR_RELATIVE_PATH = "config";
    private static final String MANIFESTS_RELATIVE_PATH = "manifests";
    private static final String MEMDRONE_RELATIVE_PATH = "memdrone";
    private static final String REPORTS_DIR_RELATIVE_PATH = "reports";

    /**
     * Location of the bin directory relative to either
     * {@link PropertyNames#PIPELINE_HOME_DIR_PROP_NAME} or
     * {@link PropertyNames#ZIGGY_HOME_DIR_PROP_NAME}.
     */
    private static final String BIN_DIR_RELATIVE_PATH = "bin";

    /**
     * Location of the lib directory relative to either
     * {@link PropertyNames#PIPELINE_HOME_DIR_PROP_NAME} or
     * {@link PropertyNames#ZIGGY_HOME_DIR_PROP_NAME}.
     */
    private static final String LIB_DIR_RELATIVE_PATH = "lib";

    private static final String BUILD = "build";

    private static final String TMP = "tmp";

    public static Path pipelineResultsDir() {
        return Paths
            .get(ZiggyConfiguration.getInstance().getString(PropertyNames.RESULTS_DIR_PROP_NAME));
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

    public static Path stateFilesDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(STATE_FILES_RELATIVE_PATH);
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
            ZiggyConfiguration.getInstance().getString(PropertyNames.PIPELINE_HOME_DIR_PROP_NAME));
    }

    public static Path pipelineBinDir() {
        return pipelineHomeDir().resolve(BIN_DIR_RELATIVE_PATH);
    }

    public static Path pipelineConfigDir() {
        return pipelineHomeDir().resolve(CONFIG_DIR_RELATIVE_PATH);
    }

    public static Path ziggyHomeDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyNames.ZIGGY_HOME_DIR_PROP_NAME));
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

    public static Path workerLogDir() {
        return pipelineResultsDir()
            .resolve(Paths.get(LOG_FILES_RELATIVE_PATH, WORKER_LOG_FILES_RELATIVE_PATH));
    }

    public static Path reportsDir() {
        return pipelineResultsDir().resolve(LOG_FILES_RELATIVE_PATH)
            .resolve(REPORTS_DIR_RELATIVE_PATH);
    }

    public static Path pipelineDefinitionDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyNames.PIPELINE_DEFS_DIR_PROP_NAME));
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
     * Returns the root of the database directory ({@value PropertyNames#DATABASE_DIR_PROP_NAME}).
     *
     * @return the database root directory, or null if this property is empty, which can be the case
     * if a system database is in use
     */
    public static Path databaseDir() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyNames.DATABASE_DIR_PROP_NAME, null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * Returns the absolute path to the database executables, which is in the properties file as the
     * value of the database.bin.dir property. If no such property is set, null is returned. This
     * last is necessary because the user may decide to put the database executables on their search
     * path rather than specifying the path in the properties file.
     */
    public static Path databaseBinDir() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyNames.DATABASE_BIN_DIR_PROP_NAME, null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * Returns the absolute path to the database executables, which is in the properties file as the
     * value of the database.conf.file property. If no such property is set, null is returned. This
     * last is necessary because the specifying such a file is optional.
     */
    public static Path databaseConfFile() {
        String pathString = ZiggyConfiguration.getInstance()
            .getString(PropertyNames.DATABASE_CONF_FILE_PROP_NAME, null);
        return pathString != null ? Paths.get(pathString) : null;
    }

    /**
     * During normal activities, returns the working directory specified by the user.dir system
     * property. During testing, setting the ziggy.test.working.dir property allows this to return a
     * different path to be used as the working directory for test purposes.
     */
    public static Path workingDir() {
        return Paths.get(ZiggyConfiguration.getInstance()
            .getString(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME, SystemUtils.USER_DIR));
    }

    public static Path datastoreRootDir() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME));
    }
}
