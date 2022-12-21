/*
 * Copyright © 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.ui;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.data.management.DataFileTypeImporter;
import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseController;
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.events.ZiggyEventHandlerDefinitionImporter;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Tool to manage the pipeline cluster (here "cluster" means the combination of a database instance,
 * a datastore directory, and a worker process). The available options are:
 * <ol>
 * <li>Initialize the cluster: this means clearing out the existing database and datastore and
 * constructing a fresh database from the schema, plus populating the parameters, data types, and
 * pipeline definitions.
 * <li>Start the cluster: this means updating the Java heap space available for the worker, starting
 * the worker, and starting the database.
 * <li>Stop the cluster: this means stopping the worker and stopping the database.
 * <li>Check cluster status: check that the cluster is initialized and that both the worker and the
 * database are running.
 * <li>Start pipeline console: check whether the cluster is running and if so start an instance of
 * the {@link ZiggyGuiConsole} class.
 * </ol>
 * The cluster controller can accept multiple command line arguments (i.e., you can start the
 * cluster and then check its status, etc.). Some combination of options are not permitted (i.e.,
 * starting and stopping the cluster in a single operation), and an exception will be thrown if
 * these are used together. The order of the options at the command line is not the order in which
 * the actions are executed: actions proceed in the order initialize-start (or stop)-status-console.
 * <p>
 * Ziggy allows the user to select a database that is not under Ziggy's control. In this case, it is
 * assumed that the user will be responsible for creating, starting, and stopping the database, and
 * Ziggy will not attempt to perform these functions. To use an external, independent database, the
 * user must do the following:
 * <ol>
 * <li>Have the DBA create a database account for the user and a database owned by the user.
 * <li>Specify that database name via the {@value PropertyNames#DATABASE_NAME_PROP_NAME} property in
 * the properties file.
 * <li>Ensure that the {@value PropertyNames#DATABASE_DIR_PROP_NAME} property is empty or missing.
 * </ol>
 * <p>
 * If the database name is not a supported database, the user will also have to do the following:
 * <ol>
 * <li>Ensure that the {@value PropertyNames#DATABASE_SOFTWARE_PROP_NAME} property is empty,
 * missing, or contains a string not in the list of supported databases.
 * <li>Specify the database SQL dialect via the {@value PropertyNames#HIBERNATE_DIALECT_PROP_NAME}
 * property in the properties file.
 * <li>Specify the fully-qualified Java class to use as a driver for communication with the database
 * via the {@value PropertyNames#HIBERNATE_DRIVER_PROP_NAME} property in the properties file. The
 * driver must be on Ziggy's classpath.
 * <li>Update the build to create a schema for the database.
 * <li>Manually load this database schema, as well as the pipeline definitions by calling runjava
 * p*import.
 * </ol>
 *
 * @author PT
 * @author Bill Wohler
 */
public class ClusterController {

    public static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    // Parameters related to cluster initialization
    private static final String PARAM_LIBRARY_PREFIX = "pl-";
    private static final String TYPE_FILE_PREFIX = "pt-";
    private static final String PIPELINE_DEF_FILE_PREFIX = "pd-";
    private static final String EVENT_HANDLER_DEF_FILE_PREFIX = "pe-";
    private static final String XML_SUFFIX = ".xml";

    private static final String WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX = "wrapper.java.library.path.";
    private static final String WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX = "wrapper.java.additional.";
    private static final String WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX = "wrapper.app.parameter.";

    // Parameters related to the worker process
    private static final String WORKER_BIN_NAME = "worker";
    private static final int WORKER_HEAP_SIZE_DEFAULT = 16000;
    private static final int WORKER_THREAD_COUNT_DEFAULT = 1;
    private static final int WORKER_THREAD_COUNT_ALL_CORES = 0; // use number of cores
    private static final String WORKER_LOG_FILE_NAME = "worker.log";

    private static final String RUNJAVA_CONSOLE_COMMAND = "runjava console";

    // Cluster options
    private static final String FORCE_OPTION = "force";
    private static final String WORKER_HEAP_SIZE_OPTION = "workerHeapSize";
    private static final String WORKER_THREAD_COUNT_OPTION = "workerThreadCount";

    // Commands
    private static final String INIT_COMMAND = "init";
    private static final String START_COMMAND = "start";
    private static final String STOP_COMMAND = "stop";
    private static final String STATUS_COMMAND = "status";
    private static final String CONSOLE_COMMAND = "console";
    private static final List<String> ALL_COMMANDS = ImmutableList.of(INIT_COMMAND, START_COMMAND,
        STOP_COMMAND, STATUS_COMMAND, CONSOLE_COMMAND);

    private static final String COMMAND_HELP = "\nCommands:\n"
        + "init                    Initialize the cluster\n"
        + "start                   Start the cluster\n"
        + "stop                    Stop the cluster\n"
        + "status                  Check cluster status\n"
        + "console                 Start pipeline console GUI\n\n" + "Options:";

    private final DatabaseController databaseController;

    private final int workerHeapSize;
    private final int workerThreadCount;

    public ClusterController(int workerHeapSize, int workerThreadCount) {
        databaseController = DatabaseController.newInstance();
        this.workerHeapSize = workerHeapSize;
        this.workerThreadCount = workerThreadCount != WORKER_THREAD_COUNT_ALL_CORES
            ? workerThreadCount
            : cpuCount();
    }

    private int cpuCount() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        log.info("Setting number of worker threads to number of available processors ("
            + availableProcessors + ")");
        return availableProcessors;
    }

    public static void main(String[] args) {

        // Define all the command options.
        Options options = new Options()
            .addOption(Option.builder("f")
                .longOpt(FORCE_OPTION)
                .desc("Force initialization if cluster is already initialized")
                .build())
            .addOption(Option.builder()
                .longOpt(WORKER_HEAP_SIZE_OPTION)
                .hasArg()
                .desc("Total heap size used by all workers (MB)")
                .build())
            .addOption(Option.builder()
                .longOpt(WORKER_THREAD_COUNT_OPTION)
                .hasArg()
                .desc("Number of worker threads or 0 to use all cores")
                .build());

        CommandLine cmdLine = null;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            usageAndExit(options, e);
        }
        // If no commands, go to usage and exit.
        List<String> commands = cmdLine.getArgList();
        if (commands.isEmpty()) {
            usageAndExit(options, (String) null);
        }
        List<String> unrecognizedCommands = unrecognizedCommands(commands);
        if (!unrecognizedCommands.isEmpty()) {
            usageAndExit(options, "unrecognized commands: " + unrecognizedCommands.toString());
        }

        int workerHeapSize = -1;
        int workerThreadCount = -1;
        try {
            Configuration config = ZiggyConfiguration.getInstance();
            if (cmdLine.hasOption(WORKER_HEAP_SIZE_OPTION)) {
                workerHeapSize = Integer.parseInt(cmdLine.getOptionValue(WORKER_HEAP_SIZE_OPTION));
            } else {
                workerHeapSize = config.getInt(PropertyNames.WORKER_HEAP_SIZE_PROP_NAME,
                    WORKER_HEAP_SIZE_DEFAULT);
            }
            if (cmdLine.hasOption(WORKER_THREAD_COUNT_OPTION)) {
                workerThreadCount = Integer
                    .parseInt(cmdLine.getOptionValue(WORKER_THREAD_COUNT_OPTION));
            } else {
                workerThreadCount = config.getInt(PropertyNames.WORKER_THREAD_COUNT_PROP_NAME,
                    WORKER_THREAD_COUNT_DEFAULT);
            }
        } catch (NumberFormatException e) {
            usageAndExit(options, "Worker option values are not numeric: " + e.getMessage());
        }

        ClusterController clusterController = new ClusterController(workerHeapSize,
            workerThreadCount);

        // Reject illegal combinations of commands
        if (commands.contains(INIT_COMMAND) && commands.size() > 1) {
            usageAndExit(options,
                "Initialize command can only be used by itself or with force option");
        }

        if (commands.contains(START_COMMAND) && commands.contains(STOP_COMMAND)) {
            usageAndExit(options, "Start and stop commands cannot be used simultaneously");
        }

        if (commands.contains(STOP_COMMAND) && commands.contains(CONSOLE_COMMAND)) {
            usageAndExit(options, "Stop and GUI commands cannot be used simultaneously");
        }

        // Execute commands
        if (commands.contains(INIT_COMMAND)) {
            try {
                clusterController.initializeCluster(cmdLine.hasOption(FORCE_OPTION));
            } catch (Exception e) {
                usageAndExit(null, e);
            }
        }

        if (commands.contains(START_COMMAND)) {
            clusterController.startCluster(cmdLine.hasOption(FORCE_OPTION));
        }

        if (commands.contains(STOP_COMMAND)) {
            clusterController.stopCluster();
        }

        if (commands.contains(STATUS_COMMAND)) {
            clusterController.status();
        }

        if (commands.contains(CONSOLE_COMMAND)) {
            clusterController.startPipelineConsole();
        }

    }

    private static List<String> unrecognizedCommands(List<String> commands) {
        return commands.stream()
            .filter(s -> !ALL_COMMANDS.contains(s))
            .collect(Collectors.toList());

    }

    private void initializeCluster(boolean force) throws Exception {
        if (isWorkerRunning() || databaseController != null && databaseController.status() == 0) {
            throw new PipelineException("Cannot reinitialize cluster when cluster is running");
        }
        if (isInitialized() && !force) {
            throw new PipelineException(
                "Cannot re-initialize an initialized cluster without --force option");
        }

        if (databaseController != null) {
            if (databaseController.isSystemDatabase()) {
                log.info("INIT: deleting contents of existing database");
                databaseController.dropDatabase();
                log.info("INIT: deleting contents of existing database...done");
            } else {
                log.info("INIT: deleting existing database directory");
                FileUtils.deleteDirectory(DirectoryProperties.databaseDir().toFile());
                log.info("INIT: deleting existing database directory...done");
            }
        }

        log.info("INIT: deleting existing datastore directory");
        FileUtils.deleteDirectory(DirectoryProperties.datastoreRootDir().toFile());
        log.info("INIT: deleting existing datastore directory...done");

        if (databaseController != null) {
            log.info("INIT: creating database");
            databaseController.createDatabase();
            log.info("INIT: creating database...done");
        }

        log.info("INIT: creating datastore directory");
        if (!DirectoryProperties.datastoreRootDir().toFile().mkdirs()) {
            throw new PipelineException("Unable to create directory defined by "
                + PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME);
        }
        log.info("INIT: creating datastore directory...done");

        int dbStartRetCode = databaseController.start();
        if (dbStartRetCode == DatabaseController.NOT_SUPPORTED) {
            log.info("INIT: system database should be started already");
        } else if (dbStartRetCode != 0) {
            throw new PipelineException("Unable to start initialized database");
        }

        log.info("INIT: Creating utility pipeline modules...");
        createUtilityPipelineModules();
        log.info("INIT: Creating utility pipeline modules...done");

        DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<Void>() {
            @Override
            public Void transaction() throws Exception {

                // Import the parameters, data types, and pipelines.
                Path pipelineDefsDir = DirectoryProperties.pipelineDefinitionDir();
                if (!Files.exists(pipelineDefsDir)) {
                    throw new PipelineException("Pipeline definitions directory "
                        + pipelineDefsDir.toString() + " does not exist");
                }

                log.info("INIT: importing parameter libraries from directory "
                    + pipelineDefsDir.toString());
                File[] parameterFiles = pipelineDefsDir.toFile()
                    .listFiles(
                        (FilenameFilter) (dir, name) -> (name.startsWith(PARAM_LIBRARY_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(parameterFiles, (f1, f2) -> f1.getName().compareTo(f2.getName()));
                ParametersOperations paramsOps = new ParametersOperations();
                for (File parameterFile : parameterFiles) {
                    log.info("INIT: importing library " + parameterFile.getName());
                    paramsOps.importParameterLibrary(parameterFile, null, ParamIoMode.STANDARD);
                }

                log.info(
                    "INIT: importing data file types from directory " + pipelineDefsDir.toString());
                File[] dataTypeFiles = pipelineDefsDir.toFile()
                    .listFiles((FilenameFilter) (dir,
                        name) -> (name.startsWith(TYPE_FILE_PREFIX) && name.endsWith(XML_SUFFIX)));
                Arrays.sort(dataTypeFiles, (f1, f2) -> f1.getName().compareTo(f2.getName()));
                List<String> dataTypeFileNames = new ArrayList<>(dataTypeFiles.length);
                for (File dataTypeFile : dataTypeFiles) {
                    log.info("INIT: adding " + dataTypeFile.getName() + " to imports list");
                    dataTypeFileNames.add(dataTypeFile.getAbsolutePath());
                }
                new DataFileTypeImporter(dataTypeFileNames, false).importFromFiles();

                log.info("INIT: importing pipeline definitions from directory "
                    + pipelineDefsDir.toString());
                File[] pipelineDefinitionFiles = pipelineDefsDir.toFile()
                    .listFiles(
                        (FilenameFilter) (dir, name) -> (name.startsWith(PIPELINE_DEF_FILE_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(pipelineDefinitionFiles,
                    (f1, f2) -> f1.getName().compareTo(f2.getName()));
                List<File> pipelineDefFileList = new ArrayList<>();
                for (File pipelineDefinitionFile : pipelineDefinitionFiles) {
                    log.info(
                        "INIT: adding " + pipelineDefinitionFile.getName() + " to imports list");
                    pipelineDefFileList.add(pipelineDefinitionFile);
                }
                new PipelineDefinitionOperations().importPipelineConfiguration(pipelineDefFileList);

                log.info("INIT: importing event definitions from directory "
                    + pipelineDefsDir.toString());
                File[] handlerDefinitionFiles = pipelineDefsDir.toFile()
                    .listFiles((FilenameFilter) (dir,
                        name) -> (name.startsWith(EVENT_HANDLER_DEF_FILE_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(handlerDefinitionFiles,
                    (f1, f2) -> f1.getName().compareTo(f2.getName()));
                new ZiggyEventHandlerDefinitionImporter(handlerDefinitionFiles).importFromFiles();

                return null;
            }

            @Override
            public void finallyBlock() {
                databaseController.stop();
            }

        });
        log.info("INIT: database initialization and creation complete");
    }

    /**
     * Creates instances of {@link PipelineModuleDefinition} for pipeline modules that perform
     * utility functions for data analysis pipelines. This allows users to implement pipelines that
     * use the utility pipeline modules without having to define them explicitly themselves. At the
     * moment, the only such is the data receipt module, which imports data and models into the
     * datastore.
     */
    private void createUtilityPipelineModules() {

        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineModuleDefinitionCrud()
                .createOrUpdate(DataReceiptPipelineModule.createDataReceiptPipelineForDb());
            return null;
        });
    }

    private boolean isInitialized() {
        return Files.exists(DirectoryProperties.datastoreRootDir());
    }

    private boolean isClusterRunning() {
        return isDatabaseRunning() && isWorkerRunning();
    }

    /**
     * If an unsupported or system database is in use, we have to assume it is running.
     *
     * @return false if we know for sure the database is not running; true if we know for sure or
     * don't know
     */
    public boolean isDatabaseRunning() {
        return databaseController == null
            || databaseController.status() == DatabaseController.NOT_SUPPORTED
            || databaseController.status() == 0;
    }

    public boolean isWorkerRunning() {
        WorkerCommandLine workerStatusCommand = workerCommand(WorkerCommand.STATUS);
        log.debug("Command line: " + workerStatusCommand);
        return ExternalProcess.simpleExternalProcess(workerStatusCommand).execute() == 0;
    }

    private enum WorkerCommand {
        START, STOP, STATUS;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    private WorkerCommandLine workerCommand(WorkerCommand cmd) {
        Path workerPath = DirectoryProperties.ziggyBinDir().resolve(WORKER_BIN_NAME);
        WorkerCommandLine commandLine = new WorkerCommandLine(workerPath.toString());
        if (cmd == WorkerCommand.START) {
            // Refer to worker.wrapper.conf for appropriate indices for the parameters specified
            // here.
            String ziggyLibDir = DirectoryProperties.ziggyLibDir().toString();
            commandLine
                .addWorkerOption(PropertyNames.WRAPPER_LOG_FILE_PROP_NAME,
                    DirectoryProperties.workerLogDir().resolve(WORKER_LOG_FILE_NAME).toString())
                .addWorkerOption(PropertyNames.WRAPPER_HEAP_SIZE_PROP_NAME,
                    Integer.toString(workerHeapSize))
                .addWorkerOption(
                    wrapperParameterString(PropertyNames.WRAPPER_CLASSPATH_PROP_NAME, 1),
                    DirectoryProperties.ziggyHomeDir().resolve("libs").resolve("*.jar").toString())
                .addWorkerOption(wrapperParameterString(WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX, 1),
                    ziggyLibDir)
                .addWorkerOption(
                    wrapperParameterString(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 5),
                    "-Djna.library.path=" + ziggyLibDir)
                .addWorkerOption(wrapperParameterString(WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX, 2),
                    Integer.toString(workerThreadCount));

            // Add classpaths for pipeline side, if any are needed.
            String pipelineClasspath = ZiggyConfiguration.getInstance()
                .getString(PropertyNames.PIPELINE_CLASSPATH_PROP_NAME, null);
            if (pipelineClasspath != null) {
                String[] pipelineClasspaths = pipelineClasspath.split(":");
                for (int i = 0; i < pipelineClasspaths.length; i++) {
                    int classpathIndex = i + 2;
                    commandLine.addWorkerOption(
                        wrapperParameterString(PropertyNames.WRAPPER_CLASSPATH_PROP_NAME,
                            classpathIndex),
                        pipelineClasspaths[i]);
                }
            }
        }

        return commandLine.addArgument(cmd.toString());
    }

    /**
     * Returns a wrapper parameter string such as wrapper.app.parameter.1.
     *
     * @param index counter to append to parameter prefix
     * @param wrapperPropNamePrefix wrapper parameter prefix
     */
    private String wrapperParameterString(String wrapperPropNamePrefix, int index) {
        StringBuilder s = new StringBuilder();
        s.append(wrapperPropNamePrefix).append(index);
        return s.toString();
    }

    private void startCluster(boolean force) {

        if (isClusterRunning()) {
            throw new PipelineException("Cannot start cluster; cluster already running");
        }
        if (!isInitialized()) {
            if (force) {
                log.warn("Attempting to start uninitialized cluster");
            } else {
                throw new PipelineException("Cannot start cluster; cluster not initialized");
            }
        }

        try {
            if (isDatabaseRunning()) {
                log.info("Database already running");
            } else {
                log.info("Starting database");
                if (databaseController == null) {
                    log.info("Not starting an unsupported database");
                } else {
                    int dbStartStatus = databaseController.start();
                    if (dbStartStatus == 0) {
                        log.info("Database started");
                    } else if (dbStartStatus == DatabaseController.NOT_SUPPORTED) {
                        log.info("Not starting a system database");
                    } else {
                        throw new PipelineException("Could not start database");
                    }
                }
            }

            if (isWorkerRunning()) {
                log.info("Worker already running");
            } else {
                log.info("Starting worker");
                log.debug("Creating directory " + DirectoryProperties.workerLogDir());
                Files.createDirectories(DirectoryProperties.workerLogDir());
                WorkerCommandLine workerStatusCommand = workerCommand(WorkerCommand.START);
                log.debug("Command line: " + workerStatusCommand.toString());
                ExternalProcess.simpleExternalProcess(workerStatusCommand)
                    .exceptionOnFailure()
                    .execute();
                log.info("Worker started");
            }
            log.info("Cluster started");
        } catch (Throwable t) {
            log.error("Caught exception when trying to start cluster, shutting down", t);
            stopCluster();
            throw new PipelineException("Unable to start cluster", t);
        }
    }

    private void stopCluster() {
        log.info("Worker stopping");
        WorkerCommandLine workerStopCommand = workerCommand(WorkerCommand.STOP);
        log.debug("Command line: " + workerStopCommand.toString());
        ExternalProcess.simpleExternalProcess(workerStopCommand).execute(true);
        if (!isWorkerRunning()) {
            log.info("Worker stopped");
        } else {
            log.error("Unable to stop worker process!");
        }
        log.info("Database stopping");
        if (databaseController == null) {
            log.info("Not stopping an unsupported database");
        } else {
            int dbStopStatus = databaseController.stop();
            if (dbStopStatus == 0) {
                log.info("Database stopped");
            } else if (dbStopStatus == DatabaseController.NOT_SUPPORTED) {
                log.info("Not stopping a system database");
            } else {
                log.error("Unable to stop database!");
            }
        }
        log.info("Cluster stopped");
    }

    private void status() {
        boolean supportedDatabase = databaseController != null;
        boolean systemDatabase = supportedDatabase
            && databaseController.status() == DatabaseController.NOT_SUPPORTED;

        log.info("Cluster is " + (isInitialized() ? "initialized" : "NOT initialized"));
        log.info("Worker is " + (isWorkerRunning() ? "running" : "NOT running"));
        log.info((systemDatabase ? "System database is " : "Database is ")
            + (isDatabaseRunning() ? "running" : "NOT running"));
        log.info(
            "Cluster is " + (isInitialized() && isDatabaseRunning() && isWorkerRunning() ? "running"
                : "NOT running"));
    }

    private void startPipelineConsole() {
        if (isClusterRunning()) {
            String consoleCommand = DirectoryProperties.ziggyBinDir()
                .resolve(RUNJAVA_CONSOLE_COMMAND)
                .toString();
            log.debug("Command line: " + consoleCommand);
            ExternalProcess.simpleExternalProcess(consoleCommand).execute(false);
        } else {
            throw new PipelineException(
                "Cannot start pipeline console when cluster is not running");
        }
    }

    private static void usageAndExit(Options options, Throwable e) {
        Throwable rootCause = e;
        StringBuilder s = new StringBuilder();
        s.append(e.getClass().toString());
        while (rootCause != null) {
            if (s.length() != 0) {
                s.append(": ");
            }
            s.append(rootCause.getMessage());
            rootCause = rootCause.getCause();
        }
        usageAndExit(options, s.length() > 0 ? s.toString() : e != null ? e.toString() : null);
    }

    private static void usageAndExit(Options options, String message) {
        if (message != null) {
            System.err.println(message);
        }
        if (options != null) {
            new HelpFormatter().printHelp("ClusterController [options] command...", COMMAND_HELP,
                options, null);

        }
        System.exit(-1);
    }

    /**
     * Convenience class that allows us to use both the Apache commons CLI command line class and
     * the Apache commons Exec command line class without using the fully-qualified class of either
     * one in all the places it appears. It also provides a specialized method for adding a worker
     * option to the command line.
     *
     * @author PT
     */
    private static class WorkerCommandLine extends org.apache.commons.exec.CommandLine {

        public WorkerCommandLine(String command) {
            super(command);
        }

        @Override
        public WorkerCommandLine addArgument(String command) {
            super.addArgument(command);
            return this;
        }

        public WorkerCommandLine addWorkerOption(String optionName, String optionValue) {
            return addArgument(optionName + "=" + optionValue);
        }
    }
}
