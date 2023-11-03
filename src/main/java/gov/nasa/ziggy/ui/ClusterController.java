/*
 * Copyright (C) 2022-2023 United States Government as represented by the Administrator of the
 * National Aeronautics and Space Administration. All Rights Reserved.
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

import static gov.nasa.ziggy.supervisor.PipelineSupervisor.supervisorCommand;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.exec.CommandLine;
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
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseController;
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.events.ZiggyEventHandlerDefinitionImporter;
import gov.nasa.ziggy.services.messages.ShutdownMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.WrapperUtils.WrapperCommand;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Tool to manage the pipeline cluster (here "cluster" means the combination of a database instance,
 * a datastore directory, and a supervisor process). The available options are:
 * <ol>
 * <li>Initialize the cluster: this means clearing out the existing database and datastore and
 * constructing a fresh database from the schema, plus populating the parameters, data types, and
 * pipeline definitions.
 * <li>Start the cluster: this means updating the Java heap space available for the workers,
 * starting the supervisor, and starting the database.
 * <li>Stop the cluster: this means stopping the workers, stopping the supervisor, and stopping the
 * database.
 * <li>Check cluster status: check that the cluster is initialized and that both the supervisor and
 * the database are running (or available in the case of the database).
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
 * <li>Specify that database name via the {@link PropertyName#DATABASE_NAME} property in the
 * properties file.
 * <li>Ensure that the {@link PropertyName#DATABASE_DIR} property is empty or missing.
 * </ol>
 * <p>
 * If the database name is not a supported database, the user will also have to do the following:
 * <ol>
 * <li>Ensure that the {@link PropertyName#DATABASE_SOFTWARE} property is empty, missing, or
 * contains a string not in the list of supported databases.
 * <li>Specify the database SQL dialect via the {@link PropertyName#HIBERNATE_DIALECT} property in
 * the properties file.
 * <li>Specify the fully-qualified Java class to use as a driver for communication with the database
 * via the {@link PropertyName#HIBERNATE_DRIVER} property in the properties file. The driver must be
 * on Ziggy's classpath.
 * <li>Update the build to create a schema for the database.
 * <li>Manually load this database schema, as well as the pipeline definitions by calling ziggy
 * p*import.
 * </ol>
 *
 * @author PT
 * @author Bill Wohler
 */
public class ClusterController {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);
    private static final String NAME = "ClusterController";

    // Parameters related to cluster initialization
    private static final String PARAM_LIBRARY_PREFIX = "pl-";
    private static final String TYPE_FILE_PREFIX = "pt-";
    private static final String PIPELINE_DEF_FILE_PREFIX = "pd-";
    private static final String EVENT_HANDLER_DEF_FILE_PREFIX = "pe-";
    private static final String XML_SUFFIX = ".xml";

    // Parameters related to the supervisor process
    private static final int WORKER_HEAP_SIZE_DEFAULT = 16000;
    private static final int WORKER_THREAD_COUNT_DEFAULT = 1;
    private static final int WORKER_COUNT_ALL_CORES = 0; // use number of cores

    private static final String ZIGGY_CONSOLE_COMMAND = "ziggy console";

    // Cluster options
    private static final String FORCE_OPTION = "force";
    private static final String HELP_OPTION = "help";
    private static final String WORKER_HEAP_SIZE_OPTION = "workerHeapSize";
    private static final String WORKER_COUNT_OPTION = "workerCount";

    // Commands
    private static final String INIT_COMMAND = "init";
    private static final String START_COMMAND = "start";
    private static final String STOP_COMMAND = "stop";
    private static final String STATUS_COMMAND = "status";
    private static final String CONSOLE_COMMAND = "console";
    private static final String VERSION_COMMAND = "version";
    private static final List<String> ALL_COMMANDS = ImmutableList.of(INIT_COMMAND, START_COMMAND,
        STOP_COMMAND, STATUS_COMMAND, CONSOLE_COMMAND, VERSION_COMMAND);

    // Uncategorized constants
    private static final long DATABASE_SETTLE_MILLIS = 4000L;
    private static final String COMMAND_HELP = """

        Commands:
        init                    Initialize the cluster
        start                   Start the cluster
        stop                    Stop the cluster
        status                  Check cluster status
        console                 Start pipeline console GUI
        version                 Display the version (as a Git tag)

        Options:""";

    private final DatabaseController databaseController;

    private final int workerHeapSize;
    private final int workerCount;

    public ClusterController(int workerHeapSize, int workerCount) {
        databaseController = DatabaseController.newInstance();
        this.workerHeapSize = workerHeapSize;
        this.workerCount = workerCount != WORKER_COUNT_ALL_CORES ? workerCount : cpuCount();
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
            .addOption(Option.builder("h").longOpt(HELP_OPTION).desc("Show this help").build())
            .addOption(Option.builder()
                .longOpt(WORKER_HEAP_SIZE_OPTION)
                .hasArg()
                .desc("Total heap size used by all workers (MB)")
                .build())
            .addOption(Option.builder()
                .longOpt(WORKER_COUNT_OPTION)
                .hasArg()
                .desc("Number of worker threads or 0 to use all cores")
                .build());

        org.apache.commons.cli.CommandLine cmdLine = null;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            usageAndExit(options, e.getMessage());
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
            ImmutableConfiguration config = ZiggyConfiguration.getInstance();
            if (cmdLine.hasOption(WORKER_HEAP_SIZE_OPTION)) {
                workerHeapSize = Integer.parseInt(cmdLine.getOptionValue(WORKER_HEAP_SIZE_OPTION));
            } else {
                workerHeapSize = config.getInt(PropertyName.WORKER_HEAP_SIZE.property(),
                    WORKER_HEAP_SIZE_DEFAULT);
            }
            if (cmdLine.hasOption(WORKER_COUNT_OPTION)) {
                workerThreadCount = Integer.parseInt(cmdLine.getOptionValue(WORKER_COUNT_OPTION));
            } else {
                workerThreadCount = config.getInt(PropertyName.WORKER_COUNT.property(),
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
                usageAndExit("Failed to initialize cluster", e);
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

        if (commands.contains(VERSION_COMMAND)) {
            log.info(
                ZiggyConfiguration.getInstance().getString(PropertyName.ZIGGY_VERSION.property()));
        }
    }

    private static List<String> unrecognizedCommands(List<String> commands) {
        return commands.stream()
            .filter(s -> !ALL_COMMANDS.contains(s))
            .collect(Collectors.toList());
    }

    private void initializeCluster(boolean force) throws Exception {
        if (isSupervisorRunning()
            || databaseController != null && databaseController.status() == 0) {
            throw new PipelineException("Cannot reinitialize cluster when cluster is running");
        }
        if (isInitialized() && !force) {
            throw new PipelineException(
                "Cannot re-initialize an initialized cluster without --force option");
        }

        if (databaseController != null) {
            if (databaseController.isSystemDatabase()) {
                log.info("Deleting contents of existing database");
                databaseController.dropDatabase();
                log.info("Deleting contents of existing database...done");
            } else {
                log.info("Deleting existing database directory {}",
                    DirectoryProperties.databaseDir());
                FileUtil.deleteDirectoryTree(DirectoryProperties.databaseDir(), true);
                log.info("Deleting existing database directory...done");
            }
        }

        log.info("Deleting existing datastore directory");
        FileUtil.deleteDirectoryTree(DirectoryProperties.datastoreRootDir(), true);
        log.info("Deleting existing datastore directory...done");

        if (databaseController != null) {
            log.info("Creating database");
            databaseController.createDatabase();
            log.info("Creating database...done");
        }

        log.info("Creating datastore directory");
        if (!DirectoryProperties.datastoreRootDir().toFile().mkdirs()) {
            throw new PipelineException(
                "Unable to create directory defined by " + PropertyName.DATASTORE_ROOT_DIR);
        }
        log.info("Creating datastore directory...done");

        int dbStartRetCode = databaseController != null ? databaseController.start()
            : DatabaseController.NOT_SUPPORTED;
        if (dbStartRetCode == DatabaseController.NOT_SUPPORTED) {
            log.info("Database should be available");
        } else if (dbStartRetCode != 0) {
            throw new PipelineException("Unable to start initialized database");
        }

        log.info("Creating utility pipeline modules...");
        createUtilityPipelineModules();
        log.info("Creating utility pipeline modules...done");

        DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<Void>() {
            @Override
            public Void transaction() throws Exception {

                // Import the parameters, data types, and pipelines.
                Path pipelineDefsDir = DirectoryProperties.pipelineDefinitionDir();
                if (!Files.exists(pipelineDefsDir)) {
                    throw new PipelineException("Pipeline definitions directory "
                        + pipelineDefsDir.toString() + " does not exist");
                }

                log.info(
                    "Importing parameter libraries from directory " + pipelineDefsDir.toString());
                File[] parameterFiles = pipelineDefsDir.toFile()
                    .listFiles(
                        (FilenameFilter) (dir, name) -> (name.startsWith(PARAM_LIBRARY_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(parameterFiles, Comparator.comparing(File::getName));
                ParametersOperations paramsOps = new ParametersOperations();
                for (File parameterFile : parameterFiles) {
                    log.info("Importing library " + parameterFile.getName());
                    paramsOps.importParameterLibrary(parameterFile, null, ParamIoMode.STANDARD);
                }

                log.info("Importing data file types from directory " + pipelineDefsDir.toString());
                File[] dataTypeFiles = pipelineDefsDir.toFile()
                    .listFiles((FilenameFilter) (dir,
                        name) -> (name.startsWith(TYPE_FILE_PREFIX) && name.endsWith(XML_SUFFIX)));
                Arrays.sort(dataTypeFiles, Comparator.comparing(File::getName));
                List<String> dataTypeFileNames = new ArrayList<>(dataTypeFiles.length);
                for (File dataTypeFile : dataTypeFiles) {
                    log.info("Adding " + dataTypeFile.getName() + " to imports list");
                    dataTypeFileNames.add(dataTypeFile.getAbsolutePath());
                }
                new DataFileTypeImporter(dataTypeFileNames, false).importFromFiles();

                log.info(
                    "Importing pipeline definitions from directory " + pipelineDefsDir.toString());
                File[] pipelineDefinitionFiles = pipelineDefsDir.toFile()
                    .listFiles(
                        (FilenameFilter) (dir, name) -> (name.startsWith(PIPELINE_DEF_FILE_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(pipelineDefinitionFiles, Comparator.comparing(File::getName));
                List<File> pipelineDefFileList = new ArrayList<>();
                for (File pipelineDefinitionFile : pipelineDefinitionFiles) {
                    log.info("Adding " + pipelineDefinitionFile.getName() + " to imports list");
                    pipelineDefFileList.add(pipelineDefinitionFile);
                }
                new PipelineDefinitionOperations().importPipelineConfiguration(pipelineDefFileList);

                log.info(
                    "Importing event definitions from directory " + pipelineDefsDir.toString());
                File[] handlerDefinitionFiles = pipelineDefsDir.toFile()
                    .listFiles((FilenameFilter) (dir,
                        name) -> (name.startsWith(EVENT_HANDLER_DEF_FILE_PREFIX)
                            && name.endsWith(XML_SUFFIX)));
                Arrays.sort(handlerDefinitionFiles, Comparator.comparing(File::getName));
                new ZiggyEventHandlerDefinitionImporter(handlerDefinitionFiles).importFromFiles();

                return null;
            }

            @Override
            public void finallyBlock() {
                if (databaseController != null) {
                    databaseController.stop();
                }
            }
        });
        log.info("Database initialization and creation complete");
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
                .merge(DataReceiptPipelineModule.createDataReceiptPipelineForDb());
            return null;
        });
    }

    private boolean isInitialized() {
        return Files.exists(DirectoryProperties.datastoreRootDir());
    }

    private boolean isClusterRunning() {
        return isDatabaseAvailable() && isSupervisorRunning();
    }

    /**
     * Returns false if we know for sure the database is not running; true if we know for sure or
     * don't know.
     */
    public boolean isDatabaseAvailable() {
        return switch (databaseStatus()) {
            case 0 -> true;
            case DatabaseController.NOT_SUPPORTED -> true;
            default -> false;
        };
    }

    public int databaseStatus() {
        return databaseController != null ? databaseController.status()
            : DatabaseController.NOT_SUPPORTED;
    }

    public boolean isSupervisorRunning() {
        CommandLine supervisorStatusCommand = supervisorCommand(WrapperCommand.STATUS, workerCount,
            workerHeapSize);
        log.debug("Command line: " + supervisorStatusCommand);
        return ExternalProcess.simpleExternalProcess(supervisorStatusCommand).execute() == 0;
    }

    /**
     * Waits the given number of milliseconds for a process to settle. Some processes, like
     * Postgres, will shut down and exit if they are pinged too soon.
     */
    private void waitForProcessToSettle(long millis) {
        try {
            log.debug("Waiting for process to settle");
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startCluster(boolean force) {

        if (isClusterRunning()) {
            throw new PipelineException("Cannot start cluster; cluster already running");
        }
        if (!isInitialized()) {
            if (!force) {
                throw new PipelineException("Cannot start cluster; cluster not initialized");
            }
            log.warn("Attempting to start uninitialized cluster");
        }

        try {
            if (isDatabaseAvailable()) {
                log.info("Database available");
            } else {
                log.info("Starting database");
                if (databaseController == null) {
                    log.info("Not starting an unsupported database");
                } else {
                    int dbStartStatus = databaseController.start();
                    if (dbStartStatus == 0) {
                        waitForProcessToSettle(DATABASE_SETTLE_MILLIS);
                        log.info("Database started");
                    } else if (dbStartStatus == DatabaseController.NOT_SUPPORTED) {
                        log.info("Not starting a system database");
                    } else {
                        throw new PipelineException("Could not start database");
                    }
                }
            }

            if (isSupervisorRunning()) {
                log.info("Supervisor already running");
            } else {
                log.info("Starting supervisor");
                log.debug("Creating directory " + DirectoryProperties.supervisorLogDir());
                Files.createDirectories(DirectoryProperties.supervisorLogDir());
                CommandLine supervisorStartCommand = supervisorCommand(WrapperCommand.START,
                    workerCount, workerHeapSize);
                log.debug("Command line: " + supervisorStartCommand.toString());
                ExternalProcess.simpleExternalProcess(supervisorStartCommand)
                    .exceptionOnFailure()
                    .execute();
                log.info("Supervisor started");
            }
            log.info("Cluster started");
        } catch (Throwable t) {
            log.error("Caught exception when trying to start cluster, shutting down", t);
            stopCluster();
            throw new PipelineException("Unable to start cluster", t);
        }
    }

    private void stopCluster() {
        // Start RMI in order to publish the shutdown message.
        int rmiPort = ZiggyConfiguration.getInstance()
            .getInt(PropertyName.SUPERVISOR_PORT.property(), ZiggyRmiServer.RMI_PORT_DEFAULT);
        log.info("Starting ZiggyRmiClient instance with registry on port " + rmiPort);
        try {
            ZiggyRmiClient.initializeInstance(rmiPort, NAME);
            log.info("Starting ZiggyRmiClient instance...done");
            ZiggyMessenger.publish(new ShutdownMessage());
        } catch (PipelineException e) {
            log.info("Starting ZiggyRmiClient instance...(no server to talk to)");
        }

        log.info("Supervisor stopping");
        CommandLine supervisorStopCommand = supervisorCommand(WrapperCommand.STOP, workerCount,
            workerHeapSize);
        log.debug("Command line: " + supervisorStopCommand.toString());
        ExternalProcess.simpleExternalProcess(supervisorStopCommand).execute(true);
        if (!isSupervisorRunning()) {
            log.info("Supervisor stopped");
        } else {
            log.error("Unable to stop supervisor process!");
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
        ZiggyRmiClient.reset();
        log.info("Cluster stopped");

        // Force exit due to the RMI client.
        System.exit(0);
    }

    private void status() {
        int databaseStatus = databaseStatus();
        log.info("Cluster is " + (isInitialized() ? "initialized" : "NOT initialized"));
        log.info("Supervisor is " + (isSupervisorRunning() ? "running" : "NOT running"));
        log.info("Database "
            + (databaseStatus == 0 ? "is"
                : databaseStatus == DatabaseController.NOT_SUPPORTED ? "should be" : "is NOT")
            + " available");
        log.info("Cluster is "
            + (isInitialized() && isDatabaseAvailable() && isSupervisorRunning() ? "running"
                : "NOT running"));
    }

    private void startPipelineConsole() {
        if (!isClusterRunning()) {
            throw new PipelineException(
                "Cannot start pipeline console when cluster is not running");
        }
        String consoleCommand = DirectoryProperties.ziggyBinDir()
            .resolve(ZIGGY_CONSOLE_COMMAND)
            .toString();
        log.debug("Command line: " + consoleCommand);
        ExternalProcess.simpleExternalProcess(consoleCommand).execute(false);
    }

    private static void usageAndExit(Options options, String message) {
        usageAndExit(options, message, null);
    }

    private static void usageAndExit(String message, Throwable e) {
        usageAndExit(null, message, e);
    }

    private static void usageAndExit(Options options, String message, Throwable e) {
        // Until we've gotten through argument parsing, emit errors to stderr. Once we start the
        // program, we'll be logging and throwing exceptions.
        if (options != null) {
            if (message != null) {
                System.err.println(message);
            }
            new HelpFormatter().printHelp("ClusterController [options] command...", COMMAND_HELP,
                options, null);
        } else if (e != null) {
            log.error(message, e);
        }

        System.exit(-1);
    }
}
