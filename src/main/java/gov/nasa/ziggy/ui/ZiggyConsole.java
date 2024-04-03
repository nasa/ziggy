/*
 * Copyright (C) 2022-2024 United States Government as represented by the Administrator of the
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.PerformanceReport;
import gov.nasa.ziggy.models.ModelRegistryOperations;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.messages.FireTriggerRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.ui.util.proxy.PipelineExecutorProxy;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.TasksStates;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.dispmod.AlertLogDisplayModel;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

/**
 * Top-level front end for Ziggy control. {@link ZiggyConsole} can launch a new instance of
 * {@link ZiggyGuiConsole}, the console GUI, or accept command-line options that can be used to
 * control the pipeline.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */

public class ZiggyConsole {
    private static final Logger log = LoggerFactory.getLogger(ZiggyConsole.class);

    private static final String NAME = "CLI Console";

    private enum Command {
        CANCEL, CONFIG, DISPLAY, HELP, LOG, RESET, RESTART, START, VERSION;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    private enum ConfigType {
        DATA_MODEL_REGISTRY, INSTANCE, PIPELINE, PIPELINE_NODES;
    }

    private enum DisplayType {
        ALERTS, ERRORS, FULL, STATISTICS, STATISTICS_DETAILED;
    }

    private enum ResetType {
        ALL, SUBMITTED;
    }

    // Options
    private static final String CONFIG_TYPE_OPTION = "configType";
    private static final String DISPLAY_TYPE_OPTION = "displayType";
    private static final String ERRORS_OPTION = "errors";
    private static final String HELP_OPTION = "help";
    private static final String INSTANCE_OPTION = "instance";
    private static final String PIPELINE_OPTION = "pipeline";
    private static final String RESET_TYPE_OPTION = "resetType";
    private static final String TASK_OPTION = "task";

    private static final String COMMAND_HELP = """

        Commands:
        cancel                 Cancel running pipelines
        config --configType TYPE [--instance ID | --pipeline NAME]
                               Display pipeline configuration
        display [[--displayType TYPE] --instance ID | --task ID]
                               Display pipeline activity
        log --task ID | --errors
                               Request logs for the given task(s)
        reset --resetType TYPE --instance ID
                               Put tasks in the ERROR state so they can be restarted
        restart --task ID ...  Restart tasks
        start PIPELINE [NAME [START_NODE [STOP_NODE]]]
                               Start the given pipeline and assign its name to NAME
                               (default: NAME is the current time, and the NODES are
                               the first and last nodes of the pipeline respectively)
        version                Display the version (as a Git tag)

        Options:""";

    private static final int HELP_WIDTH = 100;

    // Other constants.
    private static final long MESSAGE_SENT_WAIT_MILLIS = 500;

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public static void main(String[] args) throws Exception {

        Options options = new Options()

            .addOption(Option.builder("c")
                .longOpt(CONFIG_TYPE_OPTION)
                .hasArg()
                .desc(
                    "Configuration type (data-model-registry | instance | pipeline | pipeline-nodes)")
                .build())
            .addOption(Option.builder("d")
                .longOpt(DISPLAY_TYPE_OPTION)
                .desc("Display type (alerts | errors | full | statistics | statistics-detailed)")
                .hasArg()
                .build())
            .addOption(
                Option.builder("e").longOpt(ERRORS_OPTION).desc("Selects all failed tasks").build())
            .addOption(Option.builder("h").longOpt(HELP_OPTION).desc("Show this help").build())
            .addOption(Option.builder("i")
                .longOpt(INSTANCE_OPTION)
                .hasArg()
                .type(Long.class) // if only this did the type checking for us
                .desc("Instance ID")
                .build())
            .addOption(
                Option.builder("p").longOpt(PIPELINE_OPTION).hasArg().desc("Pipeline name").build())
            .addOption(Option.builder("r")
                .longOpt(RESET_TYPE_OPTION)
                .hasArg()
                .desc("Reset type (all | submitted)")
                .build())
            .addOption(Option.builder("t")
                .longOpt(TASK_OPTION)
                .hasArgs()
                .type(Long.class) // if only this did the type checking for us
                .desc("Task ID")
                .build());

        options.addOptionGroup(new OptionGroup()

            .addOption(options.getOption(INSTANCE_OPTION))
            .addOption(options.getOption(PIPELINE_OPTION))
            .addOption(options.getOption(TASK_OPTION)));

        CommandLine cmdLine = null;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            usageAndExit(options, e.getMessage());
        }
        if (cmdLine.hasOption(HELP_OPTION)) {
            usageAndExit(options, (String) null);
        }

        // If no commands, launch the GUI
        List<String> commands = cmdLine.getArgList();
        if (commands.isEmpty()) {
            ZiggyGuiConsole.launch();
            return;
        }

        Command command = null;
        try {
            command = checkForAmbiguousCommand(commands.get(0));
            commands.remove(0);
            new ZiggyConsole().runCommand(command, cmdLine, commands);
        } catch (IllegalArgumentException e) {
            usageAndExit(options, e.getMessage());
        } catch (Exception e) {
            // This is a runtime error so don't show options.
            usageAndExit(e.getMessage(), e);
        }

        System.exit(0);
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
            new HelpFormatter().printHelp(HELP_WIDTH, "ZiggyConsole command [options]",
                COMMAND_HELP, options, null);
        } else if (e != null) {
            log.error(message, e);
        }

        System.exit(-1);
    }

    /**
     * Checks that the possibly abbreviated user command matches 1 and only 1 known command. That
     * command is returned; otherwise, an {@link IllegalArgumentException} is thrown.
     */
    private static Command checkForAmbiguousCommand(String userCommand) {
        Set<Command> commands = new HashSet<>();
        for (Command command : Command.values()) {
            if (command.toString().startsWith(userCommand)) {
                commands.add(command);
            }
        }
        if (commands.size() > 1) {
            throw new IllegalArgumentException(
                "Ambiguous command: " + userCommand + " (could be: " + commands + ")");
        }
        if (commands.size() == 0) {
            throw new IllegalArgumentException("Unknown command " + userCommand);
        }

        return commands.iterator().next();
    }

    /**
     * Starts a {@link ZiggyRmiClient}. This method ensures that the RMI client is no longer needed
     * before allowing the system to shut down. The caller notifies this code that it is done with
     * the client by decrementing the latch that this method returns.
     *
     * @return a countdown latch that should be decremented after the caller no longer needs the
     * client
     */
    private CountDownLatch startZiggyClient() {
        ZiggyRmiClient.start(NAME);

        final CountDownLatch clientStillNeededLatch = new CountDownLatch(1);
        ZiggyShutdownHook.addShutdownHook(() -> {

            // Wait for any messages to be sent before we reset the ZiggyRmiClient.
            try {
                clientStillNeededLatch.await(MESSAGE_SENT_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            ZiggyRmiClient.reset();
        });

        return clientStillNeededLatch;
    }

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    private void runCommand(Command command, CommandLine cmdLine, List<String> commands) {

        Throwable exception = (Throwable) DatabaseTransactionFactory.performTransaction(() -> {
            try {
                switch (command) {
                    case CANCEL -> cancel();
                    case CONFIG -> config(cmdLine);
                    case DISPLAY -> display(cmdLine);
                    case HELP -> throw new IllegalArgumentException("");
                    case LOG -> log();
                    case RESET -> reset(cmdLine);
                    case RESTART -> restart(cmdLine);
                    case START -> start(commands);
                    case VERSION -> System.out.println(ZiggyConfiguration.getInstance()
                        .getString(PropertyName.ZIGGY_VERSION.property()));
                }
            } catch (Throwable e) {
                return e;
            }
            return null;
        });

        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
        if (exception != null) {
            throw new PipelineException(exception);
        }
    }

    private void cancel() {
        List<PipelineInstance> activeInstances = new PipelineInstanceCrud().retrieveAllActive();
        System.out.println("Cancelling running pipelines:");
        for (PipelineInstance instance : activeInstances) {
            System.out.println(" " + instance.getName());
        }
        new PipelineInstanceCrud().cancelAllActive();
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void config(CommandLine cmdLine) {
        switch (parseConfigType(cmdLine)) {
            case DATA_MODEL_REGISTRY:
                System.out.println("Data Model Registry\n");
                ModelRegistryOperations modelMetadataOps = new ModelRegistryOperations();
                System.out.println(modelMetadataOps.report());
                break;
            case INSTANCE:
                System.out.println("Pipeline Instance Configuration(s)\n");
                if (cmdLine.hasOption(INSTANCE_OPTION)) {
                    PipelineInstance instance = pipelineInstance(
                        cmdLine.getOptionValue(INSTANCE_OPTION));
                    System.out.println(new PipelineOperations().generatePedigreeReport(instance));
                } else {
                    for (PipelineInstance instance : new PipelineInstanceCrud().retrieveAll()) {
                        System.out
                            .println(new PipelineOperations().generatePedigreeReport(instance));
                    }
                }
                break;
            case PIPELINE:
                System.out.println("Pipeline Configuration(s)\n");
                List<PipelineDefinition> pipelines;
                if (cmdLine.hasOption(PIPELINE_OPTION)) {
                    pipelines = new PipelineDefinitionCrud()
                        .retrieveAllVersionsForName(cmdLine.getOptionValue(PIPELINE_OPTION));
                } else {
                    pipelines = new PipelineDefinitionCrud().retrieveAll();
                }
                for (PipelineDefinition pipeline : pipelines) {
                    System.out.println(new PipelineOperations().generatePipelineReport(pipeline));
                }
                break;
            case PIPELINE_NODES:
                if (!cmdLine.hasOption(PIPELINE_OPTION)) {
                    throw new IllegalArgumentException("A pipeline name is not specified");
                }
                showPipelineNodes(cmdLine.getOptionValue(PIPELINE_OPTION));
                break;
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private ConfigType parseConfigType(CommandLine cmdLine) {
        ConfigType configType = null;
        if (cmdLine.hasOption(CONFIG_TYPE_OPTION)) {
            try {
                // Allow input to be lowercase and to use dashes instead of underscores.
                configType = ConfigType.valueOf(
                    cmdLine.getOptionValue(CONFIG_TYPE_OPTION).toUpperCase().replace("-", "_"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The config type "
                    + cmdLine.getOptionValue(CONFIG_TYPE_OPTION) + " is not recognized");
            }
        }
        if (configType == null) {
            throw new IllegalArgumentException("The config type is not specified");
        }

        return configType;
    }

    private boolean showPipelineNodes(String pipelineName) {
        DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<Void>() {

            @Override
            public void catchBlock(Throwable e) {
                System.out.println("Unable to retrieve pipeline: " + e);
            }

            @Override
            public Void transaction() throws Exception {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineName);
                if (pipelineDefinition == null) {
                    System.err.println("Pipeline " + pipelineName + " not found");
                    return null;
                }

                System.out.println("Nodes for pipeline " + pipelineName + ":");
                int index = 1;
                for (PipelineDefinitionNode node : pipelineDefinition.getRootNodes()) {
                    index += showPipelineNode(node, index);
                }
                return null;
            }
        });
        return true;
    }

    private int showPipelineNode(PipelineDefinitionNode node, int index) {
        int count = 1;
        System.out.println(String.format("%6d: %s", index, node.getModuleName()));
        for (PipelineDefinitionNode sibling : node.getNextNodes()) {
            count += showPipelineNode(sibling, index + count);
        }

        return count;
    }

    private void display(CommandLine cmdLine) {
        PipelineInstance instance = null;
        PipelineTask task = null;
        if (cmdLine.hasOption(INSTANCE_OPTION)) {
            instance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
        } else if (cmdLine.hasOption(TASK_OPTION)) {
            task = pipelineTask(cmdLine.getOptionValue(TASK_OPTION));
        }
        DisplayType displayType = parseDisplayType(cmdLine);

        boolean displayed = false;
        if (displayType != null) {
            displayed = switch (displayType) {
                case ALERTS -> displayAlert(instance);
                case ERRORS -> displayErrors(instance);
                case STATISTICS -> displayStatistics(instance);
                case STATISTICS_DETAILED -> displayDetailedStatistics(instance);
                default -> false;
            };
        }
        if (displayed) {
            return;
        }

        if (instance != null) {
            InstancesDisplayModel instancesDisplayModel = new InstancesDisplayModel(instance);
            instancesDisplayModel.print(System.out, "Instance Summary");
            displayTaskSummary(instance, displayType == DisplayType.FULL);
        } else if (task != null) {
            ProcessingSummary taskAttr = new ProcessingSummaryOperations()
                .processingSummary(task.getId());
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(task, taskAttr);
            tasksDisplayModel.print(System.out, "Task Summary");
        } else {
            List<PipelineInstance> instances = new PipelineInstanceCrud().retrieveAll();
            new InstancesDisplayModel(instances).print(System.out, "Pipeline Instances");
        }
    }

    private PipelineInstance pipelineInstance(String instanceOption) {
        long id = parseId(instanceOption);

        PipelineInstance instance = new PipelineInstanceCrud().retrieve(id);
        if (instance == null) {
            throw new PipelineException("No instance found with ID " + id);
        }

        return instance;
    }

    private PipelineTask pipelineTask(String taskOption) {
        long id = parseId(taskOption);

        PipelineTask task = new PipelineTaskCrud().retrieve(id);
        if (task == null) {
            throw new PipelineException("No task found with ID " + id);
        }

        return task;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private DisplayType parseDisplayType(CommandLine cmdLine) {
        DisplayType displayType = null;

        if (cmdLine.hasOption(DISPLAY_TYPE_OPTION)) {
            try {
                // Allow input to be lowercase and to use dashes instead of underscores.
                displayType = DisplayType.valueOf(
                    cmdLine.getOptionValue(DISPLAY_TYPE_OPTION).toUpperCase().replace("-", "_"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The display type "
                    + cmdLine.getOptionValue(CONFIG_TYPE_OPTION) + " is not recognized");
            }
        }

        return displayType;
    }

    private boolean displayAlert(PipelineInstance instance) {
        if (instance == null) {
            throw new IllegalArgumentException("An instance is not specified");
        }
        List<AlertLog> alerts = new AlertLogCrud().retrieveForPipelineInstance(instance.getId());
        AlertLogDisplayModel alertLogDisplayModel = new AlertLogDisplayModel(alerts);
        alertLogDisplayModel.print(System.out, "Alerts");
        return true;
    }

    private boolean displayErrors(PipelineInstance instance) {
        if (instance == null) {
            throw new IllegalArgumentException("An instance is not specified");
        }
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveAll(instance,
            PipelineTask.State.ERROR);

        Map<Long, ProcessingSummary> taskAttrs = new ProcessingSummaryOperations()
            .processingSummaries(tasks);

        for (PipelineTask task : tasks) {
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(task,
                taskAttrs.get(task.getId()));
            tasksDisplayModel.print(System.out, "Task Summary");
        }
        return true;
    }

    private boolean displayStatistics(PipelineInstance instance) {
        if (instance == null) {
            throw new IllegalArgumentException("An instance is not specified");
        }
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveTasksForInstance(instance);
        List<String> orderedModuleNames = displayTaskSummary(instance, false).getModuleNames();

        PipelineStatsDisplayModel pipelineStatsDisplayModel = new PipelineStatsDisplayModel(tasks,
            orderedModuleNames);
        pipelineStatsDisplayModel.print(System.out, "Processing Time Statistics");

        TaskMetricsDisplayModel taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks,
            orderedModuleNames);
        taskMetricsDisplayModel.print(System.out,
            "Processing Time Breakdown (completed tasks only)");
        return true;
    }

    private boolean displayDetailedStatistics(PipelineInstance instance) {
        if (instance == null) {
            throw new IllegalArgumentException("An instance is not specified");
        }
        PerformanceReport perfReport = new PerformanceReport(instance.getId(),
            DirectoryProperties.taskDataDir().toFile(), null);
        perfReport.generateReport();
        return true;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private long parseId(String id) {
        try {
            return Long.parseLong(id);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ID " + id + " is not a number");
        }
    }

    private TasksStates displayTaskSummary(PipelineInstance instance, boolean full) {
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveTasksForInstance(instance);
        Map<Long, ProcessingSummary> taskSummaryById = new ProcessingSummaryOperations()
            .processingSummaries(tasks);
        TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel(
            new TasksStates(tasks, taskSummaryById));
        taskSummaryDisplayModel.print(System.out, "Instance Task Summary");

        if (full) {
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(tasks, taskSummaryById);
            tasksDisplayModel.print(System.out, "Pipeline Tasks");
        }

        return taskSummaryDisplayModel.getTaskStates();
    }

    private void log() {
        System.out.println("Not implemented");
        // TODO Implement log retrieval
//        startZiggyClient();
//        if (cmdLine.hasOption(TASK_OPTION)) {
//            PipelineTask task = pipelineTask(cmdLine.getOptionValue(TASK_OPTION));
//            System.out.println("Requesting log from worker...");
//            System.out.println(WorkerTaskLogRequest.requestTaskLog(task));
//        } else if (cmdLine.hasOption(ERRORS_OPTION)) {
//            if (!cmdLine.hasOption(INSTANCE_OPTION)) {
//                throw new IllegalArgumentException("An instance is not specified");
//            }
//            PipelineInstance instance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
//            List<PipelineTask> tasks = new PipelineTaskCrud().retrieveAll(instance,
//                PipelineTask.State.ERROR);
//            for (PipelineTask task : tasks) {
//                System.out.println();
//                System.out.println("Worker log: ");
//                System.out.println(WorkerTaskLogRequest.requestTaskLog(task));
//            }
//        }
    }

    private void reset(CommandLine cmdLine) {
        if (!cmdLine.hasOption(INSTANCE_OPTION)) {
            throw new IllegalArgumentException("An instance is not specified");
        }

        PipelineInstance instance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));

        switch (parseResetType(cmdLine)) {
            case ALL -> resetPipelineInstance(instance, true);
            case SUBMITTED -> resetPipelineInstance(instance, false);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private ResetType parseResetType(CommandLine cmdLine) {
        ResetType resetType = null;
        if (cmdLine.hasOption(RESET_TYPE_OPTION)) {
            try {
                // Allow input to be lowercase and to use dashes instead of underscores.
                resetType = ResetType.valueOf(
                    cmdLine.getOptionValue(RESET_TYPE_OPTION).toUpperCase().replace("-", "_"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The reset type "
                    + cmdLine.getOptionValue(RESET_TYPE_OPTION) + " is not recognized");
            }
        }
        if (resetType == null) {
            throw new IllegalArgumentException("The reset type is not specified");
        }

        return resetType;
    }

    /**
     * Sets the pipeline task state to ERROR for any tasks assigned to this worker that are in the
     * PROCESSING state. This condition indicates that the previous instance of the worker process
     * on this host died abnormally.
     */
    private void resetPipelineInstance(PipelineInstance instance, boolean allStalledTasks) {
        new PipelineTaskCrud().resetTaskStates(instance.getId(), allStalledTasks);
    }

    private void restart(CommandLine cmdLine) {
        if (!cmdLine.hasOption(TASK_OPTION)) {
            throw new IllegalArgumentException("One or more tasks are not specified");
        }

        CountDownLatch messageSentLatch = startZiggyClient();

        Collection<Long> taskIds = new ArrayList<>();
        for (String taskId : cmdLine.getOptionValues(TASK_OPTION)) {
            taskIds.add(parseId(taskId));
        }

        DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<Void>() {
            @Override
            public void catchBlock(Throwable e) {
                System.out.println("Unable to restart tasks: " + e);
            }

            @Override
            public Void transaction() throws Exception {
                List<PipelineTask> tasks = new PipelineTaskCrud().retrieveAll(taskIds);
                List<Long> missingTasks = new ArrayList<>();
                for (long taskId : taskIds) {
                    boolean found = false;
                    for (PipelineTask task : tasks) {
                        if (task.getId() == taskId) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        missingTasks.add(taskId);
                    }
                }
                if (missingTasks.size() > 0) {
                    System.out.println("Tasks not found with the following IDs: " + missingTasks);
                    return null;
                }

                new PipelineExecutorProxy().restartTasks(tasks, RunMode.RESTART_FROM_BEGINNING,
                    messageSentLatch);

                return null;
            }
        });
    }

    private void start(List<String> commands) {
        if (commands.size() < 1) {
            throw new IllegalArgumentException("A pipeline name is not specified");
        }

        CountDownLatch messageSentLatch = startZiggyClient();

        String pipelineName = commands.get(0);
        String instanceName = commands.size() > 1 ? commands.get(1) : null;
        String startNodeName = commands.size() > 2 ? commands.get(2) : null;
        String stopNodeName = commands.size() > 3 ? commands.get(3) : null;

        System.out.println(String.format("Launching %s: name=%s, start=%s, stop=%s...",
            pipelineName, instanceName, startNodeName != null ? startNodeName : "",
            stopNodeName != null ? stopNodeName : ""));
        ZiggyMessenger.publish(
            new FireTriggerRequest(pipelineName, instanceName, startNodeName, stopNodeName, 1, 0),
            messageSentLatch);
        System.out.println(String.format("Launching %s: name=%s, start=%s, stop=%s...done",
            pipelineName, instanceName, startNodeName != null ? startNodeName : "",
            stopNodeName != null ? stopNodeName : ""));
    }
}
