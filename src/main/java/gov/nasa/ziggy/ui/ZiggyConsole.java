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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.StartPipelineRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.ui.util.TaskHalter;
import gov.nasa.ziggy.ui.util.TaskRestarter;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.ZiggyStringUtils;
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
        CONFIG, DISPLAY, HALT, HELP, LOG, RESTART, START, VERSION;

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

    // Options
    private static final String CONFIG_TYPE_OPTION = "configType";
    private static final String DISPLAY_TYPE_OPTION = "displayType";
    private static final String ERRORS_OPTION = "errors";
    private static final String HELP_OPTION = "help";
    private static final String INSTANCE_OPTION = "instance";
    private static final String PIPELINE_OPTION = "pipeline";
    private static final String RESTART_MODE_OPTION = "restartMode";
    private static final String TASK_OPTION = "task";

    private static final String COMMAND_HELP = """

        Commands:
        config --configType TYPE [--instance ID | --pipeline NAME]
                               Display pipeline configuration
        display [[--displayType TYPE] --instance ID | --task ID]
                               Display pipeline activity
        halt [--instance ID | --task ID ...]
                               Halts the given task(s) or all incomplete tasks in the given instance
        log --task ID | --errors
                               Request logs for the given task(s)
        restart [--restartMode MODE] [--instance ID | --task ID ...]
                               Restarts the given task(s) or all halted tasks in the given instance
        start PIPELINE [NAME [START_NODE [STOP_NODE]]]
                               Start the given pipeline and assign its name to NAME
                               (default: NAME is the current time, and the NODES are
                               the first and last nodes of the pipeline respectively)
        version                Display the version (as a Git tag)

        Options:""";

    private static final int HELP_WIDTH = 100;

    // Other constants.
    private static final long MESSAGE_SENT_WAIT_MILLIS = 500;

    private final AlertLogOperations alertLogOperations = new AlertLogOperations();
    private final ModelOperations modelOperations = new ModelOperations();
    private final PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private final PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

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
                .longOpt(RESTART_MODE_OPTION)
                .hasArg()
                .desc("Restart mode (" + runModes().stream().collect(Collectors.joining(", "))
                    + "; default: restart-from-beginning)")
                .build())
            .addOption(Option.builder("t")
                .longOpt(TASK_OPTION)
                .hasArgs()
                .type(Long.class) // if only this did the type checking for us
                .desc("Comma-separated list of task IDs and ranges")
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
        return startZiggyClient(1);
    }

    /**
     * Starts a {@link ZiggyRmiClient}. This method ensures that the RMI client is no longer needed
     * before allowing the system to shut down. The caller notifies this code that it is done with
     * the client by decrementing the latch that this method returns the given number of times.
     *
     * @param latchCount the number of latches to create
     * @return a countdown latch that should be decremented after the caller no longer needs the
     * client
     */
    private CountDownLatch startZiggyClient(int latchCount) {
        ZiggyRmiClient.start(NAME);

        final CountDownLatch clientStillNeededLatch = new CountDownLatch(latchCount);
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

        Throwable exception = null;
        try {
            switch (command) {
                case CONFIG -> config(cmdLine);
                case DISPLAY -> display(cmdLine);
                case HALT -> halt(cmdLine);
                case HELP -> throw new IllegalArgumentException("");
                case LOG -> log();
                case RESTART -> restart(cmdLine);
                case START -> start(commands);
                case VERSION -> System.out.println(BuildInfo.ziggyVersion());
            }
        } catch (Throwable e) {
            exception = e;
        }

        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
        if (exception != null) {
            throw new PipelineException(exception);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void config(CommandLine cmdLine) {
        switch (parseConfigType(cmdLine)) {
            case DATA_MODEL_REGISTRY:
                System.out.println("Data Model Registry\n");
                System.out.println(modelOperations().report());
                break;
            case INSTANCE:
                System.out.println("Pipeline Instance Configuration(s)\n");
                if (cmdLine.hasOption(INSTANCE_OPTION)) {
                    PipelineInstance instance = pipelineInstance(
                        cmdLine.getOptionValue(INSTANCE_OPTION));
                    System.out
                        .println(new PipelineReportGenerator().generatePedigreeReport(instance));
                } else {
                    for (PipelineInstance instance : pipelineInstanceOperations()
                        .pipelineInstances()) {
                        System.out.println(
                            new PipelineReportGenerator().generatePedigreeReport(instance));
                    }
                }
                break;
            case PIPELINE:
                System.out.println("Pipeline Configuration(s)\n");
                List<PipelineDefinition> pipelines;
                if (cmdLine.hasOption(PIPELINE_OPTION)) {
                    pipelines = pipelineDefinitionOperations()
                        .allPipelineDefinitionsForName(cmdLine.getOptionValue(PIPELINE_OPTION));
                } else {
                    pipelines = pipelineDefinitionOperations().pipelineDefinitions();
                }
                for (PipelineDefinition pipeline : pipelines) {
                    System.out
                        .println(new PipelineReportGenerator().generatePipelineReport(pipeline));
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

        try {

            PipelineDefinition pipelineDefinition = pipelineDefinitionOperations()
                .pipelineDefinition(pipelineName);
            if (pipelineDefinition == null) {
                System.err.println("Pipeline " + pipelineName + " not found");
                return false;
            }

            System.out.println("Nodes for pipeline " + pipelineName + ":");
            int index = 1;
            for (PipelineDefinitionNode node : pipelineDefinition.getRootNodes()) {
                index += showPipelineNode(node, index);
            }
        } catch (Throwable e) {
            System.out.println("Unable to retrieve pipeline: " + e);
        }
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
        PipelineTaskDisplayData task = null;
        if (cmdLine.hasOption(INSTANCE_OPTION)) {
            instance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
        } else if (cmdLine.hasOption(TASK_OPTION)) {
            task = pipelineTask(cmdLine.getOptionValue(TASK_OPTION));
        }
        DisplayType displayType = parseDisplayType(cmdLine);

        boolean displayed = false;
        if (displayType != null) {
            if (instance == null) {
                throw new IllegalArgumentException("An instance is not specified");
            }
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
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(task);
            tasksDisplayModel.print(System.out, "Task Summary");
        } else {
            List<PipelineInstance> instances = pipelineInstanceOperations().pipelineInstances();
            new InstancesDisplayModel(instances).print(System.out, "Pipeline Instances");
        }
    }

    private PipelineInstance pipelineInstance(String instanceOption) {
        long id = parseId(instanceOption);

        PipelineInstance instance = pipelineInstanceOperations().pipelineInstance(id);
        if (instance == null) {
            throw new PipelineException("No instance found with ID " + id);
        }

        return instance;
    }

    private PipelineTaskDisplayData pipelineTask(String taskOption) {
        return pipelineTask(parseId(taskOption));
    }

    private PipelineTaskDisplayData pipelineTask(long id) {
        PipelineTaskDisplayData task = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(pipelineTaskOperations().pipelineTask(id));
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
                    + cmdLine.getOptionValue(DISPLAY_TYPE_OPTION) + " is not recognized");
            }
        }

        return displayType;
    }

    private boolean displayAlert(PipelineInstance instance) {
        List<AlertLog> alerts = alertLogOperations().alertLogs(instance);
        AlertLogDisplayModel alertLogDisplayModel = new AlertLogDisplayModel(alerts);
        alertLogDisplayModel.print(System.out, "Alerts");
        return true;
    }

    private boolean displayErrors(PipelineInstance instance) {
        List<PipelineTaskDisplayData> tasks = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(pipelineTaskDataOperations().erroredPipelineTasks(instance));

        for (PipelineTaskDisplayData task : tasks) {
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(task);
            tasksDisplayModel.print(System.out, "Task Summary");
        }
        return true;
    }

    private boolean displayStatistics(PipelineInstance instance) {
        List<PipelineTaskDisplayData> tasks = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(instance);
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

    private TaskCounts displayTaskSummary(PipelineInstance instance, boolean full) {
        List<PipelineTaskDisplayData> tasks = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(instance);
        TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel(
            new TaskCounts(tasks));
        taskSummaryDisplayModel.print(System.out, "Instance Task Summary");

        if (full) {
            TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(tasks);
            tasksDisplayModel.print(System.out, "Pipeline Tasks");
        }

        return taskSummaryDisplayModel.getTaskCounts();
    }

    private void halt(CommandLine cmdLine) {
        checkArgument(cmdLine.hasOption(INSTANCE_OPTION) || cmdLine.hasOption(TASK_OPTION),
            "One or more tasks or an instance are not specified");

        // Get the list of tasks, if present.
        List<Long> taskIds = new ArrayList<>();
        if (cmdLine.hasOption(TASK_OPTION)) {
            for (String taskIdOption : cmdLine.getOptionValues(TASK_OPTION)) {
                taskIds.addAll(ZiggyStringUtils.extractNumericRange(taskIdOption));
            }
        }

        // Get instance. First try getting instance option. If missing, get the instance from the
        // first task.
        PipelineInstance pipelineInstance;
        if (cmdLine.hasOption(INSTANCE_OPTION)) {
            pipelineInstance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
        } else {
            pipelineInstance = pipelineTaskOperations().pipelineInstance(taskIds.get(0));
        }

        System.out.println("Halting task(s) "
            + taskIds.stream().map(Object::toString).collect(Collectors.joining(", "))
            + " in instance " + pipelineInstance.getId());
        CountDownLatch messageSentLatch = startZiggyClient();
        new TaskHalter().haltTasks(pipelineInstance,
            taskIds.stream()
                .map(id -> pipelineTaskOperations().pipelineTask(id))
                .collect(Collectors.toList()));
        messageSentLatch.countDown();
    }

    private void log() {
        System.out.println("Not implemented");
        // TODO Implement log retrieval
//        CountDownLatch messageSentLatch = startZiggyClient();
//        if (cmdLine.hasOption(TASK_OPTION)) {
//            PipelineTask task = pipelineTask(cmdLine.getOptionValue(TASK_OPTION));
//            System.out.println("Requesting log from worker...");
//            System.out.println(WorkerTaskLogRequest.requestTaskLog(task));
//        } else if (cmdLine.hasOption(ERRORS_OPTION)) {
//            if (!cmdLine.hasOption(INSTANCE_OPTION)) {
//                throw new IllegalArgumentException("An instance is not specified");
//            }
//            PipelineInstance instance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
//            List<PipelineTask> tasks = pipelineTaskOperations().erroredPipelineTasks(instance);
//            for (PipelineTask task : tasks) {
//                System.out.println();
//                System.out.println("Worker log: ");
//                System.out.println(WorkerTaskLogRequest.requestTaskLog(task));
//            }
//        }
    }

    private void restart(CommandLine cmdLine) {
        checkArgument(cmdLine.hasOption(INSTANCE_OPTION) || cmdLine.hasOption(TASK_OPTION),
            "One or more tasks or an instance are not specified");

        // Get the list of tasks, if present.
        List<Long> taskIds = new ArrayList<>();
        if (cmdLine.hasOption(TASK_OPTION)) {
            for (String taskIdOption : cmdLine.getOptionValues(TASK_OPTION)) {
                taskIds.addAll(ZiggyStringUtils.extractNumericRange(taskIdOption));
            }
        }

        // Get instance. First try getting instance option. If missing, get the instance from the
        // first task.
        PipelineInstance pipelineInstance;
        if (cmdLine.hasOption(INSTANCE_OPTION)) {
            pipelineInstance = pipelineInstance(cmdLine.getOptionValue(INSTANCE_OPTION));
        } else {
            pipelineInstance = pipelineTaskOperations().pipelineInstance(taskIds.get(0));
        }

        // Get the run mode, using Restart from beginning if none provided.
        RunMode runMode = parseRunMode(cmdLine);
        System.out.println("Restarting task(s) "
            + taskIds.stream().map(Object::toString).collect(Collectors.joining(", "))
            + " in instance " + pipelineInstance.getId() + " with restart mode " + runMode);

        // Create two latches to wait for the restart messages.
        CountDownLatch clientStillNeededLatch = startZiggyClient(2);
        List<PipelineTask> pipelineTasks = taskIds.stream()
            .map(id -> pipelineTaskOperations().pipelineTask(id))
            .collect(Collectors.toList());
        new TaskRestarter().restartTasks(pipelineInstance, pipelineTasks, runMode,
            clientStillNeededLatch);
    }

    /**
     * Parses the optional run mode. If the option is not provided,
     * {@link RunMode#compareTo(RunMode) is returned. If the option is provided, the value can be
     * abbreviated, but must match 1 and only 1 known run mode. That run mode is returned;
     * otherwise, an {@link IllegalArgumentException} is thrown.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private RunMode parseRunMode(CommandLine cmdLine) {
        if (!cmdLine.hasOption(RESTART_MODE_OPTION)) {
            return RunMode.RESTART_FROM_BEGINNING;
        }

        String runModeInput = cmdLine.getOptionValue(RESTART_MODE_OPTION);
        Set<String> runModes = new HashSet<>();
        for (String runMode : runModes()) {
            if (runMode.startsWith(runModeInput)) {
                runModes.add(runMode);
            }
        }
        if (runModes.size() > 1) {
            throw new IllegalArgumentException("Ambiguous restart mode: " + runModeInput
                + " (could be: " + runModes.stream().collect(Collectors.joining(", ")) + ")");
        }
        if (runModes.size() == 0) {
            throw new IllegalArgumentException("Unknown restart mode " + runModeInput);
        }

        return RunMode.valueOf(runModes.iterator().next().toUpperCase().replace("-", "_"));
    }

    /**
     * Returns a list of the run modes in lowercase using dashes instead of underscores that the
     * user is expected to use. The standard mode is omitted.
     */
    private static List<String> runModes() {
        List<RunMode> runModes = new ArrayList<>(Arrays.asList(RunMode.values()));
        runModes.remove(RunMode.STANDARD);
        return runModes.stream()
            .map(Enum::name)
            .map(String::toLowerCase)
            .map(s -> s.replace("_", "-"))
            .collect(Collectors.toList());
    }

    private void start(List<String> commands) {
        checkArgument(commands.size() > 0, "A pipeline name is not specified");

        CountDownLatch messageSentLatch = startZiggyClient();

        String pipelineName = commands.get(0);
        String instanceName = commands.size() > 1 ? commands.get(1) : null;
        String startNodeName = commands.size() > 2 ? commands.get(2) : null;
        String stopNodeName = commands.size() > 3 ? commands.get(3) : null;

        System.out.println(String.format("Launching %s: name=\"%s\", start=\"%s\", stop=\"%s\"...",
            pipelineName, instanceName, startNodeName != null ? startNodeName : "",
            stopNodeName != null ? stopNodeName : ""));
        ZiggyMessenger.publish(
            new StartPipelineRequest(pipelineName, instanceName, startNodeName, stopNodeName, 1, 0),
            messageSentLatch);
    }

    private AlertLogOperations alertLogOperations() {
        return alertLogOperations;
    }

    private ModelOperations modelOperations() {
        return modelOperations;
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    private PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    private PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    private PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }
}
