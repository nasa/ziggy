package gov.nasa.ziggy.supervisor;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_CLASSPATH_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_LOG_FILE_PROP_NAME;
import static gov.nasa.ziggy.util.WrapperUtils.wrapperParameter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricsDumper;
import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.events.ZiggyEventOperations;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.messages.EventHandlerRequest;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
import gov.nasa.ziggy.services.messages.RemoveTaskFromKilledTasksMessage;
import gov.nasa.ziggy.services.messages.RestartTasksRequest;
import gov.nasa.ziggy.services.messages.RetryTransitionRequest;
import gov.nasa.ziggy.services.messages.SingleTaskLogMessage;
import gov.nasa.ziggy.services.messages.SingleTaskLogRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.TaskHaltedMessage;
import gov.nasa.ziggy.services.messages.TaskLogInformationMessage;
import gov.nasa.ziggy.services.messages.TaskLogInformationRequest;
import gov.nasa.ziggy.services.messages.UpdateProcessingStepMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesRequest;
import gov.nasa.ziggy.services.messages.ZiggyEventHandlerInfoMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.services.process.ExternalProcessUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.WrapperUtils;
import gov.nasa.ziggy.util.WrapperUtils.WrapperCommand;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.WorkerResources;
import hdf.hdf5lib.H5;

/**
 * Establishes the pipeline supervisor instance. The supervisor instance is a process that runs as a
 * daemon and provides central services for a data analysis pipeline (queueing and dispatch of task
 * requests, monitoring of algorithm execution, response to requests from console instances, etc.).
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class PipelineSupervisor extends AbstractPipelineProcess {

    private static final Logger log = LoggerFactory.getLogger(PipelineSupervisor.class);

    public static final String NAME = "Supervisor";
    private static final String SUPERVISOR_BIN_NAME = "supervisor";
    public static final int WORKER_STATUS_REPORT_INTERVAL_MILLIS_DEFAULT = 15000;

    private static final Set<ZiggyEventHandler> ziggyEventHandlers = ConcurrentHashMap.newKeySet();

    // Global list of tasks that have been killed by the TaskRequestHandlerLifecycleManager,
    // a PipelineWorker, or the delete command of a remote system's batch scheduler.
    private static final Set<PipelineTask> haltedTasks = ConcurrentHashMap.newKeySet();

    private static WorkerResources defaultResources;

    private ScheduledExecutorService heartbeatExecutor;
    private QueueCommandManager queueCommandManager = QueueCommandManager.newInstance();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineExecutor pipelineExecutor = new PipelineExecutor();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private ZiggyEventOperations ziggyEventOperations = new ZiggyEventOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private AlgorithmMonitor algorithmMonitor;

    public PipelineSupervisor(int workerCount, int workerHeapSize) {
        super(NAME);
        checkArgument(workerCount > 0, "Worker count must be positive");
        checkArgument(workerHeapSize > 0, "Worker heap size must be positive");
        defaultResources = new WorkerResources(workerCount, workerHeapSize);
        log.debug("Starting pipeline supervisor with {} workers and {} MB max heap", workerCount,
            workerHeapSize);
    }

    public PipelineSupervisor(boolean messaging, boolean database) {
        super(NAME, messaging, database);
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public void initialize() {
        try {
            super.initialize();
            clearStaleTaskStates();

            // if HDF5 is to be used as the default binfile format (or indeed at all),
            // load the library now -- note that (a) this is necessary because in linux
            // some kind of conflict occurs when libhdf5 is loaded before libjhdf5, but not
            // vice versa; (b) use of the System.loadLibrary doesn't work because at the first
            // call to an HDF5 method, the code looks at a private static var that tells it
            // whether the library is loaded, and that var is false (because H5 didn't load it).
            // It doesn't look to see whether the library was loaded by somebody other than H5.
            // It thus attempts to load again and we get the conflict.

            H5.loadH5Lib();

            log.info("Subscribing to messages");
            subscribe();
            StartPipelineRequestManager.start();
            log.info("Subscribing to messages...done");

            ZiggyRmiServer.start();

            ZiggyRmiClient.start(NAME);
            ZiggyShutdownHook.addShutdownHook(() -> {
                log.info("Sending shutdown notification");
                ZiggyRmiServer.shutdown();
                ZiggyRmiClient.reset();
            });

            // Start the heartbeat messages
            log.info("Starting supervisor-client heartbeat generator");
            heartbeatExecutor = new ScheduledThreadPoolExecutor(1);
            long heartbeatIntervalMillis = HeartbeatMessage.heartbeatIntervalMillis();
            if (heartbeatIntervalMillis > 0) {
                heartbeatExecutor.scheduleAtFixedRate(
                    () -> ZiggyRmiServer.addToBroadcastQueue(new HeartbeatMessage()), 0,
                    HeartbeatMessage.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
            }
            ZiggyShutdownHook.addShutdownHook(() -> {
                log.info("Shutting down heartbeat executor");
                heartbeatExecutor.shutdownNow();
                log.info("Shutting down heartbeat executor...done");
            });

            // Start the algorithm monitor.
            log.info("Starting algorithm monitor");
            algorithmMonitor = new AlgorithmMonitor();
            log.info("starting algorithm monitor...done");

            // Start the task request handler lifecycle manager.
            log.info("Starting task request handler lifecycle manager");
            TaskRequestHandlerLifecycleManager.initializeInstance();
            log.info("Starting task request handler lifecycle manager...done");

            log.info("Starting metrics dumper thread");
            MetricsDumper metricsDumper = new MetricsDumper(
                AbstractPipelineProcess.getProcessInfo().getPid());
            Thread metricsDumperThread = new Thread(metricsDumper, "MetricsDumper");
            metricsDumperThread.setDaemon(true);
            metricsDumperThread.start();
            log.info("Starting metrics dumper thread...done");

            log.info("Loading event handlers");
            ziggyEventHandlers.addAll(ziggyEventOperations().eventHandlers());
            for (ZiggyEventHandler handler : ziggyEventHandlers) {
                if (handler.isEnableOnClusterStart()) {
                    handler.start();
                }
            }
            log.info("Loading event handlers...done");
        } catch (Exception e) {
            log.error("Initialization failed!", e);
            System.exit(-1);
        }
    }

    /**
     * Set the error flag on stale tasks, which means they are in one of the
     * {@link ProcessingStep#processingSteps()}. Used at worker startup to reset the state after an
     * abnormal supervisor exit while processing.
     */
    public void clearStaleTaskStates() {

        // Set the error flag on stale tasks, which means they are in one of the {@link
        // ProcessingStep#processingSteps()}. These conditions indicate that the previous instance
        // of the worker process on this host died abnormally
        PipelineTaskOperations.ClearStaleStateResults clearStateResults = pipelineTaskOperations()
            .clearStaleTaskStates();

        // Update the pipeline instance state for the instances associated with the stale tasks from
        // above since that change may result in a change to the instances
        PipelineExecutor pe = new PipelineExecutor();
        for (Long instanceId : clearStateResults.uniqueInstanceIds) {
            log.info("Updating instance state for instanceId={}", instanceId);
            pe.logUpdatedInstanceState(pipelineInstanceOperations().pipelineInstance(instanceId));
        }
    }

    /**
     * Implements subscriptions that are relevant to the supervisor as a whole rather than any
     * particular class within the supervisor.
     */
    private void subscribe() {
        ZiggyMessenger.subscribe(EventHandlerRequest.class, message -> {
            ZiggyMessenger.publish(new ZiggyEventHandlerInfoMessage(message,
                PipelineSupervisor.serializableZiggyEventHandlers()));
        });
        ZiggyMessenger.subscribe(StartMemdroneRequest.class, message -> {
            new Memdrone(message.getModuleName(), message.getInstanceId()).startMemdrone();
        });
        ZiggyMessenger.subscribe(TaskLogInformationRequest.class, message -> {
            ZiggyMessenger.publish(new TaskLogInformationMessage(message,
                TaskLog.searchForLogFiles(message.getPipelineTask())));
        });
        ZiggyMessenger.subscribe(SingleTaskLogRequest.class, message -> {
            ZiggyMessenger.publish(new SingleTaskLogMessage(message, taskLogContents(message)));
        });
        ZiggyMessenger.subscribe(WorkerResourcesRequest.class, message -> {
            ZiggyMessenger.publish(new WorkerResourcesMessage(defaultResources, null));
        });
        ZiggyMessenger.subscribe(HaltTasksRequest.class, message -> {
            haltRemoteTasks(message);
        });
        ZiggyMessenger.subscribe(RemoveTaskFromKilledTasksMessage.class, message -> {
            haltedTasks.remove(message.getPipelineTask());
        });
        ZiggyMessenger.subscribe(TaskHaltedMessage.class, message -> {
            handleTaskHaltedMessage(message);
        });
        ZiggyMessenger.subscribe(RestartTasksRequest.class, message -> {
            restartTasks(message);
        });
        ZiggyMessenger.subscribe(UpdateProcessingStepMessage.class, message -> {
            updateProcessingStep(message);
        });
        ZiggyMessenger.subscribe(RetryTransitionRequest.class, message -> {
            retryTransition(message);
        });
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private String taskLogContents(SingleTaskLogRequest request) {
        File taskLogFile = new File(request.getTaskLogInformation().getFullPath());
        String stepLogContents = "<Request timed-out!>";
        log.info("Reading task log {}", taskLogFile);

        Charset defaultCharset = null;
        StringBuilder fileContents = new StringBuilder(1024 * 4);
        try {
            fileContents.append(FileUtils.readFileToString(taskLogFile, defaultCharset));
            log.info("Returning task log {} ({} chars) ", taskLogFile, fileContents.length());
            stepLogContents = fileContents.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read file " + taskLogFile.toString(), e);
        }
        return stepLogContents;
    }

    /**
     * Kills tasks that run remotely. The {@link AlgorithmManager} is queried for remote jobs of all
     * the tasks it's monitoring. If there are any such, it uses the PBS qdel command to delete all
     * such tasks from PBS.
     * <p>
     * Package scoped for unit testing.
     */
    void haltRemoteTasks(HaltTasksRequest message) {
        Map<PipelineTask, List<Long>> jobIdsByTask = jobIdsByTask(message.getPipelineTasks());
        if (jobIdsByTask.isEmpty()) {
            return;
        }
        for (PipelineTask pipelineTask : message.getPipelineTasks()) {
            List<Long> jobIds = jobIdsByTask.get(pipelineTask);
            if (CollectionUtils.isEmpty(jobIds)) {
                continue;
            }
            if (queueCommandManager().deleteJobsByJobId(jobIds) == 0) {
                publishTaskHaltedMessage(pipelineTask);
            }
        }
    }

    /** Sends task-halted message. */
    // TODO Inline
    // This requires that the ZiggyMessenger.publish() call can be verified by Mockito.
    // The KillTaskMessage.timeSent field makes it difficult.
    void publishTaskHaltedMessage(PipelineTask pipelineTask) {
        ZiggyMessenger.publish(new TaskHaltedMessage(pipelineTask));
    }

    /**
     * Performs the aspects of halting tasks that are cluster wide, to wit:
     * <ol>
     * <li>Add to the list of successfully halted tasks (so other classes can find out which ones
     * got halted).
     * <li>Set the {@link PipelineTask} error flag.
     * <li>Issue an alert that the task has been halted.
     * </ol>
     * <p>
     * Package scoped for unit testing.
     */
    void handleTaskHaltedMessage(TaskHaltedMessage message) {
        haltedTasks.add(message.getPipelineTask());

        // Issue an alert.
        alertService().generateAndBroadcastAlert("PI", message.getPipelineTask(), Severity.ERROR,
            "Task " + message.getPipelineTask().getId() + " halted");

        // Set the task's error flag.
        pipelineTaskDataOperations().taskErrored(message.getPipelineTask());
    }

    /**
     * Submits a collection of {@link PipelineTask} instances for restarting, based on the contents
     * of a restart message. Because the restart was requested by the user and is not an automatic
     * resubmit, the resubmit counts on the tasks must be reset to zero.
     */
    void restartTasks(RestartTasksRequest message) {
        Collection<PipelineTask> tasksToResubmit = message.getPipelineTasks();
        pipelineTaskDataOperations().prepareTasksForManualResubmit(tasksToResubmit);
        pipelineExecutor().restartFailedTasks(tasksToResubmit, message.isDoTransitionOnly(),
            message.getRunMode());

        ZiggyMessenger.publish(new PipelineInstanceStartedMessage());
    }

    /** Perform pipeline task processing steps serially to avoid race conditions */
    private synchronized void updateProcessingStep(UpdateProcessingStepMessage message) {
        pipelineTaskDataOperations().updateProcessingStep(message.getPipelineTask(),
            message.getProcessingStep());
    }

    /** Retry transition to the next instance node. Package scoped for testing. */
    void retryTransition(RetryTransitionRequest message) {
        PipelineInstance pipelineInstance = pipelineInstanceOperations()
            .pipelineInstance(message.getPipelineInstanceId());
        List<PipelineInstanceNode> pipelineInstanceNodes = pipelineInstanceOperations()
            .instanceNodes(pipelineInstance);
        List<PipelineInstanceNode> nodesWithFailedTransitions = pipelineInstanceNodes.stream()
            .filter(PipelineInstanceNode::isTransitionFailed)
            .collect(Collectors.toList());

        // Throw exception if no instance nodes in transition-failed state.
        if (nodesWithFailedTransitions.size() == 0) {
            throw new PipelineException("No instance nodes with failed transitions in instance "
                + message.getPipelineInstanceId());
        }

        // Throw exception if > 1 instance nodes in transition failed state.
        if (nodesWithFailedTransitions.size() > 1) {
            throw new PipelineException(
                "Multiple instance nodes with failed transitions in instance"
                    + message.getPipelineInstanceId() + ": "
                    + nodesWithFailedTransitions.stream()
                        .map(PipelineInstanceNode::getModuleName)
                        .collect(Collectors.toList())
                        .toString());
        }

        // Set the states of the instance and instance node.
        pipelineInstanceNodeOperations()
            .clearTransitionFailedState(nodesWithFailedTransitions.get(0));

        // Try the transition again.
        pipelineExecutor().transitionToNextInstanceNode(nodesWithFailedTransitions.get(0));
    }

    public static CommandLine supervisorCommand(WrapperCommand cmd, int workerCount,
        int workerHeapSize) {
        Path supervisorPath = DirectoryProperties.ziggyBinDir().resolve(SUPERVISOR_BIN_NAME);
        CommandLine commandLine = new CommandLine(supervisorPath.toString());
        if (cmd == WrapperCommand.START) {
            // Refer to supervisor.wrapper.conf for appropriate indices for the parameters specified
            // here.
            commandLine
                .addArgument(
                    wrapperParameter(WRAPPER_LOG_FILE_PROP_NAME, supervisorLogFilename(true)))
                .addArgument(wrapperParameter(WRAPPER_CLASSPATH_PROP_NAME_PREFIX, 1,
                    DirectoryProperties.ziggyHomeDir().resolve("libs").resolve("*.jar").toString()))
                .addArgument(wrapperParameter(WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX, 1,
                    DirectoryProperties.ziggyLibDir().toString()))
                .addArgument(wrapperParameter(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 3,
                    ExternalProcessUtils.log4jConfigString()))
                .addArgument(wrapperParameter(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 4,
                    ExternalProcessUtils.ziggyLog(supervisorLogFilename(false))))
                .addArgument(wrapperParameter(WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX, 2,
                    Integer.toString(workerCount)))
                .addArgument(wrapperParameter(WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX, 3,
                    Integer.toString(workerHeapSize)));

            // Add classpaths for pipeline side, if any are needed.
            String pipelineClasspath = ZiggyConfiguration.getInstance()
                .getString(PropertyName.PIPELINE_CLASSPATH.property(), null);
            if (pipelineClasspath != null) {
                String[] pipelineClasspaths = pipelineClasspath.split(":");
                for (int i = 0; i < pipelineClasspaths.length; i++) {
                    int classpathIndex = i + 2;
                    commandLine.addArgument(wrapperParameter(WRAPPER_CLASSPATH_PROP_NAME_PREFIX,
                        classpathIndex, pipelineClasspaths[i]));
                }
            }
        }
        commandLine.addArgument(cmd.toString());

        return commandLine;
    }

    /**
     * The log file name used by the supervisor.
     */
    public static String supervisorLogFilename(boolean wrapper) {
        String filename = WrapperUtils.logFilename(SUPERVISOR_BIN_NAME, wrapper);
        return DirectoryProperties.supervisorLogDir().resolve(filename).toString();
    }

    public static Set<ZiggyEventHandler> ziggyEventHandlers() {
        return ziggyEventHandlers;
    }

    public static Set<ZiggyEventHandlerInfoForDisplay> serializableZiggyEventHandlers() {
        return ziggyEventHandlers.stream()
            .map(ZiggyEventHandlerInfoForDisplay::new)
            .collect(Collectors.toSet());
    }

    public static boolean taskOnHaltedTaskList(PipelineTask pipelineTask) {
        return haltedTasks.contains(pipelineTask);
    }

    public static void removeTaskFromHaltedTaskList(PipelineTask pipelineTask) {
        haltedTasks.remove(pipelineTask);
    }

    public static WorkerResources defaultResources() {
        return defaultResources;
    }

    Map<PipelineTask, List<Long>> jobIdsByTask(Collection<PipelineTask> pipelineTasks) {
        return algorithmMonitor.jobIdsByTaskId(pipelineTasks);
    }

    // Package scoped for testing purposes.
    QueueCommandManager queueCommandManager() {
        return queueCommandManager;
    }

    AlertService alertService() {
        return AlertService.getInstance();
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineExecutor pipelineExecutor() {
        return pipelineExecutor;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    ZiggyEventOperations ziggyEventOperations() {
        return ziggyEventOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    public static void main(String[] args) {
        int workerThreads = Integer.parseInt(args[0]);
        int workerHeapSize = Integer.parseInt(args[1]);
        new PipelineSupervisor(workerThreads, workerHeapSize).initialize();
    }
}
