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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricsDumper;
import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud.ClearStaleStateResults;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.events.ZiggyEventCrud;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.messages.DefaultWorkerResourcesRequest;
import gov.nasa.ziggy.services.messages.EventHandlerRequest;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.KilledTaskMessage;
import gov.nasa.ziggy.services.messages.RemoveTaskFromKilledTasksMessage;
import gov.nasa.ziggy.services.messages.SingleTaskLogMessage;
import gov.nasa.ziggy.services.messages.SingleTaskLogRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.TaskLogInformationMessage;
import gov.nasa.ziggy.services.messages.TaskLogInformationRequest;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.services.messages.ZiggyEventHandlerInfoMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.services.process.ExternalProcessUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.WrapperUtils.WrapperCommand;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import hdf.hdf5lib.H5;

/**
 * Establishes the pipeline supervisor instance. The supervisor instance is a process that runs as a
 * daemon and provides central services for a data analysis pipeline (queueing and dispatch of task
 * requests, monitoring of algorithm execution, response to requests from console instances, etc.).
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineSupervisor extends AbstractPipelineProcess {

    private static final Logger log = LoggerFactory.getLogger(PipelineSupervisor.class);

    public static final String NAME = "Supervisor";
    private static final String SUPERVISOR_BIN_NAME = "supervisor";
    public static final int WORKER_STATUS_REPORT_INTERVAL_MILLIS_DEFAULT = 15000;

    private static final Set<ZiggyEventHandler> ziggyEventHandlers = ConcurrentHashMap.newKeySet();

    // Global list of task IDs that have been killed by the TaskRequestHandlerLifecycleManager,
    // a PipelineWorker, or the delete command of a remote system's batch scheduler.
    private static final Set<Long> killedTaskIds = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService heartbeatExecutor;
    private QueueCommandManager queueCommandManager;

    public PipelineSupervisor(int workerCount, int workerHeapSize) {
        super(NAME);
        checkArgument(workerCount > 0, "Worker count must be positive");
        checkArgument(workerHeapSize > 0, "Worker heap size must be positive");
        WorkerResources.setDefaultResources(new WorkerResources(workerCount, workerHeapSize));
        log.debug("Starting pipeline supervisor with " + workerCount + " workers and "
            + workerHeapSize + " MB max heap");
    }

    public PipelineSupervisor(boolean messaging, boolean database) {
        super(NAME, messaging, database);
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public void initialize() {
        try {
            super.initialize();

            ImmutableConfiguration config = ZiggyConfiguration.getInstance();

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

            log.info("Subscribing to messages...");
            subscribe();
            TriggerRequestManager.start();
            log.info("Subscribing to messages...done");

            int rmiPort = config.getInt(PropertyName.SUPERVISOR_PORT.property(),
                ZiggyRmiServer.RMI_PORT_DEFAULT);
            log.info("Starting RMI communications server with registry on port " + rmiPort);
            ZiggyRmiServer.initializeInstance(rmiPort);

            log.info("Starting ZiggyRmiClient instance with registry on port " + rmiPort + "...");
            ZiggyRmiClient.initializeInstance(rmiPort, NAME);
            ZiggyShutdownHook.addShutdownHook(() -> {
                log.info("Sending shutdown notification");
                ZiggyRmiServer.shutdown();
                ZiggyRmiClient.reset();
            });
            log.info("Starting ZiggyRmiClient instance ... done");

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
                log.info("Shutting down heartbeat executor...");
                heartbeatExecutor.shutdownNow();
                log.info("Shutting down heartbeat executor...done");
            });

            // Start the task request handler lifecycle manager.
            log.info("Starting task request handler lifecycle manager...");
            TaskRequestHandlerLifecycleManager.initializeInstance();
            log.info("Starting task request handler lifecycle manager...done");

            log.info("Starting metrics dumper thread...");
            MetricsDumper metricsDumper = new MetricsDumper(
                AbstractPipelineProcess.getProcessInfo().getPid());
            Thread metricsDumperThread = new Thread(metricsDumper, "MetricsDumper");
            metricsDumperThread.setDaemon(true);
            metricsDumperThread.start();

            log.info("Starting algorithm monitor threads...");
            AlgorithmMonitor.initialize();

            log.info("Loading event handlers...");
            ziggyEventHandlers.addAll(new ZiggyEventCrud().retrieveAllEventHandlers());
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
     * Set the state to ERROR for all tasks where the state is currently PROCESSING or SUBMITTED.
     * Used at worker startup to reset the state after an abnormal supervisor exit while processing.
     */
    public void clearStaleTaskStates() {

        /*
         * Set the pipeline task state to ERROR for any tasks assigned to this worker that are in
         * the PROCESSING state or the SUBMITTED state. These conditions indicate that the previous
         * instance of the worker process on this host died abnormally
         */
        ClearStaleStateResults clearStateResults = (ClearStaleStateResults) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
                return pipelineTaskCrud.clearStaleState();
            });

        /*
         * Update the pipeline instance state for the instances associated with the stale tasks from
         * above since that change may result in a change to the instances
         */

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineExecutor pe = new PipelineExecutor();
            PipelineInstanceCrud instanceCrud = new PipelineInstanceCrud();

            for (Long instanceId : clearStateResults.uniqueInstanceIds) {
                log.info("Updating instance state for instanceId = " + instanceId);
                PipelineInstance instance = instanceCrud.retrieve(instanceId);
                pe.logUpdatedInstanceState(instance);
            }
            return null;
        });
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
                TaskLog.searchForLogFiles(message.getInstanceId(), message.getTaskId())));
        });
        ZiggyMessenger.subscribe(SingleTaskLogRequest.class, message -> {
            ZiggyMessenger.publish(new SingleTaskLogMessage(message, taskLogContents(message)));
        });
        ZiggyMessenger.subscribe(DefaultWorkerResourcesRequest.class, message -> {
            ZiggyMessenger.publish(WorkerResources.getDefaultResources());
        });
        ZiggyMessenger.subscribe(KillTasksRequest.class, message -> {
            killRemoteTasks(message);
        });
        ZiggyMessenger.subscribe(RemoveTaskFromKilledTasksMessage.class, message -> {
            killedTaskIds.remove(message.getTaskId());
        });
        ZiggyMessenger.subscribe(KilledTaskMessage.class, message -> {
            handleKilledTaskMessage(message);
        });
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private String taskLogContents(SingleTaskLogRequest request) {
        File taskLogFile = new File(request.getTaskLogInformation().getFullPath());
        String stepLogContents = "<Request timed-out!>";
        log.info("Reading task log: " + taskLogFile);

        Charset defaultCharset = null;
        StringBuilder fileContents = new StringBuilder(1024 * 4);
        try {
            fileContents.append(FileUtils.readFileToString(taskLogFile, defaultCharset));
            log.info("Returning task log (" + fileContents.length() + " chars): " + taskLogFile);
            stepLogContents = fileContents.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read file " + taskLogFile.toString(), e);
        }
        return stepLogContents;
    }

    /**
     * Kills tasks that run remotely. The {@link AlgorithmManager} for remote jobs is queried for
     * the {@link StateFile} instances of all the tasks it's monitoring. If there are any such, it
     * uses the PBS qdel command to delete all such tasks from PBS.
     * <p>
     * Package scoped for unit testing.
     */
    void killRemoteTasks(KillTasksRequest message) {
        Collection<StateFile> stateFiles = remoteTaskStateFiles();
        if (CollectionUtils.isEmpty(stateFiles)) {
            return;
        }
        for (StateFile stateFile : stateFiles) {
            if (message.getTaskIds().contains(stateFile.getPipelineTaskId())
                && getQueueCommandManager().deleteJobsForStateFile(stateFile) == 0) {
                publishKilledTaskMessage(message, stateFile.getPipelineTaskId());
            }
        }
    }

    /** Sends killed-task message. */
    // TODO Inline
    // This requires that the ZiggyMessenger.publish() call can be verified by Mockito.
    // The KillTaskMessage.timeSent field makes it difficult.
    void publishKilledTaskMessage(KillTasksRequest message, long taskId) {
        ZiggyMessenger.publish(new KilledTaskMessage(message, taskId));
    }

    /**
     * Performs the aspects of halting tasks that are cluster wide, to wit:
     * <ol>
     * <li>Add to the list of successfully halted tasks (so other classes can find out which ones
     * got halted).
     * <li>Set the state of the {@link PipelineTask} to ERROR.
     * <li>Issue an alert that the task has been halted.
     * </ol>
     * <p>
     * Package scoped for unit testing.
     */
    void handleKilledTaskMessage(KilledTaskMessage message) {
        killedTaskIds.add(message.getTaskId());

        // Issue an alert.
        alertService().generateAndBroadcastAlert("PI", message.getTaskId(),
            AlertService.Severity.ERROR, "Task " + message.getTaskId() + " halted");

        // Set the task state.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations().setTaskState(message.getTaskId(), PipelineTask.State.ERROR);
            return null;
        });
    }

    public static CommandLine supervisorCommand(WrapperCommand cmd, int workerCount,
        int workerHeapSize) {
        Path supervisorPath = DirectoryProperties.ziggyBinDir().resolve(SUPERVISOR_BIN_NAME);
        CommandLine commandLine = new CommandLine(supervisorPath.toString());
        if (cmd == WrapperCommand.START) {
            // Refer to supervisor.wrapper.conf for appropriate indices for the parameters specified
            // here.
            String ziggyLibDir = DirectoryProperties.ziggyLibDir().toString();
            commandLine
                .addArgument(wrapperParameter(WRAPPER_LOG_FILE_PROP_NAME,
                    ExternalProcessUtils.supervisorLogFilename()))
                .addArgument(wrapperParameter(WRAPPER_CLASSPATH_PROP_NAME_PREFIX, 1,
                    DirectoryProperties.ziggyHomeDir().resolve("libs").resolve("*.jar").toString()))
                .addArgument(
                    wrapperParameter(WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX, 1, ziggyLibDir))
                .addArgument(wrapperParameter(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 5,
                    "-Djna.library.path=" + ziggyLibDir))
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

    public static Set<ZiggyEventHandler> ziggyEventHandlers() {
        return ziggyEventHandlers;
    }

    public static Set<ZiggyEventHandlerInfoForDisplay> serializableZiggyEventHandlers() {
        return ziggyEventHandlers.stream()
            .map(ZiggyEventHandlerInfoForDisplay::new)
            .collect(Collectors.toSet());
    }

    public static boolean taskOnKilledTaskList(long taskId) {
        return killedTaskIds.contains(taskId);
    }

    public static void removeTaskFromKilledTaskList(long taskId) {
        killedTaskIds.remove(taskId);
    }

    // Package scoped for testing purposes.
    Collection<StateFile> remoteTaskStateFiles() {
        return AlgorithmMonitor.remoteTaskStateFiles();
    }

    // Package scoped for testing purposes.
    QueueCommandManager getQueueCommandManager() {
        if (queueCommandManager == null) {
            queueCommandManager = QueueCommandManager.newInstance();
        }
        return queueCommandManager;
    }

    // Package scoped for testing purposes.
    AlertService alertService() {
        return AlertService.getInstance();
    }

    // Package scoped for testing purposes.
    PipelineOperations pipelineOperations() {
        return new PipelineOperations();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        int workerThreads = Integer.parseInt(args[0]);
        int workerHeapSize = Integer.parseInt(args[1]);
        new PipelineSupervisor(workerThreads, workerHeapSize).initialize();
    }
}
