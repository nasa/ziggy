package gov.nasa.ziggy.module;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Monitors algorithm processing by monitoring state files.
 * <p>
 * Two instances are used: one for local tasks and the other for remote execution jobs (on HPC
 * and/or cloud systems). Each one checks at regular intervals for updates to the {@link StateFile}
 * for the specific task. The intervals are managed by a {@link ScheduledThreadPoolExecutor}.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmMonitor.class);

    private static final long SSH_POLL_INTERVAL_MILLIS = 10 * 1000; // 10 secs
    private static final long LOCAL_POLL_INTERVAL_MILLIS = 2 * 1000; // 2 seconds

    private static AlgorithmMonitor localMonitoringInstance = null;
    private static AlgorithmMonitor remoteMonitoringInstance = null;
    private static ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(2);

    private List<String> corruptedStateFileNames = new ArrayList<>();
    private boolean startLogMessageWritten = false;
    private AlgorithmType algorithmType;
    private String monitorVersion;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineExecutor pipelineExecutor = new PipelineExecutor();

    JobMonitor jobMonitor = null;

    private final ConcurrentHashMap<String, StateFile> state = new ConcurrentHashMap<>();

    /** What needs to be done after a task exits the state file checks loop: */
    private enum Disposition {

        // Algorithm processing is complete. Persist results.
        PERSIST {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                StateFile stateFile = new StateFile(pipelineTask.getModuleName(),
                    pipelineTask.getPipelineInstanceId(), pipelineTask.getId())
                        .newStateFileFromDiskFile();
                if (stateFile.getNumFailed() != 0) {
                    log.warn("{} subtasks out of {} failed but task completed",
                        stateFile.getNumFailed(), stateFile.getNumComplete());
                    monitor.alertService()
                        .generateAndBroadcastAlert("Algorithm Monitor", pipelineTask.getId(),
                            Severity.WARNING, "Failed subtasks, see logs for details");
                }

                log.info("Sending task with id: " + pipelineTask.getId()
                    + " to worker to persist results");

                monitor.pipelineExecutor()
                    .persistTaskResults(
                        monitor.pipelineTaskOperations().pipelineTask(pipelineTask.getId()));
            }
        },

        // Algorithm processing has failures but resubmitting is allowed. Resubmit.
        RESUBMIT {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.warn("Resubmitting task with id: " + pipelineTask.getId()
                    + " for additional processing");
                monitor.alertService()
                    .generateAndBroadcastAlert("Algorithm Monitor", pipelineTask.getId(),
                        Severity.WARNING, "Resubmitting task for further processing");
                PipelineTask databaseTask = monitor.pipelineTaskOperations()
                    .prepareTaskForAutoResubmit(pipelineTask);

                // Submit tasks for resubmission at highest priority.
                monitor.pipelineExecutor()
                    .restartFailedTasks(List.of(databaseTask), false, RunMode.RESUBMIT);
            }
        },

        // Algorithm was killed or has failures but can't be resubmitted. Mark as
        // failed.
        FAIL {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.error("Task with id " + pipelineTask.getId() + " failed on remote system, "
                    + "marking task as errored and not restarting.");
                monitor.handleFailedTask(monitor.state.get(StateFile.invariantPart(pipelineTask)));
                monitor.pipelineTaskOperations().taskErrored(pipelineTask);
            }
        };

        /**
         * Performs the necessary actions for each of the enumerations.
         */
        public abstract void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask);
    }

    public static void initialize() {
        synchronized (AlgorithmMonitor.class) {
            if (localMonitoringInstance == null || remoteMonitoringInstance == null) {
                ZiggyMessenger.subscribe(MonitorAlgorithmRequest.class, message -> {
                    if (message.getAlgorithmType().equals(AlgorithmType.LOCAL)) {
                        AlgorithmMonitor.startLocalMonitoring(
                            message.getStateFile().newStateFileFromDiskFile());
                    } else {
                        AlgorithmMonitor.startRemoteMonitoring(
                            message.getStateFile().newStateFileFromDiskFile());
                    }
                });
            }
            if (localMonitoringInstance == null) {
                localMonitoringInstance = new AlgorithmMonitor(AlgorithmType.LOCAL);
                localMonitoringInstance.startMonitoringThread();
            }
            if (remoteMonitoringInstance == null) {
                remoteMonitoringInstance = new AlgorithmMonitor(AlgorithmType.REMOTE);
                remoteMonitoringInstance.startMonitoringThread();
            }

            // Whenever a worker sends a "last message," run an unscheduled
            // update of the monitors.
            ZiggyMessenger.subscribe(WorkerStatusMessage.class, message -> {
                if (message.isLastMessageFromWorker()) {
                    localMonitoringInstance.run();
                    remoteMonitoringInstance.run();
                }
            });
        }
    }

    /**
     * Returns the collection of {@link StateFile} instances currently being tracked by the remote
     * execution monitor.
     */
    public static Collection<StateFile> remoteTaskStateFiles() {
        if (remoteMonitoringInstance == null || remoteMonitoringInstance.state.isEmpty()) {
            return null;
        }
        return remoteMonitoringInstance.state.values();
    }

    /**
     * Constructor. Default scope for use in unit tests.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    AlgorithmMonitor(AlgorithmType algorithmType) {
        this.algorithmType = algorithmType;
        monitorVersion = algorithmType.name().toLowerCase();

        log.info("Starting new monitor for: " + DirectoryProperties.stateFilesDir().toString());
        initializeJobMonitor();
    }

    /**
     * Start the monitoring thread for a given monitor.
     */
    void startMonitoringThread() {
        long pollingIntervalMillis = pollingIntervalMillis();
        if (pollingIntervalMillis > 0) {
            log.info("Starting polling on " + monitorVersion + " with " + pollingIntervalMillis
                + " msec interval");
            threadPool.scheduleWithFixedDelay(this, 0, pollingIntervalMillis(),
                TimeUnit.MILLISECONDS);
        }
    }

    private String username() {
        String username = System.getenv("USERNAME");
        if (username == null) {
            username = System.getenv("USER");
        }
        return username;
    }

    public static void startLocalMonitoring(StateFile task) {
        startMonitoring(task, AlgorithmType.LOCAL);
    }

    public static void startRemoteMonitoring(StateFile task) {
        startMonitoring(task, AlgorithmType.REMOTE);
    }

    private static void startMonitoring(StateFile task, AlgorithmType algorithmType) {
        AlgorithmMonitor instance = algorithmType.equals(AlgorithmType.REMOTE)
            ? remoteMonitoringInstance
            : localMonitoringInstance;
        instance.startMonitoring(task);
    }

    void startMonitoring(StateFile task) {
        log.info("Starting monitoring for: " + task.invariantPart() + " on " + monitorVersion
            + " algorithm monitor");
        state.put(task.invariantPart(), new StateFile(task));
        jobMonitor().addToMonitoring(task);
    }

    private List<File> stateFiles() {

        List<File> stateDirListing = new LinkedList<>();

        // get the raw list, excluding directories
        File stateDirFile = DirectoryProperties.stateFilesDir().toFile();
        List<File> allFiles = new ArrayList<>();
        if (stateDirFile != null) {
            File[] files = stateDirFile.listFiles();
            if (files != null) {
                allFiles.addAll(Arrays.asList(files));
            }
            stateDirListing = allFiles.stream()
                .filter(s -> !s.isDirectory())
                .collect(Collectors.toList());
        }

        // throw away everything that doesn't have the correct pattern, and everything
        // that is on the corrupted list
        List<File> filteredFiles = stateDirListing.stream()
            .filter(s -> StateFile.STATE_FILE_NAME_PATTERN.matcher(s.getName()).matches())
            .filter(s -> !corruptedStateFileNames.contains(s.getName()))
            .collect(Collectors.toList());
        return new LinkedList<>(filteredFiles);
    }

    private void performStateFileChecks(StateFile oldState, StateFile remoteState) {

        String key = remoteState.invariantPart();

        if (!oldState.equals(remoteState)) {
            // state change
            log.info("Updating state for: " + remoteState + " (was: " + oldState + ")");
            state.put(key, remoteState);

            long taskId = remoteState.getPipelineTaskId();

            pipelineTaskOperations().updateSubtaskCounts(taskId, remoteState.getNumTotal(),
                remoteState.getNumComplete(), remoteState.getNumFailed());

            if (remoteState.isRunning()) {
                // update processing state
                pipelineTaskOperations().updateProcessingStep(taskId, ProcessingStep.EXECUTING);
            }

            if (remoteState.isDone()) {
                // update processing state
                pipelineTaskOperations().updateProcessingStep(taskId,
                    ProcessingStep.WAITING_TO_STORE);

                // It may be the case that all the subtasks are processed, but that
                // there are jobs still running, or (more likely) queued. We can address
                // that by deleting them from PBS.
                Set<Long> jobIds = jobMonitor().allIncompleteJobIds(remoteState);
                if (jobIds != null && !jobIds.isEmpty()) {
                    jobMonitor().getQstatCommandManager().deleteJobsByJobId(jobIds);
                }

                // Always send the task back to the worker
                sendTaskToWorker(remoteState);

                log.info("Removing monitoring for: " + key);

                state.remove(key);
                jobMonitor().endMonitoring(remoteState);
            }
        } else if (jobMonitor().isFinished(remoteState)) {
            // Some job failures leave the state file untouched. If this happens, the QstatMonitor
            // can determine that in fact the job is no longer running. In this case, set the state
            // file to FAILED. Then in the next pass through this loop, standard handling for a
            // failed job can be applied.
            moveStateFileToCompleteState(remoteState);
        }
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void run() {

        if (!startLogMessageWritten) {
            log.info("Algorithm monitor started...");
            startLogMessageWritten = true;
        }
        try {
            if (!state.isEmpty()) {
                jobMonitor().update();

                List<File> stateDirListing = stateFiles();
                if (log.isDebugEnabled()) {
                    dumpRemoteState(stateDirListing);
                }
                performStateFileLoop(stateDirListing);
            }
        } catch (Exception e) {
            // We don't want transient problems with the remote monitoring tool
            // (which is third party software and not under our control) to bring
            // down the monitor, so we catch all exceptions here including runtime
            // ones in the hope and expectation that the next time we call the
            // monitor the transient problem will have resolved itself.
            log.warn("Task monitor: exception has occurred", e);
        }
    }

    /**
     * Loops over all files in the stateDirListing and, if they are in the monitoring list, performs
     * state file checks on the cached and file states. Any state file name that cannot be parsed
     * into a new StateFile object is added to a registry of corrupted names and subsequently
     * ignored.
     *
     * @param stateDirListing
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void performStateFileLoop(List<File> stateDirListing) {
        for (File remoteFile : stateDirListing) {
            String name = remoteFile.getName();
            try {
                StateFile remoteState = new StateFile(name);
                StateFile oldState = state.get(remoteState.invariantPart());
                if (oldState != null) { // ignore tasks we were not
                    // charged with
                    performStateFileChecks(oldState, remoteState);
                }
            } catch (Exception e) {
                log.error("State file with name " + name
                    + " encountered exception and will be removed from monitoring", e);
                corruptedStateFileNames.add(name);
            }
        }
    }

    /**
     * Resubmits a task to the worker and, optionally, to the NAS. This method is called both for
     * complete tasks and failing tasks, because each needs to be looked at again by the worker. If
     * the task has failing subtasks which should be resubmitted, the caller should specify
     * <code>true</code> for <code>restart</code>.
     *
     * @param remoteState the state file
     * @param restart if true, resubmit the task in the NAS
     */
    private void sendTaskToWorker(StateFile remoteState) {

        PipelineTask pipelineTask = pipelineTaskOperations()
            .pipelineTask(remoteState.getPipelineTaskId());

        pipelineTaskOperations().updateJobs(pipelineTask, true);

        // Perform the actions necessary based on the task disposition
        determineDisposition(remoteState, pipelineTask).performActions(this, pipelineTask);
    }

    /**
     * Handles a task for which processing has failed (either due to error or because the user
     * killed it). In this case, several actions need to be taken: the information about the cause
     * of the error has to be captured via qstat and logged locally; the pipeline task entry in the
     * database needs its TaskExecutionLog updated; the remote state file needs to be renamed to
     * indicate that the job errored.
     *
     * @param stateFile StateFile instance for deleted task.
     */
    private void handleFailedTask(StateFile stateFile) {

        // get the exit code and comment via qstat
        String exitStatus = taskStatusValues(stateFile);
        String exitComment = taskCommentValues(stateFile);
        String exitState = stateFile.getState().toString().toLowerCase();

        if (exitState.equals("deleted")) {
            log.error("Task " + stateFile.getPipelineTaskId() + " has state file in "
                + exitState.toUpperCase() + " state");
        } else {
            log.error("Task " + stateFile.getPipelineTaskId() + " has failed");
            exitState = "failed";
        }
        if (exitStatus != null) {
            log.error("Exit status from remote system for all jobs: " + exitStatus);
        } else {
            log.error("No exit status provided");
            exitStatus = "not provided";
        }
        if (exitComment != null) {
            log.error("Exit comment from remote system: " + exitComment);
        } else {
            log.error("No exit comment provided");
            exitComment = "not provided";
        }

        // issue an alert about the deletion
        String message = algorithmType.equals(AlgorithmType.REMOTE)
            ? "Task " + exitState + ", return codes = " + exitStatus + ", comments = " + exitComment
            : "Task " + exitState;
        alertService().generateAndBroadcastAlert("Algorithm Monitor", stateFile.getPipelineTaskId(),
            AlertService.Severity.ERROR, message);
    }

    private String taskStatusValues(StateFile stateFile) {
        Map<Long, Integer> exitStatusByJobId = jobMonitor().exitStatus(stateFile);
        if (exitStatusByJobId.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Integer> entry : exitStatusByJobId.entrySet()) {
            sb.append(entry.getKey());
            sb.append("(");
            if (entry.getValue() != null) {
                sb.append(entry.getValue());
            }
            sb.append(") ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private String taskCommentValues(StateFile stateFile) {
        Map<Long, String> exitComment = jobMonitor().exitComment(stateFile);
        if (exitComment == null || exitComment.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, String> entry : exitComment.entrySet()) {
            sb.append(entry.getKey());
            sb.append("(");
            if (entry.getValue() != null) {
                sb.append(entry.getValue());
            }
            sb.append(") ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Moves the state file for a remote job to the COMPLETE state. This is only done if the job
     * ended on the remote system in a way that was not detected by the job itself but was later
     * detected via qstat calls.
     *
     * @param stateFile State file for failed job.
     */
    private void moveStateFileToCompleteState(StateFile stateFile) {
        stateFile.setStateAndPersist(StateFile.State.COMPLETE);
    }

    private Disposition determineDisposition(StateFile state, PipelineTask pipelineTask) {

        // A task that was deliberately killed must be marked as failed regardless of
        // how many subtasks completed.
        if (taskIsKilled(pipelineTask.getId())) {
            return Disposition.FAIL;
        }
        // The total number of bad subtasks includes both the ones that failed and the
        // ones that never ran / never finished. If there are few enough bad subtasks,
        // then we can persist results.

        PipelineDefinitionNodeExecutionResources resources = pipelineTaskOperations()
            .executionResources(pipelineTask);
        if (state.getNumTotal() - state.getNumComplete() <= resources.getMaxFailedSubtaskCount()) {
            return Disposition.PERSIST;
        }

        // If the task has bad subtasks but the number of automatic resubmits hasn't
        // been exhausted, then resubmit.
        if (pipelineTask.getAutoResubmitCount() < resources.getMaxAutoResubmits()) {
            return Disposition.RESUBMIT;
        }

        // If we've gotten this far, then the task has to be considered as failed:
        // it has too many bad subtasks and has exhausted its automatic retries.
        return Disposition.FAIL;
    }

    private void dumpRemoteState(List<File> remoteState) {
        log.debug("Remote state dir:");
        for (File file : remoteState) {
            log.debug(file.toString());
        }
    }

    /**
     * Determines whether to continue the monitoring while-loop. For testing purposes, this can be
     * replaced with a mocked version that performs a finite number of loops.
     *
     * @return true
     */
    boolean continueMonitoring() {
        return true;
    }

    /**
     * Obtains a new PipelineExecutor. Replace with mocked method for unit testing.
     *
     * @return
     */
    PipelineExecutor pipelineExecutor() {
        return pipelineExecutor;
    }

    /** Replace with mocked method for unit testing. */
    AlertService alertService() {
        return AlertService.getInstance();
    }

    /** Replace with mocked method for unit testing. */
    boolean taskIsKilled(long taskId) {
        return PipelineSupervisor.taskOnKilledTaskList(taskId);
    }

    /**
     * Returns the polling interval, in milliseconds. Replace with mocked method for unit testing.
     */
    long pollingIntervalMillis() {
        return algorithmType.equals(AlgorithmType.REMOTE) ? SSH_POLL_INTERVAL_MILLIS
            : LOCAL_POLL_INTERVAL_MILLIS;
    }

    /** Stops the thread pool and replaces it. For testing only. */
    static void resetThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = new ScheduledThreadPoolExecutor(2);
    }

    /** Gets the {@link StateFile} from the state {@link Map}. For testing only. */
    StateFile getStateFile(StateFile stateFile) {
        return state.get(stateFile.invariantPart());
    }

    private void initializeJobMonitor() {
        jobMonitor = JobMonitor.newInstance(username(), algorithmType);
    }

    /** Replace with mocked method for unit testing. */
    JobMonitor jobMonitor() {
        return jobMonitor;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }
}
