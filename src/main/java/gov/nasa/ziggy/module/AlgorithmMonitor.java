package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.database.DatabaseTransactionFactory.performTransaction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

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

    private String stateFileDirPath = null;
    private List<String> corruptedStateFileNames = new ArrayList<>();
    private boolean startLogMessageWritten = false;
    private boolean monitorRemoteJobs;

    JobMonitor jobMonitor = null;

    private final ConcurrentHashMap<String, StateFile> state = new ConcurrentHashMap<>();

    /** What needs to be done after a task exits the state file checks loop: */
    private enum Disposition {

        // Algorithm processing is complete. Persist results.
        PERSIST {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.info("Sending task with id: " + pipelineTask.getId()
                    + " to worker to persist results");

                DatabaseTransactionFactory.performTransaction(() -> {
                    PipelineTaskCrud pipelineTaskCrud = monitor.pipelineTaskCrud();
                    PipelineTask dbTask = pipelineTaskCrud.retrieve(pipelineTask.getId());
                    // Submit tasks for persistence at highest priority.
                    monitor.pipelineExecutor().sendWorkerMessageForTask(dbTask, 0);
                    return null;
                });
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
                PipelineExecutor pipelineExecutor = monitor.pipelineExecutor();
                PipelineTask databaseTask = (PipelineTask) DatabaseTransactionFactory
                    .performTransaction(() -> {
                        PipelineTaskCrud pipelineTaskCrud = monitor.pipelineTaskCrud();
                        PipelineTask dbTask = pipelineTaskCrud.retrieve(pipelineTask.getId());
                        dbTask.incrementAutoResubmitCount();
                        dbTask.setState(PipelineTask.State.ERROR);
                        dbTask.stopExecutionClock();
                        pipelineTaskCrud.update(dbTask);
                        pipelineExecutor.updateTaskCountsForCurrentNode(dbTask, false);
                        pipelineExecutor.updateInstanceState(dbTask.getPipelineInstance());
                        return dbTask;
                    });

                // Submit tasks for resubmission at highest priority.
                DatabaseTransactionFactory.performTransaction(() -> {
                    pipelineExecutor.restartFailedTask(databaseTask, false);
                    pipelineExecutor.sendWorkerMessageForTask(databaseTask, 0, false,
                        RunMode.RESUBMIT);
                    return null;
                });
            }
        },

        // Algorithm was deleted or has failures but can't be resubmitted. Mark as
        // failed.
        FAIL {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.error("Task with id " + pipelineTask.getId() + " failed on remote system, "
                    + "marking task as errored and not restarting.");

                // If the state file was deleted, no alert is needed since one was already
                // issued;
                // if it's failed, an alert is still needed.
                StateFile stateFile = monitor.state.get(StateFile.invariantPart(pipelineTask));
                if (!stateFile.getState().equals(StateFile.State.DELETED)) {
                    monitor.handleFailedOrDeletedTask(
                        monitor.state.get(StateFile.invariantPart(pipelineTask)));
                }
                DatabaseTransactionFactory.performTransaction(() -> {
                    PipelineTaskCrud pipelineTaskCrud = monitor.pipelineTaskCrud();
                    PipelineTask databaseTask = pipelineTaskCrud.retrieve(pipelineTask.getId());
                    databaseTask.setState(PipelineTask.State.ERROR);
                    databaseTask.stopExecutionClock();
                    pipelineTaskCrud.update(databaseTask);
                    PipelineExecutor pipelineExecutor = monitor.pipelineExecutor();
                    pipelineExecutor.updateTaskCountsForCurrentNode(databaseTask, false);
                    pipelineExecutor.updateInstanceState(databaseTask.getPipelineInstance());
                    return null;
                });
            }
        };

        /**
         * Performs the necessary actions for each of the enumerations.
         *
         * @param monitor {@link AlgorithmMonitor}, provided so that the monitor's instances of
         * {@link PipelineTaskCrud} and {@link PipelineExecutor} can be used by the enumerations,
         * which in turn means that they can be given mocked instances for test purposes.
         * @param pipelineTask {@link PipelineTask} that is ready for results persisting,
         * resubmission, or declaration of failure.
         */
        public abstract void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask);
    }

    public static void initialize() {
        synchronized (AlgorithmMonitor.class) {
            if (localMonitoringInstance == null) {
                localMonitoringInstance = new AlgorithmMonitor(false);
                localMonitoringInstance.startMonitoringThread();
            }
            if (remoteMonitoringInstance == null) {
                remoteMonitoringInstance = new AlgorithmMonitor(true);
                remoteMonitoringInstance.startMonitoringThread();
            }
        }
    }

    /**
     * Constructor. Default scope for use in unit tests.
     */
    AlgorithmMonitor(boolean monitorRemoteJobs) {
        stateFileDirPath = DirectoryProperties.stateFilesDir().toString();
        this.monitorRemoteJobs = monitorRemoteJobs;
        log.info("Starting new monitor for: " + stateFileDirPath);
        initializeJobMonitor();
    }

    /**
     * Start the monitoring thread for a given monitor.
     */
    void startMonitoringThread() {
        if (pollingIntervalMillis() > 0) {
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
        startMonitoring(task, false);
    }

    public static void startRemoteMonitoring(StateFile task) {
        startMonitoring(task, true);
    }

    /**
     * Moves state files for locally-executing tasks to the DELETED state, based on a {@link List}
     * of task IDs that the caller wishes to delete. Returns the task IDs of tasks that were
     * successfully deleted (since some tasks on the list may have been in the submitted state, or
     * running remotely).
     *
     * @throws IOException
     */
    public static List<Long> deleteLocalTasks(List<Long> taskIds) throws IOException {
        List<Long> deletedTaskIds = new ArrayList<>();
        for (StateFile stateFile : localMonitoringInstance.state.values()) {
            if (taskIds.contains(stateFile.getPipelineTaskId())) {
                localMonitoringInstance.moveStateFileToDeletedState(stateFile);
                deletedTaskIds.add(stateFile.getPipelineTaskId());
            }
        }
        return deletedTaskIds;
    }

    private static void startMonitoring(StateFile task, boolean remoteExecution) {
        AlgorithmMonitor instance = remoteExecution ? remoteMonitoringInstance
            : localMonitoringInstance;
        instance.startMonitoring(task);

    }

    void startMonitoring(StateFile task) {
        log.info("Starting monitoring for: " + task.invariantPart());
        state.put(task.invariantPart(), new StateFile(task));
        jobMonitor().addToMonitoring(task);
    }

    private List<File> stateFiles() {

        List<File> stateDirListing = new LinkedList<>();

        // get the raw list, excluding directories
        File stateDirFile = new File(stateFileDirPath);
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

        // throw away everything that doesn't have the correct prefix, and everything
        // that is on the corrupted list
        List<File> filteredFiles = stateDirListing.stream()
            .filter(s -> s.getName().startsWith(StateFile.PREFIX))
            .filter(s -> !corruptedStateFileNames.contains(s.getName()))
            .collect(Collectors.toList());
        return new LinkedList<>(filteredFiles);
    }

    private void performStateFileChecks(StateFile oldState, StateFile remoteState)
        throws IOException {

        String key = remoteState.invariantPart();

        if (!oldState.equals(remoteState)) {
            // state change
            log.info("Updating state for: " + remoteState + " (was: " + oldState + ")");
            state.put(key, remoteState);

            ProcessingSummaryOperations attrOps = processingSummaryOperations();
            long taskId = remoteState.getPipelineTaskId();

            attrOps.updateSubTaskCounts(taskId, remoteState.getNumTotal(),
                remoteState.getNumComplete(), remoteState.getNumFailed());

            if (remoteState.isRunning()) {
                // update processing state
                attrOps.updateProcessingState(taskId, ProcessingState.ALGORITHM_EXECUTING);
            }

            if (remoteState.isDeleted()) {
                handleFailedOrDeletedTask(remoteState);
            }

            if (remoteState.isDone()) {
                // update processing state
                attrOps.updateProcessingState(taskId, ProcessingState.ALGORITHM_COMPLETE);

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
            } // end non-empty state map

        } catch (Exception e) {
            log.warn("top: caught e=" + e, e);
        } // end try-catch
    }

    /**
     * Loops over all files in the stateDirListing and, if they are in the monitoring list, performs
     * state file checks on the cached and file states. Any state file name that cannot be parsed
     * into a new StateFile object is added to a registry of corrupted names and subsequently
     * ignored.
     *
     * @param stateDirListing
     */
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

        PipelineTask pipelineTask = (PipelineTask) performTransaction(() -> {
            long taskId = remoteState.getPipelineTaskId();

            PipelineTask task = pipelineTaskCrud().retrieve(taskId);
            Hibernate.initialize(task.getPipelineParameterSets());
            Hibernate.initialize(task.getModuleParameterSets());
            Hibernate.initialize(task.getPipelineInstance().getId());

            // Update remote job information
            pipelineTaskOperations().updateJobs(task);
            return task;
        });

        // Perform the actions necessary based on the task disposition
        determineDisposition(remoteState, pipelineTask).performActions(this, pipelineTask);
    }

    /**
     * Handles a task for which processing has failed (either due to error or because the user
     * deleted it). In this case, several actions need to be taken: the information about the cause
     * of the error has to be captured via qstat and logged locally; the pipeline task entry in the
     * database needs its TaskExecutionLog updated; the remote state file needs to be renamed to
     * indicate that the job errored.
     *
     * @param stateFile StateFile instance for deleted task.
     */
    private void handleFailedOrDeletedTask(StateFile stateFile) {

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
        String message = monitorRemoteJobs
            ? "Task " + exitState + ", return codes = " + exitStatus + ", comments = " + exitComment
            : "Task " + exitState;
        alertService().generateAndBroadcastAlert("Algorithm Monitor", stateFile.getPipelineTaskId(),
            AlertService.Severity.ERROR, message);
    }

    private String taskStatusValues(StateFile stateFile) {
        Map<Long, Integer> exitStatus = jobMonitor().exitStatus(stateFile);
        if (exitStatus.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Integer> entry : exitStatus.entrySet()) {
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
     * @throws IOException
     */
    private void moveStateFileToCompleteState(StateFile stateFile) throws IOException {
        stateFile.setStateAndPersist(StateFile.State.COMPLETE, stateFileDirPath);
    }

    /**
     * Moves the state file to the DELETED state. This indicates to the {@link ComputeNodeMaster}
     * that the worker wants processing of the task to end.
     *
     * @param stateFile State file for failed job.
     * @throws IOException
     */
    private void moveStateFileToDeletedState(StateFile stateFile) throws IOException {
        stateFile.setStateAndPersist(StateFile.State.DELETED, stateFileDirPath);
    }

    private Disposition determineDisposition(StateFile state, PipelineTask pipelineTask) {

        // Deleted tasks are automatically regarded as failed.
        if (state.isDeleted()) {
            return Disposition.FAIL;
        }

        // The total number of bad subtasks includes both the ones that failed and the
        // ones that never ran / never finished. If there are few enough bad subtasks,
        // then we can persist results.
        if (state.getNumTotal() - state.getNumComplete() <= pipelineTask.maxFailedSubtasks()) {
            return Disposition.PERSIST;
        }

        // If the task has bad subtasks but the number of automatic resubmits hasn't
        // been exhausted, then resubmit.
        if (pipelineTask.getAutoResubmitCount() < pipelineTask.maxAutoResubmits()) {
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
     * Obtains a new PipelineTaskCrud. Replace with mocked method for unit testing.
     *
     * @return
     */
    PipelineTaskCrud pipelineTaskCrud() {
        return new PipelineTaskCrud();
    }

    /**
     * Obtains a new PipelineExecutor. Replace with mocked method for unit testing.
     *
     * @return
     */
    PipelineExecutor pipelineExecutor() {
        return new PipelineExecutor();
    }

    /**
     * Replace with mocked method for unit testing.
     */
    ProcessingSummaryOperations processingSummaryOperations() {
        return new ProcessingSummaryOperations();
    }

    /**
     * Replace with mocked method for unit testing.
     */
    PipelineTaskOperations pipelineTaskOperations() {
        return new PipelineTaskOperations();
    }

    /**
     * Replace with mocked method for unit testing.
     */
    AlertService alertService() {
        return AlertService.getInstance();
    }

    /**
     * Returns the polling interval, in milliseconds. Replace with mocked method for unit testing.
     *
     * @return
     */
    long pollingIntervalMillis() {
        return monitorRemoteJobs ? SSH_POLL_INTERVAL_MILLIS : LOCAL_POLL_INTERVAL_MILLIS;
    }

    /**
     * Stops the thread pool and replaces it. For testing only.
     */
    static void resetThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = new ScheduledThreadPoolExecutor(2);
    }

    /**
     * Gets the {@link StateFile} from the state {@link Map}. For testing only.
     */
    StateFile getStateFile(StateFile stateFile) {
        return state.get(stateFile.invariantPart());
    }

    private void initializeJobMonitor() {
        jobMonitor = JobMonitor.newInstance(username(), monitorRemoteJobs);
    }

    /**
     * Replace with mocked method for unit testing.
     */
    JobMonitor jobMonitor() {
        return jobMonitor;
    }
}
