package gov.nasa.ziggy.module;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.PbsLogParser;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.module.remote.RemoteJobInformation;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.AllJobsFinishedMessage;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messages.TaskProcessingCompleteMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Monitors algorithm processing by monitoring state files.
 * <p>
 * Each task has an assigned {@link TaskMonitor} instance that periodically counts the states of
 * subtasks to determine the overall progress of that task. In addition, there is a periodic check
 * of PBS log files that allows the monitor to determine whether some or all of the remote jobs for
 * a given task have failed.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class AlgorithmMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmMonitor.class);

    private static final long REMOTE_POLL_INTERVAL_MILLIS = 10 * 1000; // 10 secs
    private static final long LOCAL_POLL_INTERVAL_MILLIS = 2 * 1000; // 2 seconds
    private static final long FINISHED_JOBS_POLL_INTERVAL_MILLIS = 10 * 1000;

    private ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(1);

    private boolean startLogMessageWritten = false;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineExecutor pipelineExecutor = new PipelineExecutor();
    private PbsLogParser pbsLogParser = new PbsLogParser();
    private QueueCommandManager queueCommandManager = QueueCommandManager.newInstance();

    private final Map<PipelineTask, List<RemoteJobInformation>> jobsInformationByTask = new ConcurrentHashMap<>();
    private final Map<PipelineTask, TaskMonitor> taskMonitorByTask = new ConcurrentHashMap<>();
    private AllJobsFinishedMessage allJobsFinishedMessage;

    // For testing only.
    private Disposition disposition;

    /** What needs to be done after a task exits the state file checks loop: */
    enum Disposition {

        // Algorithm processing is complete. Persist results.
        PERSIST {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                SubtaskCounts subtaskCounts = monitor.pipelineTaskDataOperations()
                    .subtaskCounts(pipelineTask);
                if (subtaskCounts.getFailedSubtaskCount() != 0) {
                    log.warn("{} subtasks out of {} failed but task completed",
                        subtaskCounts.getFailedSubtaskCount(),
                        subtaskCounts.getTotalSubtaskCount());
                    monitor.alertService()
                        .generateAndBroadcastAlert("Algorithm Monitor", pipelineTask,
                            Severity.WARNING, "Failed subtasks, see logs for details");
                }
                if (monitor.jobsInformationByTask.containsKey(pipelineTask)) {
                    monitor.jobsInformationByTask.remove(pipelineTask);
                }
                log.info("Sending task {} to worker to persist results", pipelineTask);

                monitor.pipelineExecutor().persistTaskResults(pipelineTask);
            }
        },

        // Algorithm processing has failures but resubmitting is allowed. Resubmit.
        RESUBMIT {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.warn("Resubmitting task {} for additional processing", pipelineTask);
                monitor.alertService()
                    .generateAndBroadcastAlert("Algorithm Monitor", pipelineTask, Severity.WARNING,
                        "Resubmitting task for further processing");
                monitor.pipelineTaskDataOperations().prepareTaskForAutoResubmit(pipelineTask);

                if (monitor.jobsInformationByTask.containsKey(pipelineTask)) {
                    monitor.jobsInformationByTask.remove(pipelineTask);
                }

                // Submit tasks for resubmission at highest priority.
                monitor.pipelineExecutor()
                    .restartFailedTasks(List.of(pipelineTask), false, RunMode.RESUBMIT);
            }
        },

        // Algorithm was killed or has failures but can't be resubmitted. Mark as
        // failed.
        FAIL {
            @Override
            public void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask) {
                log.error(
                    "Task {} failed on remote system, marking task as errored and not restarting",
                    pipelineTask);

                monitor.handleFailedTask(pipelineTask);
                monitor.pipelineTaskDataOperations().taskErrored(pipelineTask);
                if (monitor.jobsInformationByTask.containsKey(pipelineTask)) {
                    monitor.jobsInformationByTask.remove(pipelineTask);
                }
            }
        };

        /**
         * Performs the necessary actions for each of the enumerations.
         */
        public abstract void performActions(AlgorithmMonitor monitor, PipelineTask pipelineTask);
    }

    public AlgorithmMonitor() {
        ZiggyMessenger.subscribe(MonitorAlgorithmRequest.class, message -> {
            addToMonitor(message);
        });
        ZiggyMessenger.subscribe(TaskProcessingCompleteMessage.class, message -> {
            endTaskMonitoring(message.getPipelineTask());
        });
        startMonitoringThread();
    }

    /**
     * Start the monitoring thread.
     */
    private void startMonitoringThread() {
        long pollingIntervalMillis = finishedJobsPollingIntervalMillis();
        if (pollingIntervalMillis > 0) {
            log.info("Starting polling with {} msec interval", REMOTE_POLL_INTERVAL_MILLIS);
            threadPool.scheduleWithFixedDelay(this, 0, REMOTE_POLL_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
        }
    }

    // Protected access for unit tests.
    protected final void addToMonitor(MonitorAlgorithmRequest request) {
        List<RemoteJobInformation> remoteJobsInformation = request.getRemoteJobsInformation();
        log.info("Starting algorithm monitoring for task {}", request.getPipelineTask());
        if (!CollectionUtils.isEmpty(remoteJobsInformation)) {
            jobsInformationByTask.put(request.getPipelineTask(), remoteJobsInformation);
        }
        TaskMonitor taskMonitor = taskMonitor(request);
        taskMonitorByTask.put(request.getPipelineTask(), taskMonitor);
        taskMonitor.startMonitoring();
    }

    // Actions to be taken when a task monitor reports that a task is done.
    // Protected access for unit tests.
    protected final void endTaskMonitoring(PipelineTask pipelineTask) {

        // When the worker that persists the results exits, it will cause
        // this method to execute even though the algorithm is no longer under
        // monitoring. In that circumstance, exit now.
        if (!taskMonitorByTask.containsKey(pipelineTask)) {
            return;
        }

        log.info("End monitoring for task {}", pipelineTask);
        // update processing state
        pipelineTaskDataOperations().updateProcessingStep(pipelineTask,
            ProcessingStep.WAITING_TO_STORE);

        // It may be the case that all the subtasks are processed, but that
        // there are jobs still running, or (more likely) queued. We can address
        // that by deleting them from PBS.
        List<Long> jobIds = remoteJobIds(
            incompleteRemoteJobs(jobsInformationByTask.get(pipelineTask)));
        if (!CollectionUtils.isEmpty(jobIds)) {
            queueCommandManager().deleteJobsByJobId(jobIds);
        }

        if (jobsInformationByTask.containsKey(pipelineTask)) {
            updateRemoteJobs(pipelineTask);
        }

        taskMonitorByTask.remove(pipelineTask);

        // Figure out what needs to happen next, and do it.
        determineDisposition(pipelineTask).performActions(this, pipelineTask);
    }

    private void updateRemoteJobs(PipelineTask pipelineTask) {
        pipelineTaskDataOperations().updateJobs(pipelineTask, true);
    }

    /**
     * Returns the collection of remote job IDs that correspond to a given collection of pipeline
     * tasks, as a {@link Map}.
     */
    public Map<PipelineTask, List<Long>> jobIdsByTaskId(Collection<PipelineTask> pipelineTasks) {
        Map<PipelineTask, List<Long>> jobIdsByTaskId = new HashMap<>();
        if (jobsInformationByTask.size() == 0) {
            return jobIdsByTaskId;
        }
        for (PipelineTask pipelineTask : pipelineTasks) {
            List<RemoteJobInformation> remoteJobsInformation = jobsInformationByTask
                .get(pipelineTask);
            if (remoteJobsInformation != null) {
                jobIdsByTaskId.put(pipelineTask,
                    remoteJobIds(incompleteRemoteJobs(remoteJobsInformation)));
            }
        }
        return jobIdsByTaskId;
    }

    private List<RemoteJobInformation> incompleteRemoteJobs(
        Collection<RemoteJobInformation> remoteJobsInformation) {
        List<RemoteJobInformation> incompleteRemoteJobs = new ArrayList<>();
        if (CollectionUtils.isEmpty(remoteJobsInformation)) {
            return incompleteRemoteJobs;
        }
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            if (!Files.exists(Paths.get(remoteJobInformation.getLogFile()))) {
                incompleteRemoteJobs.add(remoteJobInformation);
            }
        }
        return incompleteRemoteJobs;
    }

    private List<Long> remoteJobIds(Collection<RemoteJobInformation> remoteJobsInformation) {
        return remoteJobsInformation.stream()
            .map(RemoteJobInformation::getJobId)
            .collect(Collectors.toList());
    }

    private void checkForFinishedJobs() {

        for (PipelineTask pipelineTask : jobsInformationByTask.keySet()) {
            if (isFinished(pipelineTask)) {
                publishFinishedJobsMessage(pipelineTask);
            }
        }
    }

    private void publishFinishedJobsMessage(PipelineTask pipelineTask) {
        allJobsFinishedMessage = new AllJobsFinishedMessage(pipelineTask);
        ZiggyMessenger.publish(allJobsFinishedMessage, false);
    }

    private boolean isFinished(PipelineTask pipelineTask) {
        List<RemoteJobInformation> remoteJobsInformation = jobsInformationByTask.get(pipelineTask);
        if (CollectionUtils.isEmpty(remoteJobsInformation)) {
            return false;
        }
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            if (!Files.exists(Paths.get(remoteJobInformation.getLogFile()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void run() {

        if (!startLogMessageWritten) {
            log.info("Algorithm monitor started...");
            startLogMessageWritten = true;
        }
        try {
            checkForFinishedJobs();
        } catch (Exception e) {
            log.warn("Task monitor: exception has occurred", e);
        }
    }

    /**
     * Handles a task for which processing has failed (either due to error or because the user
     * killed it). In this case, several actions need to be taken: the information about the cause
     * of the error has to be captured via qstat and logged locally; the pipeline task entry in the
     * database needs its TaskExecutionLog updated; the remote state file needs to be renamed to
     * indicate that the job errored.
     */
    private void handleFailedTask(PipelineTask pipelineTask) {

        // Get the exit code and comment.
        String exitStatus = taskStatusValues(pipelineTask);
        String exitComment = taskCommentValues(pipelineTask);

        log.error("Task {} has failed", pipelineTask);
        if (exitStatus != null) {
            log.error("Exit status from remote system for all jobs is {}", exitStatus);
        } else {
            log.error("No exit status provided");
            exitStatus = "not provided";
        }
        if (exitComment != null) {
            log.error("Exit comment from remote system is {}", exitComment);
        } else {
            log.error("No exit comment provided");
            exitComment = "not provided";
        }

        // issue an alert about the deletion
        String message = pipelineTaskDataOperations()
            .algorithmType(pipelineTask) == AlgorithmType.REMOTE
                ? "Task failed, return codes = " + exitStatus + ", comments = " + exitComment
                : "Task failed";
        alertService().generateAndBroadcastAlert("Algorithm Monitor", pipelineTask, Severity.ERROR,
            message);
    }

    private String taskStatusValues(PipelineTask pipelineTask) {
        Map<Long, Integer> exitStatusByJobId = pbsLogParser()
            .exitStatusByJobId(jobsInformationByTask.get(pipelineTask));
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

    private String taskCommentValues(PipelineTask pipelineTask) {
        Map<Long, String> exitCommentByJobId = pbsLogParser()
            .exitCommentByJobId(jobsInformationByTask.get(pipelineTask));
        if (exitCommentByJobId == null || exitCommentByJobId.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, String> entry : exitCommentByJobId.entrySet()) {
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

    private Disposition determineDisposition(PipelineTask pipelineTask) {

        // A task that was deliberately killed must be marked as failed regardless of
        // how many subtasks completed.
        if (taskIsKilled(pipelineTask)) {
            log.debug("Task {} was halted", pipelineTask.getId());
            disposition = Disposition.FAIL;
            return Disposition.FAIL;
        }
        // The total number of bad subtasks includes both the ones that failed and the
        // ones that never ran / never finished. If there are few enough bad subtasks,
        // then we can persist results.
        PipelineDefinitionNodeExecutionResources resources = pipelineTaskOperations()
            .executionResources(pipelineTask);
        SubtaskCounts subtaskCounts = pipelineTaskDataOperations().subtaskCounts(pipelineTask);
        log.debug("Number of subtasks for task {}: {}", pipelineTask.getId(),
            subtaskCounts.getTotalSubtaskCount());
        log.debug("Number of completed subtasks for task {}: {}", pipelineTask.getId(),
            subtaskCounts.getCompletedSubtaskCount());
        log.debug("Number of failed subtasks for task {}: {}", pipelineTask.getId(),
            subtaskCounts.getFailedSubtaskCount());
        if (subtaskCounts.getTotalSubtaskCount()
            - subtaskCounts.getCompletedSubtaskCount() <= resources.getMaxFailedSubtaskCount()) {
            disposition = Disposition.PERSIST;
            return Disposition.PERSIST;
        }

        // If the task has bad subtasks but the number of automatic resubmits hasn't
        // been exhausted, then resubmit.
        if (pipelineTaskDataOperations().autoResubmitCount(pipelineTask) < resources
            .getMaxAutoResubmits()) {
            disposition = Disposition.RESUBMIT;
            return Disposition.RESUBMIT;
        }

        // If we've gotten this far, then the task has to be considered as failed:
        // it has too many bad subtasks and has exhausted its automatic retries.
        disposition = Disposition.FAIL;
        return Disposition.FAIL;
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
    boolean taskIsKilled(PipelineTask pipelineTask) {
        return PipelineSupervisor.taskOnHaltedTaskList(pipelineTask);
    }

    long finishedJobsPollingIntervalMillis() {
        return FINISHED_JOBS_POLL_INTERVAL_MILLIS;
    }

    long remotePollIntervalMillis() {
        return REMOTE_POLL_INTERVAL_MILLIS;
    }

    long localPollIntervalMillis() {
        return LOCAL_POLL_INTERVAL_MILLIS;
    }

    TaskMonitor taskMonitor(MonitorAlgorithmRequest monitorAlgorithmRequest) {
        return new TaskMonitor(monitorAlgorithmRequest.getPipelineTask(),
            monitorAlgorithmRequest.getTaskDir().toFile(),
            CollectionUtils.isEmpty(monitorAlgorithmRequest.getRemoteJobsInformation())
                ? localPollIntervalMillis()
                : remotePollIntervalMillis());
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PbsLogParser pbsLogParser() {
        return pbsLogParser;
    }

    QueueCommandManager queueCommandManager() {
        return queueCommandManager;
    }

    // For testing only.
    Map<PipelineTask, TaskMonitor> getTaskMonitorByTask() {
        return taskMonitorByTask;
    }

    // For testing only.
    Map<PipelineTask, List<RemoteJobInformation>> getJobsInformationByTask() {
        return jobsInformationByTask;
    }

    // For testing only.
    Disposition getDisposition() {
        return disposition;
    }

    // For testing only.
    AllJobsFinishedMessage allJobsFinishedMessage() {
        return allJobsFinishedMessage;
    }
}
