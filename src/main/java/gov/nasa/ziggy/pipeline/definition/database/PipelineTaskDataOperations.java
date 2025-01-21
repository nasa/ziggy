package gov.nasa.ziggy.pipeline.definition.database;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmType;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.module.remote.RemoteJobInformation;
import gov.nasa.ziggy.pipeline.definition.ExecutionClock;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.RemoteJob.RemoteJobQstatInfo;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceFinishedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;

/**
 * {@link DatabaseOperations} class to access fields from the {@link PipelineTaskData} table.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineTaskDataOperations extends DatabaseOperations {

    private static final Logger log = LoggerFactory.getLogger(PipelineTaskDataOperations.class);

    /**
     * Log filename format. This is used by {@link MessageFormat} to produce the filename for a log
     * file. The first element is the basename, "instanceId-taskId-moduleName" (i.e.,
     * "100-200-foo"). The second element is a job index, needed when running tasks on a remote
     * system that can produce multiple log files per task (i.e., one per remote job). The final
     * element is the task log index, a value that increments as a task gets executed or rerun,
     * which allows the logs to be sorted into the order in which they were generated.
     */
    private static final String LOG_FILENAME_FORMAT = "{0}.{1}-{2}.log";

    private final PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private final PipelineTaskDataCrud pipelineTaskDataCrud = new PipelineTaskDataCrud();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private final PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private final PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();

    public void createPipelineTaskData(PipelineTask pipelineTask, ProcessingStep processingStep) {
        PipelineTaskData pipelineTaskData = new PipelineTaskData(pipelineTask);
        if (processingStep != null) {
            pipelineTaskData.setProcessingStep(processingStep);
        }
        performTransaction(() -> pipelineTaskDataCrud().persist(pipelineTaskData));
    }

    public ProcessingStep processingStep(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrieveProcessingStep(pipelineTask));
    }

    public List<PipelineTask> pipelineTasks(PipelineInstance pipelineInstance,
        Set<ProcessingStep> processingSteps) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTasks(pipelineInstance, processingSteps));
    }

    /**
     * Updates the processing step of a {@link PipelineTask} and simultaneously starts or stops
     * execution clocks as needed and updates the state of that task's {@link PipelineInstance} if
     * need be.
     */
    public void updateProcessingStep(PipelineTask pipelineTask, ProcessingStep processingStep) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setProcessingStep(processingStep);
            if (processingStep == ProcessingStep.INITIALIZING
                || processingStep == ProcessingStep.COMPLETE) {
                pipelineTaskData.getExecutionClock().stop();
            } else {
                pipelineTaskData.getExecutionClock().start();
            }
        });
        updateInstanceState(pipelineTask);
    }

    public boolean hasErrored(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask).isError());
    }

    /**
     * Sets the error flag on the pipeline task and and simultaneously stops execution clock and
     * updates the state of that task's {@link PipelineInstance} if need be.
     */
    public void taskErrored(PipelineTask pipelineTask) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setError(true);
            pipelineTaskData.getExecutionClock().stop();
        });
        updateInstanceState(pipelineTask);
    }

    public void clearError(PipelineTask pipelineTask) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setError(false);
        });
    }

    /**
     * For testing only. Use {@link #taskErrored(PipelineTask)} or {@link #clearError(PipelineTask)}
     * instead.
     */
    public void setError(PipelineTask pipelineTask, boolean error) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setError(error);
        });
    }

    public boolean haltRequested(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask).isHaltRequested());
    }

    public void setHaltRequested(PipelineTask pipelineTask, boolean haltRequested) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setHaltRequested(haltRequested);
        });
    }

    private PipelineInstance updateInstanceState(PipelineTask pipelineTask) {
        PipelineInstance newPipelineInstance = null;
        try {
            newPipelineInstance = performTransaction(() -> {
                PipelineInstance pipelineInstance = pipelineTaskCrud()
                    .retrievePipelineInstance(pipelineTask);

                TaskCounts instanceNodeCounts = pipelineTaskDisplayDataOperations
                    .taskCounts(pipelineTaskCrud().retrievePipelineInstanceNode(pipelineTask));

                State state = PipelineInstance.State.INITIALIZED;
                if (allInstanceNodesExecutionComplete(pipelineInstance)) {

                    // If all the instance nodes are done, we can set the instance state to
                    // either completed or stalled.
                    state = instanceNodeCounts.isPipelineTasksComplete()
                        ? PipelineInstance.State.COMPLETED
                        : PipelineInstance.State.ERRORS_STALLED;
                } else if (instanceNodeCounts.isPipelineTasksExecutionComplete()) {

                    // If the current node is done, then the state is either stalled or processing.
                    state = instanceNodeCounts.isPipelineTasksComplete()
                        ? PipelineInstance.State.PROCESSING
                        : PipelineInstance.State.ERRORS_STALLED;
                } else {

                    // If the current instance node is still grinding away, then the state is either
                    // errors running or processing
                    state = instanceNodeCounts.getTotalCounts().getFailedTaskCount() == 0
                        ? PipelineInstance.State.PROCESSING
                        : PipelineInstance.State.ERRORS_RUNNING;
                }

                state.setExecutionClockState(pipelineInstance);
                pipelineInstance.setState(state);

                return pipelineInstanceCrud().merge(pipelineInstance);
            });
        } finally {
            if (newPipelineInstance != null) {
                State state = newPipelineInstance.getState();
                if (state == State.COMPLETED || state == State.TRANSITION_FAILED
                    || state == State.ERRORS_STALLED) {
                    ZiggyMessenger.publish(new PipelineInstanceFinishedMessage());
                }
            }
        }

        return newPipelineInstance;
    }

    /**
     * Determines whether all pipeline instances have completed execution. This means that all nodes
     * have tasks that submitted and all tasks for all nodes are either complete or errored.
     */
    private boolean allInstanceNodesExecutionComplete(PipelineInstance pipelineInstance) {
        List<PipelineInstanceNode> instanceNodes = pipelineInstanceNodeCrud()
            .retrieveAll(pipelineInstance);
        for (PipelineInstanceNode node : instanceNodes) {
            if (!pipelineTaskDisplayDataOperations().taskCounts(node)
                .isPipelineTasksExecutionComplete()) {
                return false;
            }
        }
        return true;
    }

    public List<PipelineTask> erroredPipelineTasks(PipelineInstance pipelineInstance) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrieveErroredTasks(pipelineInstance));
    }

    public ExecutionClock executionClock(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrieveExecutionClock(pipelineTask));
    }

    public void updateWorkerInfo(PipelineTask pipelineTask, String workerHost, int workerThread) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setWorkerHost(workerHost);
            pipelineTaskData.setWorkerThread(workerThread);
        });
    }

    public List<String> distinctSoftwareRevisions(PipelineInstance pipelineInstance) {
        return performTransaction(
            () -> pipelineTaskDataCrud().distinctSoftwareRevisions(pipelineInstance));
    }

    public List<String> distinctSoftwareRevisions(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(
            () -> pipelineTaskDataCrud().distinctSoftwareRevisions(pipelineInstanceNode));
    }

    public void updateZiggySoftwareRevision(PipelineTask pipelineTask, String softwareRevision) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setZiggySoftwareRevision(softwareRevision);
        });
    }

    public void updatePipelineSoftwareRevision(PipelineTask pipelineTask, String softwareRevision) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setPipelineSoftwareRevision(softwareRevision);
        });
    }

    public String pipelineSoftwareRevision(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineSoftwareRevision(pipelineTask));
    }

    public int autoResubmitCount(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask)
                .getAutoResubmitCount());
    }

    public boolean retrying(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask).isRetry());
    }

    public void prepareTasksForManualResubmit(Collection<PipelineTask> pipelineTasks) {
        performTransaction(() -> {
            List<PipelineTaskData> pipelineTaskDataList = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTasks);
            for (PipelineTaskData pipelineTaskData : pipelineTaskDataList) {
                pipelineTaskData.resetAutoResubmitCount();
                pipelineTaskData.setRetry(true);
            }
        });
    }

    public void prepareTaskForAutoResubmit(PipelineTask pipelineTask) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.incrementAutoResubmitCount();
            taskErrored(pipelineTask);
        });
    }

    public void prepareTaskForRestart(PipelineTask pipelineTask) {
        boolean taskErrored = hasErrored(pipelineTask);
        ProcessingStep processingStep = processingStep(pipelineTask);

        if (!taskErrored && (processingStep != ProcessingStep.COMPLETE
            || failedSubtaskCount(pipelineTask) <= 0)) {
            log.warn("Task {} is on step {} without errors, not restarting", pipelineTask,
                processingStep);
            return;
        }

        log.info("Restarting task {} on step {} {} errors", pipelineTask, processingStep,
            taskErrored ? "with" : "without");

        clearError(pipelineTask);
    }

    private int failedSubtaskCount(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask)
                .getFailedSubtaskCount());
    }

    public SubtaskCounts subtaskCounts(PipelineTask pipelineTask) {
        PipelineTaskData pipelineTaskData = performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask));
        return new SubtaskCounts(pipelineTaskData.getTotalSubtaskCount(),
            pipelineTaskData.getCompletedSubtaskCount(), pipelineTaskData.getFailedSubtaskCount());
    }

    /**
     * Updates the current subtask counts. Negative counts are ignored and are not transmitted to
     * the database.
     */
    public void updateSubtaskCounts(PipelineTask pipelineTask, int totalSubtaskCount,
        int completedSubtaskCount, int failedSubtaskCount) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            if (totalSubtaskCount >= 0) {
                pipelineTaskData.setTotalSubtaskCount(totalSubtaskCount);
            }
            if (completedSubtaskCount >= 0) {
                pipelineTaskData.setCompletedSubtaskCount(completedSubtaskCount);
            }
            if (failedSubtaskCount >= 0) {
                pipelineTaskData.setFailedSubtaskCount(failedSubtaskCount);
            }
        });
    }

    public void incrementFailureCount(PipelineTask pipelineTask) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.incrementFailureCount();
        });
    }

    public AlgorithmType algorithmType(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskData(pipelineTask).getAlgorithmType());
    }

    /** Sets the algorithm type for the given pipeline task. The task log index is incremented. */
    public void updateAlgorithmType(PipelineTask pipelineTask, AlgorithmType algorithmType) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setAlgorithmType(algorithmType);
            pipelineTaskData.incrementTaskLogIndex();
        });
    }

    public void incrementTaskLogIndex(PipelineTask pipelineTask) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.incrementTaskLogIndex();
        });
    }

    /**
     * Produces a file name for log files of the following form: {@code
     * <instanceId>-<taskId>-<moduleName>.<jobIndex>-<taskLogIndex>.log
     * }
     *
     * @param jobIndex index for the current job. Each pipeline task's algorithm execution can be
     * performed across multiple independent jobs; this index identifies a specific job out of the
     * set that are running for the current task.
     */
    public String logFilename(PipelineTask pipelineTask, int jobIndex) {
        return performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            return logFilename(pipelineTask, jobIndex, pipelineTaskData.getTaskLogIndex());
        });
    }

    /**
     * Produces a file name for log files like {@link #logFilename(PipelineTask, int)}, except the
     * taskLogIndex is given rather than obtained from the pipeline task. Good for testing without a
     * database.
     */
    public String logFilename(PipelineTask pipelineTask, int jobIndex, int taskLogIndex) {
        return MessageFormat.format(LOG_FILENAME_FORMAT, pipelineTask.taskBaseName(), jobIndex,
            taskLogIndex);
    }

    public List<PipelineTaskMetric> pipelineTaskMetrics(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrievePipelineTaskMetrics(pipelineTask));
    }

    public Map<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask(
        PipelineInstanceNode node) {
        return performTransaction(() -> {
            HashMap<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask = new HashMap<>();
            for (PipelineTask pipelineTask : pipelineInstanceNodeCrud()
                .retrievePipelineTasks(List.of(node))) {
                taskMetricsByTask.put(pipelineTask, pipelineTaskMetrics(pipelineTask));
            }
            return taskMetricsByTask;
        });
    }

    public void updatePipelineTaskMetrics(PipelineTask pipelineTask,
        List<PipelineTaskMetric> pipelineTaskMetrics) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setPipelineTaskMetrics(pipelineTaskMetrics);
        });
    }

    public List<TaskExecutionLog> taskExecutionLogs(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskDataCrud().retrieveTaskExecutionLogs(pipelineTask));
    }

    /**
     * Creates and saves a new {@link TaskExecutionLog} instance.
     */
    public void addTaskExecutionLog(PipelineTask pipelineTask, String workerHost, int workerThread,
        long startProcessingTimeMillis) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            TaskExecutionLog taskExecutionLog = new TaskExecutionLog(workerHost, workerThread);
            taskExecutionLog.setStartProcessingTime(new Date(startProcessingTimeMillis));
            taskExecutionLog.setInitialProcessingStep(pipelineTaskData.getProcessingStep());
            pipelineTaskData.getTaskExecutionLogs().add(taskExecutionLog);
        });
    }

    /**
     * Updates the end processing time and final processing step fields in the last task execution
     * log.
     */
    public void updateLastTaskExecutionLog(PipelineTask pipelineTask, Date endProcessingTime) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            List<TaskExecutionLog> taskExecutionLogs = pipelineTaskData.getTaskExecutionLogs();
            log.debug("taskExecutionLogs size={}", taskExecutionLogs.size());

            if (CollectionUtils.isEmpty(taskExecutionLogs)) {
                log.warn("Task execution log is missing or empty for task {}", pipelineTask);
                return;
            }
            TaskExecutionLog currentTaskExecutionLog = taskExecutionLogs
                .get(taskExecutionLogs.size() - 1);
            currentTaskExecutionLog.setEndProcessingTime(endProcessingTime);
            currentTaskExecutionLog.setFinalProcessingStep(pipelineTaskData.getProcessingStep());
        });
    }

    public Set<RemoteJob> remoteJobs(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskDataCrud().retrieveRemoteJobs(pipelineTask));
    }

    public void updateRemoteJobs(PipelineTask pipelineTask, Set<RemoteJob> remoteJobs) {
        performTransaction(() -> {
            PipelineTaskData pipelineTaskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            pipelineTaskData.setRemoteJobs(remoteJobs);
        });
    }

    /**
     * Updates all of the {@link PipelineTask} instances associated with a particular
     * {@link PipelineInstance}.
     */
    public void updateJobs(PipelineInstance pipelineInstance) {
        List<PipelineTask> tasks = performTransaction(
            () -> pipelineTaskCrud().retrieveTasksForInstance(pipelineInstance));
        for (PipelineTask task : tasks) {
            updateJobs(task);
        }
    }

    public void updateJobs(PipelineTask pipelineTask) {
        updateJobs(pipelineTask, false);
    }

    /**
     * Updates the state of all {@link RemoteJob}s associated with a {@link PipelineTask}. Any jobs
     * that have completed since the last update will be marked as finished and get their final cost
     * estimates calculated; any that are still running will get an up-to-the-minute cost estimate
     * calculated. If the boolean argument is true, all jobs associated with the pipeline task will
     * be moved to the finished state. This is useful when the pipeline task algorithm has
     * completed, and we want to ensure that all the jobs that were used for the task are correctly
     * recorded as complete.
     */
    public void updateJobs(PipelineTask pipelineTask, boolean markJobsCompleted) {
        QueueCommandManager queueCommandManager = queueCommandManager();
        Set<RemoteJob> remoteJobs = remoteJobs(pipelineTask);
        for (RemoteJob job : remoteJobs) {
            if (job.isFinished()) {
                continue;
            }
            RemoteJobQstatInfo jobInfo = queueCommandManager.remoteJobQstatInfo(job.getJobId());
            job.setCostEstimate(jobInfo.costEstimate());

            // Is the job finished?
            if (markJobsCompleted ? true : queueCommandManager.exitStatus(job.getJobId()) != null) {
                log.info("Job {} marked as finished", job.getJobId());
                job.setFinished(true);
                log.info("Job {} cost estimate is {}", job.getJobId(), job.getCostEstimate());
            } else {
                log.info("Incomplete job {} running cost estimate is {}", job.getJobId(),
                    job.getCostEstimate());
            }
        }

        updateRemoteJobs(pipelineTask, remoteJobs);
    }

    /**
     * Creates instances of {@link RemoteJob} in the database using information obtained from the
     * collection of {@link RemoteJobInformation} instances. The instances are initialized to
     * estimated cost of zero and unfinished status and are added to the current set of remote jobs.
     */
    public void addRemoteJobs(PipelineTask pipelineTask,
        List<RemoteJobInformation> remoteJobsInformation) {
        Set<RemoteJob> remoteJobs = remoteJobs(pipelineTask);
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            remoteJobs.add(new RemoteJob(remoteJobInformation.getJobId()));
        }
        updateRemoteJobs(pipelineTask, remoteJobs);
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineTaskDataCrud pipelineTaskDataCrud() {
        return pipelineTaskDataCrud;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }

    QueueCommandManager queueCommandManager() {
        return QueueCommandManager.newInstance();
    }
}
