package gov.nasa.ziggy.pipeline.definition.database;

import static gov.nasa.ziggy.services.process.AbstractPipelineProcess.getProcessInfo;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Hibernate;
import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.module.remote.QstatMonitor;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Provides methods that perform database access for {@link PipelineTask} instances.
 *
 * @author PT
 */
public class PipelineTaskOperations extends DatabaseOperations {
    private static final Logger log = LoggerFactory.getLogger(PipelineTaskOperations.class);

    public static final String CSV_REPORT_DELIMITER = ":";

    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

    /**
     * Set the error flag on stale tasks, which means they are in one of the
     * {@link ProcessingStep#processingSteps()}.
     * <p>
     * This is typically called by the supervisor during startup to clear the stale state of any
     * tasks that were processing when the supervisor exited abnormally (without a chance to set the
     * task's error flag).
     */
    public ClearStaleStateResults clearStaleTaskStates() {
        return performTransaction(() -> {
            ClearStaleStateResults results = new ClearStaleStateResults();
            List<PipelineTask> staleTasks = pipelineTaskCrud().retrieveTasksWithStaleStates();
            results.totalUpdatedTaskCount += staleTasks.size();
            for (PipelineTask task : staleTasks) {
                long instanceId = task.getPipelineInstanceId();
                long instanceNodeId = pipelineTaskCrud().retrievePipelineInstanceNode(task).getId();

                log.info("instanceId={}", instanceId);
                log.info("instanceNodeId={}", instanceNodeId);

                // If the task was executing remotely, and it was queued or executing, we can try
                // to resume monitoring on it -- the jobs may have continued to run while the
                // supervisor was down.
                if (task.getProcessingMode() != null
                    && task.getProcessingMode().equals(AlgorithmType.REMOTE)) {
                    ProcessingStep processingStep = task.getProcessingStep();
                    if (processingStep == ProcessingStep.QUEUED
                        || processingStep == ProcessingStep.EXECUTING) {
                        log.info("Resuming monitoring for task={}", task.getId());
                        TaskRequest taskRequest = new TaskRequest(instanceId, instanceNodeId,
                            pipelineTaskCrud().retrievePipelineDefinitionNode(task).getId(),
                            task.getId(), Priority.HIGHEST, false,
                            PipelineModule.RunMode.RESUME_MONITORING);
                        ZiggyMessenger.publish(taskRequest);
                        continue;
                    }
                }
                results.uniqueInstanceIds.add(instanceId);
                log.info("Setting error flag on task {}", task.getId());
                updateJobs(taskErrored(task));
            }
            log.info("totalUpdatedTaskCount={} in instances={}", results.totalUpdatedTaskCount,
                results.uniqueInstanceIds);

            return results;
        });
    }

    public WorkerResources workerResourcesForTask(long taskId) {
        return performTransaction(() -> pipelineDefinitionNodeCrud()
            .retrieveExecutionResources(pipelineTaskCrud()
                .retrievePipelineDefinitionNode(pipelineTaskCrud().retrieve(taskId)))
            .workerResources());
    }

    public PipelineDefinitionNodeExecutionResources executionResources(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineDefinitionNodeCrud()
            .retrieveExecutionResources(pipelineTaskCrud().retrievePipelineDefinitionNode(
                pipelineTaskCrud().retrieve(pipelineTask.getId()))));
    }

    public ModelRegistry modelRegistry(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineInstance(pipelineTask).getModelRegistry());
    }

    /** Prepares tasks for manual resubmission by resetting their auto-resubmit counts. */
    public List<PipelineTask> prepareTasksForManualResubmit(List<Long> taskIds) {
        return performTransaction(() -> {
            List<PipelineTask> tasksToResubmit = pipelineTaskCrud().retrieveAll(taskIds);
            for (PipelineTask pipelineTask : tasksToResubmit) {
                pipelineTask.resetAutoResubmitCount();
                pipelineTask.setRetry(true);
                pipelineTaskCrud().merge(pipelineTask);
            }
            return tasksToResubmit;
        });
    }

    public PipelineTask prepareTaskForAutoResubmit(PipelineTask pipelineTask) {
        return performTransaction(() -> {
            pipelineTask.incrementAutoResubmitCount();
            return taskErrored(pipelineTask);
        });
    }

    public void setLocalExecution(long pipelineTaskId) {
        performTransaction(() -> {
            PipelineTask pipelineTask = pipelineTaskCrud().retrieve(pipelineTaskId);
            pipelineTask.setRemoteExecution(false);
            pipelineTask.incrementTaskLogIndex();
            pipelineTaskCrud().merge(pipelineTask);
        });
    }

    public void setRemoteExecution(long pipelineTaskId) {
        performTransaction(() -> {
            PipelineTask pipelineTask = pipelineTaskCrud().retrieve(pipelineTaskId);
            pipelineTask.setRemoteExecution(true);
            pipelineTask.incrementTaskLogIndex();
            pipelineTaskCrud().merge(pipelineTask);
        });
    }

    /**
     * Merges a pipeline task and returns the merged task. The merge will not be attempted if the
     * call is made inside another transaction.
     */
    public PipelineTask merge(PipelineTask pipelineTask) {
        return performTransaction(new ReturningDatabaseTransaction<PipelineTask>() {
            @Override
            public boolean allowExistingTransaction() {
                return false;
            }

            @Override
            public PipelineTask transaction() throws Exception {
                return pipelineTaskCrud().merge(pipelineTask);
            }
        });
    }

    /** Retrieves a specified pipeline task. */
    public PipelineTask pipelineTask(long id) {
        return performTransaction(() -> pipelineTaskCrud().retrieve(id));
    }

    public List<PipelineTaskMetrics> summaryMetrics(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineTaskMetrics(pipelineTask.getId()));
    }

    public Map<PipelineTask, List<PipelineTaskMetrics>> taskMetricsByTask(
        PipelineInstanceNode node) {
        return performTransaction(() -> {
            HashMap<PipelineTask, List<PipelineTaskMetrics>> taskMetricsByTask = new HashMap<>();
            for (PipelineTask task : pipelineTasks(node)) {
                taskMetricsByTask.put(task, summaryMetrics(task));
            }
            return taskMetricsByTask;
        });
    }

    public List<TaskExecutionLog> execLogs(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrieveExecLogs(pipelineTask.getId()));
    }

    public List<PipelineTask> pipelineTasks(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(
            () -> pipelineInstanceNodeCrud().retrieve(pipelineInstanceNode.getId())
                .getPipelineTasks());
    }

    public List<PipelineTask> pipelineTasks(PipelineInstance pipelineInstance,
        Set<ProcessingStep> processingSteps) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveAll(pipelineInstance, processingSteps));
    }

    public List<PipelineTask> pipelineTasks(Collection<Long> pipelineTaskIds) {
        return performTransaction(() -> pipelineTaskCrud().retrieveAll(pipelineTaskIds));
    }

    public List<PipelineTask> erroredPipelineTasks(PipelineInstance pipelineInstance) {
        return performTransaction(() -> pipelineTaskCrud().retrieveErroredTasks(pipelineInstance));
    }

    /**
     * Retrieves pipeline tasks for the given instance. Fields with collections are not populated.
     */
    public List<PipelineTask> pipelineTasks(PipelineInstance pipelineInstance) {
        return pipelineTasks(pipelineInstance, false);
    }

    /**
     * Retrieves pipeline tasks.
     *
     * @param pipelineInstance retrieve tasks for the given instance
     * @param initialize if true, call {@link Hibernate#initialize(Object)} to populate the
     * following collections in each task: summaryMetrics, execLog, producerTaskIds
     */
    public List<PipelineTask> pipelineTasks(PipelineInstance pipelineInstance, boolean initialize) {
        return performTransaction(() -> {
            List<PipelineTask> pipelineTasks = pipelineTaskCrud()
                .retrieveTasksForInstance(pipelineInstance);
            if (initialize) {
                for (PipelineTask pipelineTask : pipelineTasks) {
                    Hibernate.initialize(pipelineTask.getSummaryMetrics());
                    Hibernate.initialize(pipelineTask.getExecLog());
                }
            }
            return pipelineTasks;
        });
    }

    /**
     * Creates a new {@link TaskExecutionLog} instance and attaches it to a given
     * {@link PipelineTask}. The merged pipeline task is returned.
     * <p>
     * We're doing it this way because there seems to be no way to do this if the new task execution
     * log is transient and the pipeline task is detached. Thus we do it all in one transaction.
     */
    public PipelineTask addTaskExecutionLog(long taskId, int workerNumber,
        long startProcessingTimeMillis) {
        return performTransaction(() -> {
            PipelineTask pipelineTask = pipelineTaskCrud().retrieve(taskId);
            TaskExecutionLog execLog = new TaskExecutionLog(getProcessInfo().getHost(),
                workerNumber);
            execLog.setStartProcessingTime(new Date(startProcessingTimeMillis));
            execLog.setInitialProcessingStep(pipelineTask.getProcessingStep());
            pipelineTask.getExecLog().add(execLog);
            return pipelineTaskCrud().merge(pipelineTask);
        });
    }

    /**
     * Creates instances of {@link RemoteJob} in the database using information obtained from the
     * output of the remote cluster's "qstat" command. The instances are initialized to estimated
     * cost of zero and unfinished status.
     */
    public void createRemoteJobsFromQstat(long pipelineTaskId) {

        performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask databaseTask = pipelineTaskCrud.retrieve(pipelineTaskId);
            QueueCommandManager queueCommandManager = queueCommandManager();
            QstatMonitor qstatMonitor = new QstatMonitor(queueCommandManager);
            qstatMonitor.addToMonitoring(databaseTask);
            qstatMonitor.update();
            Set<Long> allIncompleteJobIds = qstatMonitor.allIncompleteJobIds(databaseTask);
            log.info("Job IDs for task " + databaseTask.getId() + " from qstat : "
                + allIncompleteJobIds.toString());
            Set<RemoteJob> remoteJobs = databaseTask.getRemoteJobs();
            for (long jobId : allIncompleteJobIds) {
                remoteJobs.add(new RemoteJob(jobId));
            }
        });
    }

    QueueCommandManager queueCommandManager() {
        return QueueCommandManager.newInstance();
    }

    public PipelineTask updateJobs(PipelineTask pipelineTask) {
        return updateJobs(pipelineTask, false);
    }

    /**
     * Updates the state of all {@link RemoteJob}s associated with a {@link PipelineTask}. Any jobs
     * that have completed since the last update will be marked as finished and get their final cost
     * estimates calculated; any that are still running will get an up-to-the-minute cost estimate
     * calculated. If the boolean argument is true, all jobs associated with the pipeline task will
     * be moved to the finished state. This is useful when the pipeline task algorithm has
     * completed, and we want to ensure that all the jobs that were used for the task are correctly
     * recorded as complete.
     *
     * @return a new object with an updated remoteJobs property
     */
    public PipelineTask updateJobs(PipelineTask pipelineTask, boolean markJobsCompleted) {

        QueueCommandManager queueCommandManager = queueCommandManager();
        Set<RemoteJob> remoteJobs = remoteJobs(pipelineTask);
        for (RemoteJob job : remoteJobs) {
            if (job.isFinished()) {
                continue;
            }
            RemoteJob.RemoteJobQstatInfo jobInfo = queueCommandManager
                .remoteJobQstatInfo(job.getJobId());
            job.setCostEstimate(jobInfo.costEstimate());

            // Is the job finished?
            if (markJobsCompleted ? true : queueCommandManager.exitStatus(job.getJobId()) != null) {
                Log.info("Job " + job.getJobId() + " marked as finished");
                job.setFinished(true);
                log.info("Job " + job.getJobId() + " cost estimate: " + job.getCostEstimate());
            } else {
                log.info("Incomplete job " + job.getJobId() + " running cost estimate: "
                    + job.getCostEstimate());
            }
        }

        return performTransaction(() -> {
            PipelineTask databaseTask = pipelineTask(pipelineTask.getId());
            databaseTask.setRemoteJobs(remoteJobs);
            return pipelineTaskCrud().merge(databaseTask);
        });
    }

    Set<RemoteJob> remoteJobs(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveRemoteJobs(pipelineTask.getId()));
    }

    public String pipelineDefinitionName(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrievePipelineInstance(pipelineTask)
            .getPipelineDefinition()
            .getName());
    }

    public List<Long> taskIdsForPipelineDefinitionNode(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrieveIdsForPipelineDefinitionNode(
            pipelineTaskCrud().retrievePipelineDefinitionNode(pipelineTask)));
    }

    /**
     * Updates the subtask counts in the database.
     */
    public void updateSubtaskCounts(long taskId, int totalSubtaskCount, int completedSubtaskCount,
        int failedSubtaskCount) {
        performTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            PipelineTask task = crud.retrieve(taskId);
            task.setCompletedSubtaskCount(completedSubtaskCount);
            task.setFailedSubtaskCount(failedSubtaskCount);
            task.setTotalSubtaskCount(totalSubtaskCount);
            crud.merge(task);
        });
    }

    /**
     * Updates the processing step of a {@link PipelineTask} and simultaneously updates the state of
     * that task's {@link PipelineInstance} if need be. This is a safer method to use than the
     * "bare" {@link PipelineTask#setProcessingStep(ProcessingStep)} because it also automatically
     * updates the pipeline instance, and starts or stops execution clocks as needed.
     * <p>
     * This method first retrieves the pipeline task from the database; it then performs the updates
     * and merges the changes back to the database. Thus, this is a better method to use in cases
     * where a local copy of the pipeline task has changes that should not be merged to the
     * database.
     *
     * @return the updated and merged pipeline task, which must be used in subsequent processing
     */
    public PipelineTask updateProcessingStep(long taskId, ProcessingStep processingStep) {
        return updateProcessingStep(pipelineTaskCrud().retrieve(taskId), processingStep);
    }

    /**
     * Updates the processing step of a {@link PipelineTask} and simultaneously updates the state of
     * that task's {@link PipelineInstance} if need be. This is a safer method to use than the
     * "bare" {@link PipelineTask#setProcessingStep(ProcessingStep)} because it also automatically
     * updates the pipeline instance, and starts or stops execution clocks as needed.
     * <p>
     * This is the method to use if you have a detached pipeline task that contains changes which
     * should be persisted.
     *
     * @return the updated and merged pipeline task, which must be used in subsequent processing
     */
    public PipelineTask updateProcessingStep(PipelineTask task, ProcessingStep processingStep) {
        return performTransaction(() -> {
            task.setProcessingStep(processingStep);
            if (processingStep == ProcessingStep.INITIALIZING
                || processingStep == ProcessingStep.COMPLETE) {
                task.stopExecutionClock();
            } else {
                task.startExecutionClock();
            }
            PipelineTask mergedTask = pipelineTaskCrud().merge(task);
            updateInstanceState(mergedTask);

            return mergedTask;
        });
    }

    /**
     * Sets the {@link PipelineTask}'s error flag and simultaneously updates the state of that
     * task's {@link PipelineInstance} if need be. This is a safer method to use than the "bare"
     * {@link PipelineTask#setProcessingStep(gov.nasa.ziggy.pipeline.definition.ProcessingStep)}
     * because it also automatically updates the pipeline instance, and starts or stops execution
     * clocks as needed.
     * <p>
     * This method first retrieves the pipeline task from the database; it then performs the updates
     * and merges the changes back to the database. Thus, this is a better method to use in cases
     * where a local copy of the pipeline task has changes that should not be merged to the
     * database.
     *
     * @return the updated and merged pipeline task, which must be used in subsequent processing
     */
    public PipelineTask taskErrored(long taskId) {
        return taskErrored(pipelineTaskCrud().retrieve(taskId));
    }

    /**
     * Sets the {@link PipelineTask}'s error flag and simultaneously updates the state of that
     * task's {@link PipelineInstance} if need be. This is a safer method to use than the "bare"
     * {@link PipelineTask#setError(boolean)} because it also automatically updates the pipeline
     * instance, and stops the execution clock.
     * <p>
     * This is the method to use if you have a detached pipeline task that contains changes which
     * should be persisted.
     *
     * @return the updated and merged pipeline task, which must be used in subsequent processing
     */
    public PipelineTask taskErrored(PipelineTask task) {
        return performTransaction(() -> {
            task.setError(true);
            task.stopExecutionClock();
            PipelineTask mergedTask = pipelineTaskCrud().merge(task);
            updateInstanceState(mergedTask);

            return mergedTask;
        });
    }

    private PipelineInstance updateInstanceState(PipelineTask mergedTask) {

        PipelineInstance pipelineInstance = pipelineTaskCrud().retrievePipelineInstance(mergedTask);

        PipelineInstanceNode pipelineInstanceNode = pipelineTaskCrud()
            .retrievePipelineInstanceNode(mergedTask);
        TaskCounts instanceNodeCounts = pipelineInstanceNodeOperations()
            .taskCounts(pipelineInstanceNode);

        PipelineInstance.State state = PipelineInstance.State.INITIALIZED;
        if (new PipelineInstanceOperations().allInstanceNodesExecutionComplete(pipelineInstance)) {

            // If all the instance nodes are done, we can set the instance state to
            // either completed or stalled.
            state = instanceNodeCounts.isPipelineTasksComplete() ? PipelineInstance.State.COMPLETED
                : PipelineInstance.State.ERRORS_STALLED;
        } else if (instanceNodeCounts.isPipelineTasksExecutionComplete()) {

            // If the current node is done, then the state is either stalled or processing.
            state = instanceNodeCounts.isPipelineTasksComplete() ? PipelineInstance.State.PROCESSING
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
    }

    public PipelineTask clearError(long taskId) {
        return performTransaction(() -> {
            PipelineTask task = pipelineTaskCrud().retrieve(taskId);
            task.clearError();
            return pipelineTaskCrud().merge(task);
        });
    }

    public void incrementPipelineTaskLogIndex(long taskId) {
        performTransaction(() -> {
            PipelineTask task = pipelineTaskCrud().retrieve(taskId);
            task.incrementTaskLogIndex();
            pipelineTaskCrud.merge(task);
        });
    }

    public PipelineDefinitionNode pipelineDefinitionNode(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineDefinitionNode(pipelineTask));
    }

    public Set<DataFileType> inputDataFileTypes(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveInputDataFileTypes(pipelineTask));
    }

    public Set<DataFileType> outputDataFileTypes(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveOutputDataFileTypes(pipelineTask));
    }

    public Set<ModelType> modelTypes(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrieveModelTypes(pipelineTask));
    }

    public PipelineInstanceNode pipelineInstanceNode(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineInstanceNode(pipelineTask));
    }

    public long pipelineInstanceNodeId(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineInstanceNodeId(pipelineTask));
    }

    public PipelineInstance pipelineInstance(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrievePipelineInstance(pipelineTask));
    }

    public PipelineModuleDefinition pipelineModuleDefinition(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineModuleDefinition(pipelineTask));
    }

    public PipelineModule moduleImplementation(PipelineTask pipelineTask) {
        return moduleImplementation(pipelineTask, RunMode.STANDARD);
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public PipelineModule moduleImplementation(PipelineTask pipelineTask, RunMode runMode) {
        ClassWrapper<PipelineModule> moduleWrapper = pipelineModuleDefinition(pipelineTask)
            .getPipelineModuleClass();
        try {
            return moduleWrapper.constructor(PipelineTask.class, RunMode.class)
                .newInstance(pipelineTask, runMode);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
            // Can never occur. The PipelineModule has a constructor that takes PipelineTask
            // and RunMode as arguments.
            throw new AssertionError(e);
        }
    }

    public Map<PipelineTask, List<RunMode>> supportedRunModesByPipelineTask(
        List<PipelineTask> pipelineTasks) {
        Map<PipelineTask, List<RunMode>> supportedRunModesByPipelineTask = new HashMap<>();
        for (PipelineTask pipelineTask : pipelineTasks) {
            supportedRunModesByPipelineTask.put(pipelineTask,
                moduleImplementation(pipelineTask).supportedRestartModes());
        }
        return supportedRunModesByPipelineTask;
    }

    public List<PipelineTask> allPipelineTasks() {
        return performTransaction(() -> pipelineTaskCrud().retrieveAll());
    }

    public Set<ParameterSet> parameterSets(PipelineTask pipelineTask) {
        return performTransaction(() -> {
            Set<ParameterSet> parameterSets = pipelineInstanceCrud()
                .retrieveParameterSets(pipelineTaskCrud().retrievePipelineInstance(pipelineTask));
            parameterSets.addAll(pipelineInstanceNodeCrud().retrieveParameterSets(
                pipelineTaskCrud().retrievePipelineInstanceNode(pipelineTask)));
            return parameterSets;
        });
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        return pipelineDefinitionNodeCrud;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    public class ClearStaleStateResults {
        public int totalUpdatedTaskCount = 0;
        public Set<Long> uniqueInstanceIds = new HashSet<>();
    }
}
