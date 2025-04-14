package gov.nasa.ziggy.pipeline.definition.database;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.remote.RemoteAlgorithmExecutor;
import gov.nasa.ziggy.services.database.DatabaseOperations;
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
    private PipelineTaskDataCrud pipelineTaskDataCrud = new PipelineTaskDataCrud();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineNodeCrud pipelineNodeCrud = new PipelineNodeCrud();
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
            List<Long> staleTaskIds = pipelineTaskDataCrud().retrieveTasksWithStaleStates();
            List<PipelineTask> staleTasks = pipelineTaskCrud().retrieveAll(staleTaskIds);
            results.totalUpdatedTaskCount += staleTasks.size();
            for (PipelineTask task : staleTasks) {
                if (pipelineTaskDataOperations().haltRequested(task)) {
                    continue;
                }
                long instanceId = task.getPipelineInstanceId();
                long instanceNodeId = pipelineTaskCrud().retrievePipelineInstanceNode(task).getId();

                log.info("instanceId={}", instanceId);
                log.info("instanceNodeId={}", instanceNodeId);

                // If the task was executing remotely, and it was queued or executing, we can try
                // to resume monitoring on it -- the jobs may have continued to run while the
                // supervisor was down.
                if (unfinishedJobs(task)) {
                    ProcessingStep processingStep = pipelineTaskDataCrud()
                        .retrieveProcessingStep(task);
                    if (processingStep == ProcessingStep.QUEUED
                        || processingStep == ProcessingStep.EXECUTING) {
                        RemoteAlgorithmExecutor remoteExecutor = new RemoteAlgorithmExecutor(task);
                        if (remoteExecutor.resumeMonitoring()) {
                            continue;
                        }
                    }
                }
                results.uniqueInstanceIds.add(instanceId);
                log.info("Setting error flag on task {}", task);
                pipelineTaskDataOperations().taskErrored(task);
                pipelineTaskDataOperations().updateJobs(task);
            }
            log.info("totalUpdatedTaskCount={} in instances={}", results.totalUpdatedTaskCount,
                results.uniqueInstanceIds);

            return results;
        });
    }

    private boolean unfinishedJobs(PipelineTask pipelineTask) {
        Set<RemoteJob> remoteJobs = pipelineTaskDataOperations().remoteJobs(pipelineTask);
        if (remoteJobs.isEmpty()) {
            return false;
        }
        for (RemoteJob remoteJob : remoteJobs) {
            if (!remoteJob.isFinished()) {
                return true;
            }
        }
        return false;
    }

    public WorkerResources workerResourcesForTask(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineNodeCrud()
            .retrieveExecutionResources(pipelineTaskCrud().retrievePipelineNode(pipelineTask))
            .workerResources());
    }

    public PipelineNodeExecutionResources executionResources(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineNodeCrud()
            .retrieveExecutionResources(pipelineTaskCrud().retrievePipelineNode(pipelineTask)));
    }

    public ModelRegistry modelRegistry(PipelineTask pipelineTask) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineInstance(pipelineTask).getModelRegistry());
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

    public List<PipelineTask> pipelineTasks(Collection<Long> pipelineTaskIds) {
        return performTransaction(() -> pipelineTaskCrud().retrieveAll(pipelineTaskIds));
    }

    /**
     * Retrieves pipeline tasks for the given instance. Fields with collections are not populated.
     */
    public List<PipelineTask> pipelineTasks(PipelineInstance pipelineInstance) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveTasksForInstance(pipelineInstance));
    }

    public String pipelineName(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrievePipelineInstance(pipelineTask)
            .getPipeline()
            .getName());
    }

    public List<PipelineTask> tasksForPipelineNode(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud()
            .retrieveTasksForPipelineNode(pipelineTaskCrud().retrievePipelineNode(pipelineTask)));
    }

    public PipelineNode pipelineNode(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrievePipelineNode(pipelineTask));
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

    public PipelineInstance pipelineInstance(long pipelineTaskId) {
        return performTransaction(
            () -> pipelineTaskCrud().retrievePipelineInstance(pipelineTaskId));
    }

    public PipelineStep pipelineStep(PipelineTask pipelineTask) {
        return performTransaction(() -> pipelineTaskCrud().retrievePipelineStep(pipelineTask));
    }

    public PipelineStepExecutor pipelineStepExecutorImplementation(PipelineTask pipelineTask) {
        return pipelineStepExecutorImplementation(pipelineTask, RunMode.STANDARD);
    }

    public PipelineStepExecutor pipelineStepExecutorImplementation(PipelineTask pipelineTask,
        RunMode runMode) {
        ClassWrapper<PipelineStepExecutor> pipelineStepExecutorClassWrapper = pipelineStep(
            pipelineTask).getPipelineStepExecutorClass();
        try {
            return pipelineStepExecutorClassWrapper.constructor(PipelineTask.class, RunMode.class)
                .newInstance(pipelineTask, runMode);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
            // Can never occur. The PipelineStepExecutor has a constructor that takes PipelineTask
            // and RunMode as arguments.
            throw new AssertionError(e);
        }
    }

    public Map<PipelineTask, List<RunMode>> supportedRunModesByPipelineTask(
        List<PipelineTask> pipelineTasks) {
        Map<PipelineTask, List<RunMode>> supportedRunModesByPipelineTask = new HashMap<>();
        for (PipelineTask pipelineTask : pipelineTasks) {
            supportedRunModesByPipelineTask.put(pipelineTask,
                pipelineStepExecutorImplementation(pipelineTask).supportedRestartModes());
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

    public List<PipelineTask> tasksForPipelineNode(PipelineNode pipelineNode) {
        return performTransaction(
            () -> pipelineTaskCrud().retrieveTasksForPipelineNode(pipelineNode));
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

    PipelineTaskDataCrud pipelineTaskDataCrud() {
        return pipelineTaskDataCrud;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineNodeCrud pipelineNodeCrud() {
        return pipelineNodeCrud;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    public class ClearStaleStateResults {
        public int totalUpdatedTaskCount = 0;
        public Set<Long> uniqueInstanceIds = new HashSet<>();
    }
}
