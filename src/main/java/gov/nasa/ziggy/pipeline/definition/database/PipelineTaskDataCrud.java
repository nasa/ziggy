package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ExecutionClock;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData_;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import jakarta.persistence.metamodel.SingularAttribute;

public class PipelineTaskDataCrud extends AbstractCrud<PipelineTaskData> {

    private final PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

    /** For use by PipelineTaskDataOperations only. */
    PipelineTaskData retrievePipelineTaskData(PipelineTask pipelineTask) {
        return retrievePipelineTaskData(List.of(pipelineTask)).get(0);
    }

    /** For use by PipelineTaskDataOperations only. */
    PipelineTaskData retrievePipelineTaskData(long taskId) {
        return retrievePipelineTaskData(pipelineTaskCrud().retrieve(taskId));
    }

    /** For use by PipelineTaskDataOperations only. */
    List<PipelineTaskData> retrievePipelineTaskData(Collection<PipelineTask> pipelineTasks) {
        Set<Long> taskIds = new HashSet<>(
            pipelineTasks.stream().map(PipelineTask::getId).collect(Collectors.toSet()));

        List<PipelineTaskData> pipelineTaskData = list(
            createZiggyQuery(PipelineTaskData.class).column(PipelineTaskData_.pipelineTaskId)
                .in(taskIds)
                .ascendingOrder());

        // Create objects if not already present in database.
        Set<PipelineTask> newTasks = new HashSet<>(pipelineTasks);
        newTasks.removeAll(pipelineTaskData.stream()
            .map(PipelineTaskData::getPipelineTask)
            .collect(Collectors.toSet()));
        if (newTasks.size() > 0) {
            List<PipelineTaskData> newPipelineTaskData = new ArrayList<>();
            for (PipelineTask pipelineTask : newTasks) {
                newPipelineTaskData.add(new PipelineTaskData(pipelineTask));
            }
            persist(newPipelineTaskData);
            newPipelineTaskData.addAll(pipelineTaskData);
            Collections.sort(newPipelineTaskData);
            return newPipelineTaskData;
        }

        return pipelineTaskData;
    }

    ProcessingStep retrieveProcessingStep(PipelineTask pipelineTask) {
        return retrievePipelineTaskData(pipelineTask).getProcessingStep();
    }

    ExecutionClock retrieveExecutionClock(PipelineTask pipelineTask) {
        return retrievePipelineTaskData(pipelineTask).getExecutionClock();
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance} with errors.
     */
    List<PipelineTask> retrieveErroredTasks(PipelineInstance pipelineInstance) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class)
            .column(PipelineTask_.pipelineInstanceId)
            .in(pipelineInstance.getId());
        ZiggyQuery<PipelineTaskData, Long> taskIdQuery = query
            .ziggySubquery(PipelineTaskData.class, Long.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .select()
            .column(PipelineTaskData_.error)
            .in(true);
        query.column(PipelineTask_.id).in(taskIdQuery);
        return list(query);
    }

    /**
     * Retrieves the tasks for all {@link PipelineTask}s in the specified {@link PipelineInstance}
     * for the specified {@link ProcessingStep}s.
     */
    List<PipelineTask> retrievePipelineTasks(PipelineInstance pipelineInstance,
        Set<ProcessingStep> processingSteps) {

        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class)
            .column(PipelineTask_.pipelineInstanceId)
            .in(pipelineInstance.getId());
        ZiggyQuery<PipelineTaskData, Long> taskIdQuery = query
            .ziggySubquery(PipelineTaskData.class, Long.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .select()
            .column(PipelineTaskData_.processingStep)
            .in(processingSteps);
        query.column(PipelineTask_.id).select().in(taskIdQuery);

        // TODO When PipelineTaskData contains a PipelineTask, this query goes away
        return list(query).stream()
            .map(t -> pipelineTaskCrud().retrieve(t))
            .collect(Collectors.toList());
    }

    /**
     * Locates tasks that have "stale" states, i.e., tasks that were in process when the cluster was
     * shut down.
     */
    List<Long> retrieveTasksWithStaleStates() {
        return list(createZiggyQuery(PipelineTaskData.class, Long.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .select()
            .column(PipelineTaskData_.processingStep)
            .in(ProcessingStep.processingSteps())
            .column(PipelineTaskData_.error)
            .in(false)
            .distinct(true));
    }

    /**
     * Retrieves the list of distinct softwareRevisions for the specified node.
     */
    List<String> distinctSoftwareRevisions(PipelineInstanceNode pipelineInstanceNode) {
        List<String> distinctSoftwareRevisions = new ArrayList<>();

        distinctSoftwareRevisions.addAll(list(
            softwareRevisionQuery(pipelineInstanceNode, PipelineTaskData_.ziggySoftwareRevision)));
        distinctSoftwareRevisions.addAll(list(softwareRevisionQuery(pipelineInstanceNode,
            PipelineTaskData_.pipelineSoftwareRevision)));
        return distinctSoftwareRevisions;
    }

    private ZiggyQuery<PipelineTaskData, String> softwareRevisionQuery(
        PipelineInstanceNode pipelineInstanceNode,
        SingularAttribute<PipelineTaskData, String> versionType) {
        return createZiggyQuery(PipelineTaskData.class, String.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .in(pipelineTaskCrud().taskIdsForPipelineInstanceNode(pipelineInstanceNode))
            .column(versionType)
            .select()
            .column(versionType)
            .ascendingOrder()
            .distinct(true);
    }

    /**
     * Retrieves the list of distinct softwareRevisions for the specified pipeline instance.
     */
    List<String> distinctSoftwareRevisions(PipelineInstance pipelineInstance) {
        List<String> distinctSoftwareRevisions = new ArrayList<>();
        distinctSoftwareRevisions.addAll(
            list(softwareRevisionQuery(pipelineInstance, PipelineTaskData_.ziggySoftwareRevision)));
        distinctSoftwareRevisions.addAll(list(
            softwareRevisionQuery(pipelineInstance, PipelineTaskData_.pipelineSoftwareRevision)));
        return distinctSoftwareRevisions;
    }

    private ZiggyQuery<PipelineTaskData, String> softwareRevisionQuery(
        PipelineInstance pipelineInstance,
        SingularAttribute<PipelineTaskData, String> versionType) {
        ZiggyQuery<PipelineTaskData, String> softwareRevisionQuery = createZiggyQuery(
            PipelineTaskData.class, String.class).column(versionType)
                .select()
                .ascendingOrder()
                .distinct(true);
        ZiggyQuery<PipelineTask, Long> taskIdQuery = softwareRevisionQuery
            .ziggySubquery(PipelineTask.class, Long.class)
            .column(PipelineTask_.pipelineInstanceId)
            .in(pipelineInstance.getId())
            .column(PipelineTask_.id)
            .select();
        softwareRevisionQuery.column(PipelineTaskData_.pipelineTaskId).in(taskIdQuery);
        return softwareRevisionQuery;
    }

    public List<PipelineTaskMetric> retrievePipelineTaskMetrics(PipelineTask pipelineTask) {
        return list(createZiggyQuery(PipelineTaskData.class, PipelineTaskMetric.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .in(pipelineTask.getId())
            .column(PipelineTaskData_.pipelineTaskMetrics)
            .select());
    }

    public List<TaskExecutionLog> retrieveTaskExecutionLogs(PipelineTask pipelineTask) {
        return list(createZiggyQuery(PipelineTaskData.class, TaskExecutionLog.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .in(pipelineTask.getId())
            .column(PipelineTaskData_.taskExecutionLogs)
            .select());
    }

    public Set<RemoteJob> retrieveRemoteJobs(PipelineTask pipelineTask) {
        return new TreeSet<>(list(createZiggyQuery(PipelineTaskData.class, RemoteJob.class)
            .column(PipelineTaskData_.pipelineTaskId)
            .in(pipelineTask.getId())
            .column(PipelineTaskData_.remoteJobs)
            .select()));
    }

    @Override
    public Class<PipelineTaskData> componentClass() {
        return PipelineTaskData.class;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }
}
