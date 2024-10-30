package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * {@link DatabaseOperations} class to access fields from the {@link PipelineTaskData} table.
 *
 * @author PT
 * @author Bill Wohler
 */

public class PipelineTaskDisplayDataOperations extends DatabaseOperations {
    private final PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private final PipelineTaskDataCrud pipelineTaskDataCrud = new PipelineTaskDataCrud();
    private final PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();

    public PipelineTaskDisplayData pipelineTaskDisplayData(PipelineTask pipelineTask) {
        PipelineTaskData pipelineTaskData = performTransaction(() -> {
            PipelineTaskData taskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTask);
            initializeCollections(List.of(taskData));
            return taskData;
        });
        return new PipelineTaskDisplayData(pipelineTaskData);
    }

    public List<PipelineTaskDisplayData> pipelineTaskDisplayData(
        PipelineInstance pipelineInstance) {
        List<PipelineTask> pipelineTasks = performTransaction(
            () -> pipelineTaskCrud().retrieveTasksForInstance(pipelineInstance));
        return pipelineTaskDisplayData(pipelineTasks);
    }

    public List<PipelineTaskDisplayData> pipelineTaskDisplayData(
        PipelineInstanceNode pipelineInstanceNode) {
        return pipelineTaskDisplayDataForNodes(List.of(pipelineInstanceNode));
    }

    public List<PipelineTaskDisplayData> pipelineTaskDisplayDataForNodes(
        List<PipelineInstanceNode> pipelineInstanceNodes) {
        return pipelineTaskDisplayData(performTransaction(
            () -> pipelineInstanceNodeCrud().retrievePipelineTasks(pipelineInstanceNodes)));
    }

    public List<PipelineTaskDisplayData> pipelineTaskDisplayData(List<PipelineTask> pipelineTasks) {
        List<PipelineTaskData> pipelineTaskData = performTransaction(() -> {
            List<PipelineTaskData> taskData = pipelineTaskDataCrud()
                .retrievePipelineTaskData(pipelineTasks);
            initializeCollections(taskData);
            return taskData;
        });
        return createPipelineTaskDisplayData(pipelineTaskData);
    }

    private void initializeCollections(List<PipelineTaskData> taskData) {
        taskData.forEach(t -> Hibernate.initialize(t.getPipelineTaskMetrics()));
        taskData.forEach(t -> Hibernate.initialize(t.getRemoteJobs()));
    }

    private List<PipelineTaskDisplayData> createPipelineTaskDisplayData(
        List<PipelineTaskData> pipelineTaskData) {

        return pipelineTaskData.stream()
            .map(PipelineTaskDisplayData::new)
            .collect(Collectors.toList());
    }

    public TaskCounts taskCounts(PipelineTask pipelineTask) {
        return new TaskCounts(pipelineTaskDisplayData(List.of(pipelineTask)));
    }

    public TaskCounts taskCounts(PipelineInstanceNode pipelineInstanceNode) {
        return new TaskCounts(pipelineTaskDisplayData(pipelineInstanceNode));
    }

    public TaskCounts taskCounts(List<PipelineInstanceNode> pipelineInstanceNodes) {
        return new TaskCounts(pipelineTaskDisplayDataForNodes(pipelineInstanceNodes));
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineTaskDataCrud pipelineTaskDataCrud() {
        return pipelineTaskDataCrud;
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }
}
