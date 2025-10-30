package gov.nasa.ziggy.ui.instances;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;

/** Unit tests for the {@link TasksTableModel} class. */
public class TasksTableModelTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private PipelineOperationsTestUtils pipelineOperationsTestUtils;

    @Before
    public void setUp() {
        pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
    }

    @Test
    public void testPipelineTasksDisplayData() {

        // Invalid instance ID: no display data, instance remains null,
        // completed task collection empty.
        TasksTableModel tasksTableModel = new TasksTableModel();
        tasksTableModel.setPipelineInstanceId(0);
        List<PipelineTaskDisplayData> pipelineTasksDisplayData = tasksTableModel
            .pipelineTasksDisplayData();
        assertTrue(CollectionUtils.isEmpty(pipelineTasksDisplayData));
        assertNull(tasksTableModel.getPipelineInstance());
        assertTrue(CollectionUtils.isEmpty(tasksTableModel.getCompletedTaskData()));
        assertTrue(CollectionUtils.isEmpty(tasksTableModel.getCompletedTaskData()));

        // Valid instance ID: display data returned, instance populated,
        // completed task collection empty.
        tasksTableModel
            .setPipelineInstanceId(pipelineOperationsTestUtils.pipelineInstance().getId());
        pipelineTasksDisplayData = tasksTableModel.pipelineTasksDisplayData();
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTasksDisplayData);
        assertEquals(pipelineOperationsTestUtils.pipelineInstance(),
            tasksTableModel.getPipelineInstance());
        assertTrue(CollectionUtils.isEmpty(tasksTableModel.getCompletedTaskData()));

        // Completed pipeline task: completed task data gets updated.
        new PipelineTaskDataOperations().updateProcessingStep(
            pipelineTasksDisplayData.get(0).getPipelineTask(), ProcessingStep.COMPLETE);
        List<PipelineTaskDisplayData> pipelineTasksDisplayDataWithCompleted = tasksTableModel
            .pipelineTasksDisplayData();
        List<PipelineTaskDisplayData> completedTaskData = tasksTableModel.getCompletedTaskData();
        assertEquals(pipelineTasksDisplayData.get(0).getPipelineTaskId(),
            completedTaskData.get(0).getPipelineTaskId());
        assertEquals(ProcessingStep.COMPLETE, completedTaskData.get(0).getProcessingStep());
        assertEquals(1, completedTaskData.size());
        assertEquals(completedTaskData.get(0), pipelineTasksDisplayDataWithCompleted.get(0));
        assertEquals(pipelineTasksDisplayData.get(1), pipelineTasksDisplayDataWithCompleted.get(1));
        assertEquals(2, pipelineTasksDisplayDataWithCompleted.size());

        // Same operation, should get the same results even though there's now a
        // known completed task.
        List<PipelineTaskDisplayData> pipelineTasksDisplayDataWithKnownCompleted = tasksTableModel
            .pipelineTasksDisplayData();
        assertEquals(pipelineTasksDisplayDataWithCompleted.get(0),
            pipelineTasksDisplayDataWithKnownCompleted.get(0));
        assertEquals(pipelineTasksDisplayDataWithCompleted.get(1),
            pipelineTasksDisplayDataWithKnownCompleted.get(1));
        completedTaskData = tasksTableModel.getCompletedTaskData();
        assertEquals(pipelineTasksDisplayData.get(0).getPipelineTaskId(),
            completedTaskData.get(0).getPipelineTaskId());
        assertEquals(ProcessingStep.COMPLETE, completedTaskData.get(0).getProcessingStep());
        assertEquals(1, completedTaskData.size());
    }

    @Test
    public void testPipelineTasksDisplayDataRetrieveAll() {
        TasksTableModel tasksTableModel = new TasksTableModel();
        new PipelineTaskDataOperations().updateProcessingStep(
            pipelineOperationsTestUtils.getPipelineTasks().get(0), ProcessingStep.COMPLETE);
        assertTrue(CollectionUtils.isEmpty(tasksTableModel.getCompletedTaskData()));
        tasksTableModel
            .setPipelineInstanceId(pipelineOperationsTestUtils.pipelineInstance().getId());
        List<PipelineTaskDisplayData> pipelineTasksDisplayData = tasksTableModel
            .pipelineTasksDisplayData();
        assertEquals(pipelineOperationsTestUtils.getPipelineTasks().get(0).getId().longValue(),
            pipelineTasksDisplayData.get(0).getPipelineTaskId());
        assertEquals(pipelineOperationsTestUtils.getPipelineTasks().get(1).getId().longValue(),
            pipelineTasksDisplayData.get(1).getPipelineTaskId());
        assertEquals(ProcessingStep.COMPLETE, pipelineTasksDisplayData.get(0).getProcessingStep());
        assertEquals(2, pipelineTasksDisplayData.size());
        List<PipelineTaskDisplayData> completedTaskData = tasksTableModel.getCompletedTaskData();
        assertEquals(pipelineTasksDisplayData.get(0).getPipelineTaskId(),
            completedTaskData.get(0).getPipelineTaskId());
        assertEquals(ProcessingStep.COMPLETE, completedTaskData.get(0).getProcessingStep());
        assertEquals(1, completedTaskData.size());
    }
}
