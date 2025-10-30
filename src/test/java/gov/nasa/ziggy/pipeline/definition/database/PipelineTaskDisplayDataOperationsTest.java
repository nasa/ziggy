package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;

public class PipelineTaskDisplayDataOperationsTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils;

    @Before
    public void setUp() {
        pipelineTaskDataOperations = new PipelineTaskDataOperations();
        pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

        pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
    }

    @Test
    public void testPipelineTaskDisplayDataPipelineTask() {
        PipelineTaskDisplayData pipelineTaskDisplayData = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineOperationsTestUtils.getPipelineTasks().get(0));
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayData);
    }

    @Test
    public void testPipelineTaskDisplayDataPipelineInstance() {
        List<PipelineTaskDisplayData> pipelineTaskDisplayDataList = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineOperationsTestUtils.pipelineInstance());
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayDataList);
    }

    @Test
    public void testPipelineTaskDisplayDataPipelineInstanceNode() {
        List<PipelineTaskDisplayData> pipelineTaskDisplayDataList = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineOperationsTestUtils.pipelineInstanceNode());
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayDataList);
    }

    @Test
    public void testPipelineTaskDisplayDataPipelineTasks() {
        List<PipelineTaskDisplayData> pipelineTaskDisplayData = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineOperationsTestUtils.getPipelineTasks());
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayData);
    }

    @Test
    public void testPipelineTaskDisplayDataFromIds() {
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayDataFromIds(pipelineOperationsTestUtils.getPipelineTasks()
                .stream()
                .map(PipelineTask::getId)
                .toList()));
    }

    @Test
    public void testPipelineTaskDisplayData() {
        PipelineInstance pipelineInstance = pipelineOperationsTestUtils.pipelineInstance();
        List<PipelineTaskDisplayData> pipelineTasksDisplayData = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineInstance);
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTasksDisplayData);
        List<PipelineTaskDisplayData> completedTasksDisplayData = new ArrayList<>();
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData));
        assertTrue(CollectionUtils.isEmpty(completedTasksDisplayData));

        // Provide an instance of PipelineTaskDisplayData that the caller knows to be completed.
        completedTasksDisplayData.add(pipelineTasksDisplayData.get(0));
        List<PipelineTaskDisplayData> displayDataWithKnownCompletedTask = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData);
        assertEquals(completedTasksDisplayData.get(0), pipelineTasksDisplayData.get(0));
        assertEquals(1, completedTasksDisplayData.size());
        assertEquals(displayDataWithKnownCompletedTask.get(0), pipelineTasksDisplayData.get(0));
        assertEquals(displayDataWithKnownCompletedTask.get(1), pipelineTasksDisplayData.get(1));
        assertEquals(2, displayDataWithKnownCompletedTask.size());

        // Set one of the tasks to completed, but don't include it in the collection of tasks
        // already known to be completed. The completed task should be added to the completed
        // tasks collection in the method argument.
        completedTasksDisplayData.clear();
        pipelineTaskDataOperations.updateProcessingStep(
            pipelineTasksDisplayData.get(0).getPipelineTask(), ProcessingStep.COMPLETE);
        List<PipelineTaskDisplayData> displayDataWithUnknownCompletedTask = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData);
        assertEquals(completedTasksDisplayData.get(0).getPipelineTaskId(),
            pipelineTasksDisplayData.get(0).getPipelineTaskId());
        assertEquals(1, completedTasksDisplayData.size());
        assertEquals(displayDataWithUnknownCompletedTask.get(0).getPipelineTaskId(),
            pipelineTasksDisplayData.get(0).getPipelineTaskId());
        assertEquals(displayDataWithUnknownCompletedTask.get(1), pipelineTasksDisplayData.get(1));
        assertEquals(2, displayDataWithUnknownCompletedTask.size());
    }

    @Test
    public void testUpdatedPipelineTaskDisplayData() {
        PipelineInstance pipelineInstance = pipelineOperationsTestUtils.pipelineInstance();
        List<PipelineTaskDisplayData> completedTasksDisplayData = new ArrayList<>();
        List<PipelineTaskDisplayData> pipelineTasksDisplayData = pipelineTaskDisplayDataOperations
            .updatedPipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData);
        pipelineOperationsTestUtils.testPipelineTaskDisplayData(pipelineTasksDisplayData);
        assertTrue(CollectionUtils.isEmpty(completedTasksDisplayData));

        // Provide an instance of PipelineTaskDisplayData that the caller knows to be completed.
        // This should return only 1 display data instance.
        completedTasksDisplayData.add(pipelineTasksDisplayData.get(0));
        List<PipelineTaskDisplayData> displayDataWithKnownCompletedTask = pipelineTaskDisplayDataOperations
            .updatedPipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData);
        assertEquals(completedTasksDisplayData.get(0), pipelineTasksDisplayData.get(0));
        assertEquals(displayDataWithKnownCompletedTask.get(0), pipelineTasksDisplayData.get(1));
        assertEquals(1, displayDataWithKnownCompletedTask.size());

        // Set one of the tasks to completed, but don't include it in the collection of tasks
        // already known to be completed.
        completedTasksDisplayData.clear();
        pipelineTaskDataOperations.updateProcessingStep(
            pipelineTasksDisplayData.get(0).getPipelineTask(), ProcessingStep.COMPLETE);
        List<PipelineTaskDisplayData> displayDataWithUnknownCompletedTask = pipelineTaskDisplayDataOperations
            .updatedPipelineTaskDisplayData(pipelineInstance, completedTasksDisplayData);
        assertTrue(CollectionUtils.isEmpty(completedTasksDisplayData));
        assertEquals(pipelineTasksDisplayData.get(1), displayDataWithUnknownCompletedTask.get(1));
        assertEquals(pipelineTasksDisplayData.get(0).getPipelineTaskId(),
            displayDataWithUnknownCompletedTask.get(0).getPipelineTaskId());
        assertEquals(ProcessingStep.COMPLETE,
            displayDataWithUnknownCompletedTask.get(0).getProcessingStep());
        assertEquals(2, displayDataWithUnknownCompletedTask.size());
    }

    @Test
    public void testTaskCountsPipelineTask() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, 6, 5, 1);
        TaskCounts taskCounts = pipelineTaskDisplayDataOperations.taskCounts(pipelineTask);
        TaskCountsTest.testTotalSubtaskCounts(6, 5, 1, taskCounts);
    }

    @Test
    public void testTaskCountsPipelineInstanceNode() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        PipelineInstanceNode pipelineInstanceNode = pipelineOperationsTestUtils
            .pipelineInstanceNode();
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, 6, 5, 1);
        TaskCounts taskCounts = pipelineTaskDisplayDataOperations.taskCounts(pipelineInstanceNode);
        TaskCountsTest.testTotalSubtaskCounts(6, 5, 1, taskCounts);
    }

    @Test
    public void testTaskCountsListOfPipelineInstanceNode() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        List<PipelineInstanceNode> pipelineInstanceNodes = pipelineOperationsTestUtils
            .getPipelineInstanceNodes();
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, 6, 5, 1);
        TaskCounts taskCounts = pipelineTaskDisplayDataOperations.taskCounts(pipelineInstanceNodes);
        TaskCountsTest.testTotalSubtaskCounts(6, 5, 1, taskCounts);
    }
}
