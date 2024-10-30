package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
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
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
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
