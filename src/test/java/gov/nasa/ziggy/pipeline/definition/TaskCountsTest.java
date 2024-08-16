package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;

public class TaskCountsTest {

    private PipelineOperationsTestUtils pipelineOperationsTestUtils;

    @Before
    public void setUp() {
        pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFivePipelineTasks();
    }

    @Test
    public void testGetModuleCounts() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(new HashMap<>(), taskCounts.getModuleCounts());

        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTasks());
        Map<String, Counts> moduleCounts = taskCounts.getModuleCounts();
        assertEquals(5, moduleCounts.size());

        PipelineOperationsTestUtils.testCounts(1, 0, 0, 0, 10, 9, 1, moduleCounts.get("module1"));
        PipelineOperationsTestUtils.testCounts(1, 0, 0, 0, 20, 18, 2, moduleCounts.get("module2"));
        PipelineOperationsTestUtils.testCounts(0, 1, 0, 0, 30, 27, 3, moduleCounts.get("module3"));
        PipelineOperationsTestUtils.testCounts(0, 0, 0, 1, 40, 36, 4, moduleCounts.get("module4"));
        PipelineOperationsTestUtils.testCounts(0, 0, 1, 0, 50, 45, 5, moduleCounts.get("module5"));
    }

    @Test
    public void testGetModuleNames() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(new ArrayList<>(), taskCounts.getModuleNames());

        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTasks());
        assertEquals(5, taskCounts.getModuleNames().size());
        assertEquals("module1", taskCounts.getModuleNames().get(0));
        assertEquals("module2", taskCounts.getModuleNames().get(1));
        assertEquals("module3", taskCounts.getModuleNames().get(2));
        assertEquals("module4", taskCounts.getModuleNames().get(3));
        assertEquals("module5", taskCounts.getModuleNames().get(4));
    }

    @Test
    public void testGetTotalProcessingCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getProcessingTaskCount());
    }

    @Test
    public void testGetTotalErrorCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getFailedTaskCount());
    }

    @Test
    public void testGetTotalCompletedCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getCompletedTaskCount());
    }

    @Test
    public void testGetTotalSubTaskTotalCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getTotalSubtaskCount());
    }

    @Test
    public void testGetTotalSubTaskCompleteCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getCompletedSubtaskCount());
    }

    @Test
    public void testGetTotalSubTaskFailedCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTotalCounts().getFailedSubtaskCount());
    }
}
