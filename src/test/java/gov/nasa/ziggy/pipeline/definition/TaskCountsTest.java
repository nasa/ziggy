package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;

public class TaskCountsTest {

    private PipelineOperationsTestUtils pipelineOperationsTestUtils;

    @Before
    public void setUp() {
        pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFivePipelineTaskDisplayData();
    }

    @Test
    public void testSubtaskCountsLabel() {
        assertEquals("1/2", TaskCounts.subtaskCountsLabel(1, 2, 0));
        assertEquals("1/2 (1)", TaskCounts.subtaskCountsLabel(1, 2, 1));
    }

    @Test
    public void testIsPipelineTasksComplete() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(true, taskCounts.isPipelineTasksComplete());
        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        assertEquals(false, taskCounts.isPipelineTasksComplete());
        taskCounts = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(4)));
        assertEquals(true, taskCounts.isPipelineTasksComplete());
    }

    @Test
    public void testIsPipelineTasksExecutionComplete() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(false, taskCounts.isPipelineTasksExecutionComplete());
        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        assertEquals(false, taskCounts.isPipelineTasksExecutionComplete());
        taskCounts = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(4)));
        assertEquals(true, taskCounts.isPipelineTasksExecutionComplete());
    }

    @Test
    public void testGetTaskCount() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(0, taskCounts.getTaskCount());
        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        assertEquals(5, taskCounts.getTaskCount());
    }

    @Test
    public void testGetModuleCounts() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(new HashMap<>(), taskCounts.getModuleCounts());

        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        Map<String, Counts> moduleCounts = taskCounts.getModuleCounts();
        assertEquals(5, moduleCounts.size());

        testCounts(1, 0, 0, 0, 10, 9, 1, moduleCounts.get("module1"));
        testCounts(1, 0, 0, 0, 20, 18, 2, moduleCounts.get("module2"));
        testCounts(0, 1, 0, 0, 30, 27, 3, moduleCounts.get("module3"));
        testCounts(0, 0, 0, 1, 40, 36, 4, moduleCounts.get("module4"));
        testCounts(0, 0, 1, 0, 50, 45, 5, moduleCounts.get("module5"));
    }

    @Test
    public void testGetModuleNames() {
        TaskCounts taskCounts = new TaskCounts();
        assertEquals(new ArrayList<>(), taskCounts.getModuleNames());

        taskCounts = new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        assertEquals(5, taskCounts.getModuleNames().size());
        assertEquals("module1", taskCounts.getModuleNames().get(0));
        assertEquals("module2", taskCounts.getModuleNames().get(1));
        assertEquals("module3", taskCounts.getModuleNames().get(2));
        assertEquals("module4", taskCounts.getModuleNames().get(3));
        assertEquals("module5", taskCounts.getModuleNames().get(4));
    }

    @Test
    public void testGetTotalCounts() {
        TaskCounts taskCounts = new TaskCounts(
            pipelineOperationsTestUtils.getPipelineTaskDisplayData());
        testCounts(2, 1, 1, 1, 150, 9 + 18 + 27 + 36 + 45, 15, taskCounts.getTotalCounts());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        TaskCounts taskCounts1 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(0)));
        TaskCounts taskCounts2 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(0)));

        assertEquals(taskCounts1.hashCode(), taskCounts2.hashCode());
        assertTrue(taskCounts1.equals(taskCounts1));
        assertTrue(taskCounts1.equals(taskCounts2));

        taskCounts2 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(1)));
        assertNotEquals(taskCounts1.hashCode(), taskCounts2.hashCode());
        assertFalse(taskCounts1.equals(taskCounts2));

        assertFalse(taskCounts1.equals(null));
        assertFalse(taskCounts1.equals("a string"));

        taskCounts1 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(2)));
        taskCounts2 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(2)));
        Counts totalCounts1 = taskCounts1.getTotalCounts();
        Counts totalCounts2 = taskCounts2.getTotalCounts();

        assertEquals(totalCounts1.hashCode(), totalCounts2.hashCode());
        assertTrue(totalCounts1.equals(totalCounts1));
        assertTrue(totalCounts1.equals(totalCounts2));

        taskCounts2 = new TaskCounts(
            List.of(pipelineOperationsTestUtils.getPipelineTaskDisplayData().get(4)));
        totalCounts2 = taskCounts2.getTotalCounts();
        assertNotEquals(totalCounts1.hashCode(), totalCounts2.hashCode());
        assertFalse(totalCounts1.equals(totalCounts2));

        assertFalse(totalCounts1.equals(null));
        assertFalse(totalCounts1.equals("a string"));
    }

    @Test
    public void testToString() {
        assertEquals(
            "taskCount=5, waitingToRunTaskCount=2, completedTaskCount=1, failedTaskCount=1",
            new TaskCounts(pipelineOperationsTestUtils.getPipelineTaskDisplayData()).toString());
    }

    public static void testTaskCounts(int taskCount, int waitingToRunTaskCount,
        int completedTaskCount, int failedTaskCount, TaskCounts counts) {
        assertEquals(taskCount, counts.getTaskCount());
        assertEquals(waitingToRunTaskCount, counts.getTotalCounts().getWaitingToRunTaskCount());
        assertEquals(completedTaskCount, counts.getTotalCounts().getCompletedTaskCount());
        assertEquals(failedTaskCount, counts.getTotalCounts().getFailedTaskCount());
    }

    public static void testCounts(int waitingToRunTaskCount, int processingTaskCount,
        int completedTaskCount, int failedTaskCount, int totalSubtaskCount,
        int completedSubtaskCount, int failedSubtaskCount, Counts counts) {
        assertEquals(waitingToRunTaskCount, counts.getWaitingToRunTaskCount());
        assertEquals(processingTaskCount, counts.getProcessingTaskCount());
        assertEquals(completedTaskCount, counts.getCompletedTaskCount());
        assertEquals(failedTaskCount, counts.getFailedTaskCount());
        assertEquals(totalSubtaskCount, counts.getTotalSubtaskCount());
        assertEquals(completedSubtaskCount, counts.getCompletedSubtaskCount());
        assertEquals(failedSubtaskCount, counts.getFailedSubtaskCount());
    }

    public static void testSubtaskCounts(int totalSubtaskCount, int completedSubtaskCount,
        int failedSubtaskCount, SubtaskCounts counts) {
        assertEquals(totalSubtaskCount, counts.getTotalSubtaskCount());
        assertEquals(completedSubtaskCount, counts.getCompletedSubtaskCount());
        assertEquals(failedSubtaskCount, counts.getFailedSubtaskCount());
    }

    public static void testTotalSubtaskCounts(int totalSubtaskCount, int completedSubtaskCount,
        int failedSubtaskCount, TaskCounts counts) {
        assertEquals(totalSubtaskCount, counts.getTotalCounts().getTotalSubtaskCount());
        assertEquals(completedSubtaskCount, counts.getTotalCounts().getCompletedSubtaskCount());
        assertEquals(failedSubtaskCount, counts.getTotalCounts().getFailedSubtaskCount());
    }
}
