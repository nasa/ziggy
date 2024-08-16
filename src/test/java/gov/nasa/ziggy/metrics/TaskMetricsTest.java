package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;

public class TaskMetricsTest {

    private static final long START_MILLIS = 1700000000000L;
    private static final long HOUR_MILLIS = 60 * 60 * 1000;
    private long totalDuration;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        TaskMetrics taskMetrics2 = taskMetrics(2);
        taskMetrics2.calculate();
        assertTrue(taskMetrics2.equals(taskMetrics2));
        assertFalse(taskMetrics2.equals(null));
        assertFalse(taskMetrics2.equals("a string"));

        TaskMetrics taskMetrics3 = taskMetrics(3);
        taskMetrics3.calculate();

        TaskMetrics taskMetrics2Additional = taskMetrics(2);
        taskMetrics2Additional.calculate();

        assertTrue(taskMetrics2.equals(taskMetrics2Additional));
        assertFalse(taskMetrics2.equals(taskMetrics3));

        assertEquals(taskMetrics2.hashCode(), taskMetrics2Additional.hashCode());
        assertNotEquals(taskMetrics2.hashCode(), taskMetrics3.hashCode());
    }

    @Test
    public void testGetCategoryMetrics() {
        TaskMetrics taskMetrics = taskMetrics(3);
        taskMetrics.calculate();
        Map<String, TimeAndPercentile> categoryMetrics = taskMetrics.getCategoryMetrics();
        assertEquals(3, categoryMetrics.size());
        checkCategoryMetrics(categoryMetrics.get("module0"));
        checkCategoryMetrics(categoryMetrics.get("module1"));
        checkCategoryMetrics(categoryMetrics.get("module2"));
    }

    private void checkCategoryMetrics(TimeAndPercentile timeAndPercentile) {
        assertNotNull(timeAndPercentile);
        assertEquals(0.00019, timeAndPercentile.getPercent(), 0.00001);
        assertEquals(42.0, timeAndPercentile.getTimeMillis(), 0.0001);
    }

    @Test
    public void testGetUnallocatedTime() {
        TaskMetrics taskMetrics = taskMetrics(3);
        taskMetrics.calculate();
        TimeAndPercentile unallocatedTime = taskMetrics.getUnallocatedTime();
        assertEquals(99.999, unallocatedTime.getPercent(), 0.01);
        assertEquals(2.16E7, unallocatedTime.getTimeMillis(), 0.01E7);
    }

    @Test
    public void testGetTotalProcessingTimeMillis() {
        TaskMetrics taskMetrics = taskMetrics(3);
        taskMetrics.calculate();
        long totalProcessingTimeMillis = taskMetrics.getTotalProcessingTimeMillis();
        assertEquals(totalDuration, totalProcessingTimeMillis);
    }

    private TaskMetrics taskMetrics(int taskCount) {
        return new TaskMetrics(pipelineTasks(taskCount));
    }

    private List<PipelineTask> pipelineTasks(int taskCount) {
        // Each task starts one hour after the last. The task duration starts at one hour and each
        // subsequent task is one hour longer.
        ArrayList<PipelineTask> pipelineTasks = new ArrayList<>();
        long startTime = START_MILLIS;
        for (int i = 0; i < taskCount; i++) {
            long duration = (i + 1) * HOUR_MILLIS;
            totalDuration += duration;
            pipelineTasks.add(
                pipelineTask("module" + i, new Date(startTime), new Date(startTime + duration)));
            startTime += duration + HOUR_MILLIS;
        }
        return pipelineTasks;
    }

    private PipelineTask pipelineTask(String moduleName, Date start, Date end) {
        PipelineTask pipelineTask = new PipelineTask();
        pipelineTask.setStartProcessingTime(start);
        pipelineTask.setEndProcessingTime(end);
        pipelineTask.setSummaryMetrics(summaryMetrics(moduleName));
        return new PipelineTaskOperations().merge(pipelineTask);
    }

    private List<PipelineTaskMetrics> summaryMetrics(String moduleName) {
        return List.of(new PipelineTaskMetrics(moduleName, 42, Units.TIME));
    }
}
