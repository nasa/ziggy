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

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class TaskMetricsTest {

    private static final long START_MILLIS = 1700000000000L;
    private static final long HOUR_MILLIS = 60 * 60 * 1000;
    private long totalDuration;

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        TaskMetrics taskMetrics = taskMetrics(2);
        assertTrue(taskMetrics.equals(taskMetrics));
        assertFalse(taskMetrics.equals(null));
        assertFalse(taskMetrics.equals("a string"));

        assertTrue(taskMetrics(2).equals(taskMetrics(2)));
        assertFalse(taskMetrics(2).equals(taskMetrics(3)));

        assertEquals(taskMetrics(2).hashCode(), taskMetrics(2).hashCode());
        assertNotEquals(taskMetrics(2).hashCode(), taskMetrics(3).hashCode());
    }

    @Test
    public void testGetCategoryMetrics() {
        TaskMetrics taskMetrics = taskMetrics(3);
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
        TimeAndPercentile unallocatedTime = taskMetrics.getUnallocatedTime();
        assertEquals(99.999, unallocatedTime.getPercent(), 0.01);
        assertEquals(2.16E7, unallocatedTime.getTimeMillis(), 0.01E7);
    }

    @Test
    public void testGetTotalProcessingTimeMillis() {
        TaskMetrics taskMetrics = taskMetrics(3);
        long totalProcessingTimeMillis = taskMetrics.getTotalProcessingTimeMillis();
        assertEquals(totalDuration, totalProcessingTimeMillis);
    }

    /**
     * This test reproduces the following error:
     *
     * <pre>
     * org.hibernate.LazyInitializationException: failed to lazily initialize a collection of role:
     * gov.nasa.ziggy.pipeline.definition.PipelineTask.summaryMetrics: could not initialize proxy -
     * no Session
     * </pre>
     *
     * This test is commented out as it is beyond the scope of this unit test and slows down the
     * test by a couple of orders of magnitude. However, it provides a good example of the
     * importance of creating TaskMetrics objects within the same transaction in which the pipeline
     * tasks were retrieved.
     */
//    @Test
    public void testCreateMetricsWithDatabaseTasks() {
        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineTaskCrud().persist(pipelineTasks(3));
            return null;
        });
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> pipelineTasks = new PipelineTaskCrud().retrieveAll();
            new TaskMetrics(pipelineTasks);
            return null;
        });
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
        return pipelineTask;
    }

    private List<PipelineTaskMetrics> summaryMetrics(String moduleName) {
        return List.of(new PipelineTaskMetrics(moduleName, 42, Units.TIME));
    }
}
