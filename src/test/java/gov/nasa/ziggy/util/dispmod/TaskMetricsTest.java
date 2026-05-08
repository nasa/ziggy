package gov.nasa.ziggy.util.dispmod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.SystemProxy;

public class TaskMetricsTest {

    private static final long START_MILLIS = 1700000000000L;
    private static final long HOUR_MILLIS = 60 * 60 * 1000;
    private long totalDuration;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private List<Metric> metrics;
    private Iterator<Metric> metricsIterator;

    @Before
    public void setUp() {
        metrics = List.of(Metric.MARSHALING_TIME, Metric.PERSISTING_TIME, Metric.ALGORITHM_TIME,
            Metric.ALGORITHM_TIME, Metric.OUTPUTS_SIZE, Metric.PERSISTING_TIME,
            Metric.MARSHALING_TIME);
        metricsIterator = metrics.iterator();
    }

    @Test
    public void testHashCodeEquals() {
        TaskMetrics taskMetrics2 = taskMetrics(2);
        assertTrue(taskMetrics2.equals(taskMetrics2));
        assertFalse(taskMetrics2.equals(null));

        TaskMetrics taskMetrics3 = taskMetrics(3);

        TaskMetrics taskMetrics2Additional = taskMetrics(2);

        assertTrue(taskMetrics2.equals(taskMetrics2Additional));
        assertFalse(taskMetrics2.equals(taskMetrics3));

        assertEquals(taskMetrics2.hashCode(), taskMetrics2Additional.hashCode());
        assertNotEquals(taskMetrics2.hashCode(), taskMetrics3.hashCode());
    }

    @Test
    public void testGetCategoryMetrics() {
        Map<Metric, ValuePercentile> metricsByCategory = taskMetrics(3).getMetricsByCategory();
        assertEquals(3, metricsByCategory.size());
        checkCategoryMetrics(metricsByCategory.get(Metric.MARSHALING_TIME));
        checkCategoryMetrics(metricsByCategory.get(Metric.PERSISTING_TIME));
        checkCategoryMetrics(metricsByCategory.get(Metric.ALGORITHM_TIME));
    }

    private void checkCategoryMetrics(ValuePercentile metric) {
        assertNotNull(metric);
        assertEquals(0.00019, metric.getPercent(), 0.00001);
        assertEquals(42.0, metric.getValue(), 0.0001);
    }

    @Test
    public void testGetTotalProcessingTimeMillis() {
        TaskMetrics taskMetrics = taskMetrics(3);
        assertEquals(totalDuration, taskMetrics.getTotalProcessingTimeMillis());
    }

    private TaskMetrics taskMetrics(int taskCount) {
        return new TaskMetrics(pipelineTasks(taskCount));
    }

    private List<PipelineTaskDisplayData> pipelineTasks(int taskCount) {
        // Each task starts one hour after the last. The task duration starts at one hour and each
        // subsequent task is one hour longer.
        ArrayList<PipelineTaskDisplayData> pipelineTasks = new ArrayList<>();
        long startTime = START_MILLIS;
        for (int i = 0; i < taskCount; i++) {
            long duration = (i + 1) * HOUR_MILLIS;
            totalDuration += duration;
            pipelineTasks
                .add(pipelineTask("node" + i, new Date(startTime), new Date(startTime + duration)));
            startTime += duration + HOUR_MILLIS;
        }
        return pipelineTasks;
    }

    private PipelineTaskDisplayData pipelineTask(String pipelineStepName, Date start, Date end) {
        PipelineTask pipelineTask = Mockito
            .spy(new PipelineTask(null, null, new UnitOfWork(pipelineStepName)));
        doReturn(42L).when(pipelineTask).getId();

        PipelineTaskData pipelineTaskData = new PipelineTaskData(pipelineTask);
        pipelineTaskData.setPipelineTaskMetrics(pipelineTaskMetrics(pipelineStepName));
        PipelineTaskDisplayData pipelineTaskDisplayData = new PipelineTaskDisplayData(
            pipelineTaskData);
        SystemProxy.setUserTime(start.getTime());
        pipelineTaskDisplayData.getExecutionClock().start();
        SystemProxy.setUserTime(end.getTime());
        pipelineTaskDisplayData.getExecutionClock().stop();

        return pipelineTaskDisplayData;
    }

    private List<PipelineTaskMetric> pipelineTaskMetrics(String pipelineStepName) {
        PipelineTaskMetric pipelineTaskMetric = new PipelineTaskMetric(metricsIterator.next());
        pipelineTaskMetric.updateValue(42);
        return List.of(pipelineTaskMetric);
    }
}
