package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.util.SystemTime;

/**
 * @author Todd Klaus
 */
public class IntervalMetricTest {
    private static final String METRIC_1_NAME = "MetricsTest-1";

    @Before
    public void setUp() {
        Metric.clear();
    }

    @Test
    public void testIntervalMetricSimple() throws Exception {
        IntervalMetricKey key = IntervalMetric.start();
        long startTime = System.currentTimeMillis();
        SystemTime.setUserTime(startTime + 10L);
        IntervalMetric.stop(METRIC_1_NAME, key);

        IntervalMetric m = IntervalMetric.getIntervalMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(1, m.getCount());
        assertEquals(10, m.getAverage(), 1);
        assertEquals(10, m.getSum(), 1);
        assertEquals(10, m.getMin(), 1);
        assertEquals(10, m.getMax(), 1);
    }

    @Test
    public void testIntervalMetricMultiple() throws Exception {

        long totalTime = 0;
        IntervalMetricKey key = IntervalMetric.start();
        long startTime = System.currentTimeMillis();
        long intervalDuration = 100L;
        totalTime += intervalDuration;
        SystemTime.setUserTime(startTime + intervalDuration);
        IntervalMetric.stop(METRIC_1_NAME, key);

        key = IntervalMetric.start();
        startTime = System.currentTimeMillis();
        intervalDuration = 150L;
        totalTime += intervalDuration;
        SystemTime.setUserTime(startTime + intervalDuration);
        IntervalMetric.stop(METRIC_1_NAME, key);

        key = IntervalMetric.start();
        startTime = System.currentTimeMillis();
        intervalDuration = 200L;
        totalTime += intervalDuration;
        SystemTime.setUserTime(startTime + intervalDuration);
        IntervalMetric.stop(METRIC_1_NAME, key);

        IntervalMetric m = IntervalMetric.getIntervalMetric(METRIC_1_NAME).getGlobalMetric();

        int expectedCount = 3;
        double expectedSum = totalTime;
        double expectedAverage = expectedSum / expectedCount;

        // These deltas are somewhat large, but are needed because the
        // granularity of time slices could be large.
        assertEquals(expectedCount, m.getCount());
        assertEquals(expectedAverage, m.getAverage(), 1);
        assertEquals(expectedSum, m.getSum(), 1);
        assertEquals(100, m.getMin(), 1);
        assertEquals(200, m.getMax(), 1);
    }

    @Test
    public void testIntervalMetricMultiThread() throws Exception {
        Map<String, Metric> threadOneMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            IntervalMetricKey key = IntervalMetric.start();
            long startTime = System.currentTimeMillis();
            SystemTime.setUserTime(startTime + 100L);
            IntervalMetric.stop(METRIC_1_NAME, key);

            key = IntervalMetric.start();
            startTime = System.currentTimeMillis();
            SystemTime.setUserTime(startTime + 200L);
            IntervalMetric.stop(METRIC_1_NAME, key);

            return Metric.getThreadMetrics();
        });

        Map<String, Metric> threadTwoMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            IntervalMetricKey key = IntervalMetric.start();
            long startTime = System.currentTimeMillis();
            SystemTime.setUserTime(startTime + 150L);
            IntervalMetric.stop(METRIC_1_NAME, key);

            return Metric.getThreadMetrics();
        });

        IntervalMetric metricGlobal = IntervalMetric.getIntervalMetric(METRIC_1_NAME)
            .getGlobalMetric();
        IntervalMetric metricThreadOne = (IntervalMetric) threadOneMetrics.get(METRIC_1_NAME);
        IntervalMetric metricThreadTwo = (IntervalMetric) threadTwoMetrics.get(METRIC_1_NAME);

        // All of these tests have a large delta to accommodate a large
        // variation in context-switch latency.

        assertEquals(3, metricGlobal.getCount());
        assertEquals((100.0 + 200.0 + 150.0) / 3.0, metricGlobal.getAverage(), 1);
        assertEquals(450, metricGlobal.getSum(), 1);
        assertEquals(100, metricGlobal.getMin(), 1);
        assertEquals(200, metricGlobal.getMax(), 1);

        assertEquals(2, metricThreadOne.getCount());
        assertEquals((100.0 + 200.0) / 2.0, metricThreadOne.getAverage(), 1);
        assertEquals(300, metricThreadOne.getSum(), 1);
        assertEquals(100, metricThreadOne.getMin(), 1);
        assertEquals(200, metricThreadOne.getMax(), 1);

        assertEquals(1, metricThreadTwo.getCount());
        assertEquals(150.0 / 1.0, metricThreadTwo.getAverage(), 1);
        assertEquals(150, metricThreadTwo.getSum(), 1);
        assertEquals(150, metricThreadTwo.getMin(), 1);
        assertEquals(150, metricThreadTwo.getMax(), 1);
    }

    private <T> T executeSynchronous(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> result = executor.submit(task);

        return result.get();
    }
}
