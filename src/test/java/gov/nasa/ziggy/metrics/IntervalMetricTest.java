package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

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

        Thread.sleep(10);

        IntervalMetric.stop(METRIC_1_NAME, key);

        IntervalMetric m = IntervalMetric.getIntervalMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(1, m.getCount());
        assertEquals(10, m.getAverage(), 5);
        assertEquals(10, m.getSum(), 5);
        assertEquals(10, m.getMin(), 5);
        assertEquals(10, m.getMax(), 5);
    }

    @Test
    public void testIntervalMetricMultiple() throws Exception {
        long totalTime = 0;

        IntervalMetricKey key = IntervalMetric.start();
        long startTime = System.currentTimeMillis();

        Thread.sleep(100);

        totalTime += System.currentTimeMillis() - startTime;
        IntervalMetric.stop(METRIC_1_NAME, key);

        key = IntervalMetric.start();
        startTime = System.currentTimeMillis();

        Thread.sleep(150);

        totalTime += System.currentTimeMillis() - startTime;
        IntervalMetric.stop(METRIC_1_NAME, key);

        key = IntervalMetric.start();
        startTime = System.currentTimeMillis();

        Thread.sleep(200);

        totalTime += System.currentTimeMillis() - startTime;
        IntervalMetric.stop(METRIC_1_NAME, key);

        IntervalMetric m = IntervalMetric.getIntervalMetric(METRIC_1_NAME).getGlobalMetric();

        int expectedCount = 3;
        double expectedSum = totalTime;
        double expectedAverage = expectedSum / expectedCount;

        // These deltas are somewhat large, but are needed because the
        // granularity of time slices could be large.
        assertEquals(expectedCount, m.getCount());
        assertEquals(expectedAverage, m.getAverage(), 5);
        assertEquals(expectedSum, m.getSum(), 10);
        assertEquals(100, m.getMin(), 10);
        assertEquals(200, m.getMax(), 10);
    }

    @Test
    public void testIntervalMetricMultiThread() throws Exception {
        Map<String, Metric> threadOneMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            IntervalMetricKey key = IntervalMetric.start();

            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
            }

            IntervalMetric.stop(METRIC_1_NAME, key);

            key = IntervalMetric.start();

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
            }

            IntervalMetric.stop(METRIC_1_NAME, key);

            return Metric.getThreadMetrics();
        });

        Map<String, Metric> threadTwoMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            IntervalMetricKey key = IntervalMetric.start();

            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
            }

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
        assertEquals((100.0 + 200.0 + 150.0) / 3.0, metricGlobal.getAverage(), 20);
        assertEquals(450, metricGlobal.getSum(), 50);
        assertEquals(100, metricGlobal.getMin(), 50);
        assertEquals(200, metricGlobal.getMax(), 50);

        assertEquals(2, metricThreadOne.getCount());
        assertEquals((100.0 + 200.0) / 2.0, metricThreadOne.getAverage(), 20);
        assertEquals(300, metricThreadOne.getSum(), 50);
        assertEquals(100, metricThreadOne.getMin(), 50);
        assertEquals(200, metricThreadOne.getMax(), 50);

        assertEquals(1, metricThreadTwo.getCount());
        assertEquals(150.0 / 1.0, metricThreadTwo.getAverage(), 20);
        assertEquals(150, metricThreadTwo.getSum(), 50);
        assertEquals(150, metricThreadTwo.getMin(), 50);
        assertEquals(150, metricThreadTwo.getMax(), 50);
    }

    private <T> T executeSynchronous(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> result = executor.submit(task);

        return result.get();
    }
}
