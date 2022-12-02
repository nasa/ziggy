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
public class CounterMetricTest {
    private static final String METRIC_1_NAME = "MetricsTest-1";

    @Before
    public void setUp() {
        Metric.clear();
    }

    @Test
    public void testCounterMetricSimple() throws Exception {
        CounterMetric m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();

        CounterMetric.increment(METRIC_1_NAME);

        m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(1, m.getCount());
    }

    @Test
    public void testCounterMetricMultiple() throws Exception {
        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.decrement(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);

        CounterMetric m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(3, m.getCount());
    }

    @Test
    public void testCounterMetricMultiThread() throws Exception {
        Map<String, Metric> threadOneMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            CounterMetric.increment(METRIC_1_NAME);
            CounterMetric.increment(METRIC_1_NAME);

            return Metric.getThreadMetrics();
        });

        Map<String, Metric> threadTwoMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            CounterMetric.increment(METRIC_1_NAME);

            return Metric.getThreadMetrics();
        });

        CounterMetric metricGlobal = CounterMetric.getCounterMetric(METRIC_1_NAME)
            .getGlobalMetric();
        CounterMetric metricThreadOne = (CounterMetric) threadOneMetrics.get(METRIC_1_NAME);
        CounterMetric metricThreadTwo = (CounterMetric) threadTwoMetrics.get(METRIC_1_NAME);

        assertEquals(3, metricGlobal.getCount());
        assertEquals(2, metricThreadOne.getCount());
        assertEquals(1, metricThreadTwo.getCount());
    }

    private <T> T executeSynchronous(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> result = executor.submit(task);

        return result.get();
    }
}
