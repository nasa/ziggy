package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Todd Klaus
 */
public class MetricTest {
    private static final String METRIC_1_NAME = "MetricsTest-1";
    private static final String SERIALIZED_METRICS_FILE_PATH = "/tmp/metrics.ser";

    @Before
    public void setUp() {
        Metric.clear();

        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
    }

    @Test
    public void testPersist() throws Exception {
        Metric.persist(SERIALIZED_METRICS_FILE_PATH);

        Metric.clear();

        CounterMetric m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();
        assertEquals(0, m.getCount());

        Metric.merge(SERIALIZED_METRICS_FILE_PATH);

        m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();
        assertEquals(3, m.getCount());
    }

    @Test
    public void testLog() {
        Metric.log();
    }
}
