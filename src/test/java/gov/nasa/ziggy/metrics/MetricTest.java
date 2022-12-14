package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;

/**
 * @author Todd Klaus
 */
public class MetricTest {
    private static final String METRIC_1_NAME = "MetricsTest-1";
    private static final String SERIALIZED_METRICS_FILE_PATH = "metrics.ser";

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {
        Metric.clear();

        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
        CounterMetric.increment(METRIC_1_NAME);
    }

    @Test
    public void testPersist() throws Exception {
        Metric.persist(directoryRule.directory().resolve(SERIALIZED_METRICS_FILE_PATH).toString());

        Metric.clear();

        CounterMetric m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();
        assertEquals(0, m.getCount());

        Metric.merge(directoryRule.directory().resolve(SERIALIZED_METRICS_FILE_PATH).toString());

        m = CounterMetric.getCounterMetric(METRIC_1_NAME).getGlobalMetric();
        assertEquals(3, m.getCount());
    }

    @Test
    public void testLog() {
        Metric.log();
    }
}
