package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.util.pojo.PojoTest;

/**
 * @author Todd Klaus
 */
public class ValueMetricTest {
    private static final String METRIC_1_NAME = "MetricsTest-1";
    private static final String METRIC_2_NAME = "MetricsTest-2";

    @Before
    public void setUp() {
        Metric.clear();
    }

    @Test
    public void testValueMetricEmpty() {
        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(0, m.getCount());
        assertEquals(0, m.getAverage(), 0);
        assertEquals(0, m.getSum());
        assertEquals(0, m.getMin());
        assertEquals(0, m.getMax());
    }

    @Test
    public void testValueMetricSimple() {
        ValueMetric.addValue(METRIC_1_NAME, 1);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(1, m.getCount());
        assertEquals(1, m.getAverage(), 0);
        assertEquals(1, m.getSum());
        assertEquals(1, m.getMin());
        assertEquals(1, m.getMax());
    }

    @Test
    public void testValueMetricMultiple() throws Exception {
        ValueMetric.addValue(METRIC_1_NAME, 2);
        ValueMetric.addValue(METRIC_1_NAME, 4);
        ValueMetric.addValue(METRIC_1_NAME, 6);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals(3, m.getCount());
        assertEquals(4, m.getAverage(), 0);
        assertEquals(12, m.getSum());
        assertEquals(2, m.getMin());
        assertEquals(6, m.getMax());
    }

    @Test
    public void testValueMetricMultiThread() throws Exception {
        Map<String, Metric> threadOneMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            ValueMetric.addValue(METRIC_1_NAME, 1);
            ValueMetric.addValue(METRIC_1_NAME, 1);

            return Metric.getThreadMetrics();
        });

        Map<String, Metric> threadTwoMetrics = executeSynchronous(() -> {
            Metric.enableThreadMetrics();

            ValueMetric.addValue(METRIC_1_NAME, 1);

            return Metric.getThreadMetrics();
        });

        ValueMetric metricGlobal = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();
        ValueMetric metricThreadOne = (ValueMetric) threadOneMetrics.get(METRIC_1_NAME);
        ValueMetric metricThreadTwo = (ValueMetric) threadTwoMetrics.get(METRIC_1_NAME);

        assertEquals(3, metricGlobal.getCount());
        assertEquals(2, metricThreadOne.getCount());
        assertEquals(1, metricThreadTwo.getCount());
    }

    private <T> T executeSynchronous(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> result = executor.submit(task);

        return result.get();
    }

    @Test
    public void testMakeCopy() {
        ValueMetric.addValue(METRIC_1_NAME, 1);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        ValueMetric copiedMetric = m.makeCopy();

        assertEquals(m, copiedMetric);
    }

    @Test
    public void testMerge() {
        ValueMetric.addValue(METRIC_1_NAME, 4);

        ValueMetric.addValue(METRIC_2_NAME, 2);
        ValueMetric.addValue(METRIC_2_NAME, 6);

        ValueMetric metric1 = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();
        ValueMetric metric2 = ValueMetric.getValueMetric(METRIC_2_NAME).getGlobalMetric();

        metric1.merge(metric2);

        assertEquals(3, metric1.getCount());
        assertEquals(4, metric1.getAverage(), 0);
        assertEquals(12, metric1.getSum());
        assertEquals(2, metric1.getMin());
        assertEquals(6, metric1.getMax());
    }

    @Test
    public void testReset() {
        ValueMetric.addValue(METRIC_1_NAME, 1);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        m.reset();

        ValueMetric.addValue(METRIC_1_NAME, 1);

        assertEquals(1, m.getCount());
        assertEquals(1, m.getAverage(), 0);
        assertEquals(1, m.getSum());
        assertEquals(1, m.getMin());
        assertEquals(1, m.getMax());
    }

    @Test
    public void testToLogString() {
        ValueMetric.addValue(METRIC_1_NAME, 1);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        StringBuilder builder = new StringBuilder();
        m.toLogString(builder);

        assertEquals("MetricsTest-1,V,1,1,1.0,1,1", builder.toString());
    }

    @Test
    public void testToString() {
        ValueMetric.addValue(METRIC_1_NAME, 1);

        ValueMetric m = ValueMetric.getValueMetric(METRIC_1_NAME).getGlobalMetric();

        assertEquals("mean: 1.0, min: 1, max: 1, count: 1, sum: 1", m.toString());
    }

    @Test
    public void testHashCodeEquals() {
        ValueMetric valueMetric = new ValueMetric("name", 1, 2, 3, 4);
        ValueMetric valueMetricWithSameKeys = new ValueMetric("name", 1, 2, 3, 4);
        ValueMetric valueMetricWithDifferentMin = new ValueMetric("name", 0, 2, 3, 4);
        ValueMetric valueMetricWithDifferentMax = new ValueMetric("name", 1, 0, 3, 4);
        ValueMetric valueMetricWithDifferentCount = new ValueMetric("name", 1, 2, 0, 4);
        ValueMetric valueMetricWithDifferentSum = new ValueMetric("name", 1, 2, 3, 0);
        ValueMetric valueMetricWithDifferentClass = new IntervalMetric("name");

        PojoTest.testHashCodeEquals(valueMetric, valueMetricWithSameKeys,
            valueMetricWithDifferentSum, valueMetricWithDifferentCount, valueMetricWithDifferentMin,
            valueMetricWithDifferentMax, valueMetricWithDifferentClass);
    }
}
