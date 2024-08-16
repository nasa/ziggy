package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;

/**
 * Unit tests for {@link MetricsCrud} class.
 *
 * @author PT
 */
public class MetricsOperationsTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testDeleteOldMetrics() {

        MetricsOperations metricsOperations = new MetricsOperations();

        // Create 10 metrics.
        for (int i = 0; i < 10; i++) {
            metricsOperations.persist(new MetricValue("dummy", null, new Date(), 1.0F));
        }

        // Try a delete where maxRows > the actual number of rows.
        metricsOperations.deleteOldMetrics(100);
        List<Long> ids = metricsOperations.metricValueIds();
        assertEquals(10, ids.size());
        for (long i = 1; i <= 10; i++) {
            assertTrue(ids.contains(i));
        }

        metricsOperations.deleteOldMetrics(5);
        ids = metricsOperations.metricValueIds();
        assertEquals(5, ids.size());
        for (long i = 6; i <= 10; i++) {
            assertTrue(ids.contains(i));
        }
    }
}
