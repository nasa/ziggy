package gov.nasa.ziggy.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Unit tests for {@link MetricsCrud} class.
 *
 * @author PT
 */
public class MetricsCrudTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testDeleteOldMetrics() {

        MetricsCrud crud = new MetricsCrud();

        // Create 10 metrics.
        DatabaseTransactionFactory.performTransaction(() -> {
            for (int i = 0; i < 10; i++) {
                crud.persist(new MetricValue("dummy", null, new Date(), 1.0F));
            }
            return null;
        });

        // Try a delete where maxRows > the actual number of rows.
        DatabaseTransactionFactory.performTransaction(() -> {
            crud.deleteOldMetrics(100);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            ZiggyQuery<MetricValue, Long> query = crud.createZiggyQuery(MetricValue.class,
                Long.class);
            query.column(MetricValue_.id).select();
            List<Long> ids = crud.list(query);
            assertEquals(10, ids.size());
            for (long i = 1; i <= 10; i++) {
                assertTrue(ids.contains(i));
            }
            return null;
        });

        // Now try a delete where some metrics will get deleted.
        DatabaseTransactionFactory.performTransaction(() -> {
            crud.deleteOldMetrics(5);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            ZiggyQuery<MetricValue, Long> query = crud.createZiggyQuery(MetricValue.class,
                Long.class);
            query.column(MetricValue_.id).select();
            List<Long> ids = crud.list(query);
            assertEquals(5, ids.size());
            for (long i = 6; i <= 10; i++) {
                assertTrue(ids.contains(i));
            }
            return null;
        });
    }
}
