package gov.nasa.ziggy.metrics;

import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.util.TimeRange;

public class MetricsCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(MetricsCrud.class);

    protected DatabaseService databaseService = null;

    public MetricsCrud() {
    }

    public MetricsCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public void createMetricType(MetricType metricType) {
        create(metricType);
    }

    public void createMetricValue(MetricValue metricValue) {
        create(metricValue);
    }

    public List<MetricType> retrieveAllMetricTypes() {
        Query q = createQuery("from MetricType");
        return list(q);
    }

    public List<MetricValue> retrieveAllMetricValuesForType(MetricType metricType, Date start,
        Date end) {

        Query q = createQuery(
            "from MetricValue where metricType = :metricType and timestamp >= :start and timestamp <= :end order by timestamp asc");
        q.setEntity("metricType", metricType);
        q.setParameter("start", start);
        q.setParameter("end", end);

        List<MetricValue> l = list(q);

        log.debug("num matches = " + l.size());

        return l;
    }

    public TimeRange getTimestampRange(MetricType metricType) {
        Query q = createQuery(
            "select min(timestamp), max(timestamp) from MetricValue where metricType = :metricType");
        q.setEntity("metricType", metricType);

        Object[] results = uniqueResult(q);

        Date min = (Date) results[0];
        Date max = (Date) results[1];

        return new TimeRange(min, max);
    }

    public int retrieveMetricValueRowCount() {
        Query q = createQuery("select count(*) from MetricValue");

        Number count = uniqueResult(q);

        return count.intValue();
    }

    public long retrieveMinimumId() {
        Query q = createQuery("select min(id) from MetricValue");

        Number minId = uniqueResult(q);

        return minId.longValue();
    }

    public int deleteOldMetrics(int maxRows) {
        log.info("Preparing to delete old rows from PI_METRIC_VALUE.  maxRows = " + maxRows);

        int rowCount = 0;
        int numRowsOverLimit = 0;
        int numUpdated = 0;

        do {
            rowCount = retrieveMetricValueRowCount();
            numRowsOverLimit = rowCount - maxRows;

            log.info("rowCount = " + rowCount);

            if (numRowsOverLimit > 0) {
                log.info("numRowsOverLimit = " + numRowsOverLimit);

                long minId = retrieveMinimumId();
                Query q = createQuery("delete from MetricValue where id <= :id");
                long idToDelete = minId + numRowsOverLimit;
                q.setParameter("id", idToDelete);
                int numUpdatedThisChunk = q.executeUpdate();

                log.info(
                    "deleted " + numUpdatedThisChunk + " rows (where id <= " + idToDelete + ")");

                numUpdated += numUpdatedThisChunk;
            } else {
                log.info("rowCount under the limit, no delete needed");
            }
        } while (numRowsOverLimit > 0);

        log.info("deleted a total of " + numUpdated + " rows.");

        return numUpdated;
    }
}
