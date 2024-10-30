package gov.nasa.ziggy.metrics;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.util.TimeRange;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.Root;

public class MetricsCrud extends AbstractCrud<MetricType> {
    private static final Logger log = LoggerFactory.getLogger(MetricsCrud.class);

    public List<MetricType> retrieveAllMetricTypes() {
        return list(createZiggyQuery(MetricType.class));
    }

    public List<MetricValue> metricValues(MetricType metricType, Date start, Date end) {
        ZiggyQuery<MetricValue, MetricValue> query = createZiggyQuery(MetricValue.class);
        query.column(MetricValue_.timestamp).between(start, end).ascendingOrder();
        query.column(MetricValue_.metricType).in(metricType);
        return list(query);
    }

    public TimeRange getTimestampRange(MetricType metricType) {
        ZiggyQuery<MetricValue, Object[]> query = createZiggyQuery(MetricValue.class,
            Object[].class);
        query.column(MetricValue_.timestamp).minMax();
        query.column(MetricValue_.metricType).in(metricType);
        Object[] results = uniqueResult(query);

        Date min = (Date) results[0];
        Date max = (Date) results[1];

        return new TimeRange(min, max);
    }

    public long deleteOldMetrics(int maxRows) {
        log.info("Preparing to delete old rows from PI_METRIC_VALUE, maxRows={}", maxRows);

        long rowCount = 0;
        long numRowsOverLimit = 0;
        long numUpdated = 0;

        do {
            rowCount = retrieveMetricValueRowCount();
            numRowsOverLimit = rowCount - maxRows;

            log.info("rowCount={}", rowCount);

            if (numRowsOverLimit > 0) {
                log.info("numRowsOverLimit={}", numRowsOverLimit);

                long minId = retrieveMinimumId();
                long idToDelete = minId + numRowsOverLimit - 1;
                CriteriaBuilder builder = createCriteriaBuilder();
                CriteriaDelete<MetricValue> query = builder.createCriteriaDelete(MetricValue.class);
                Root<MetricValue> root = query.from(MetricValue.class);
                query.where(builder.lessThanOrEqualTo(root.get("id"), idToDelete));
                int numUpdatedThisChunk = executeUpdate(query);

                log.info("Deleted {} rows (where id <= {})", numUpdatedThisChunk, idToDelete);

                numUpdated += numUpdatedThisChunk;
            } else {
                log.info("rowCount under the limit, no delete needed");
            }
        } while (numRowsOverLimit > 0);

        log.info("Deleted a total of {} rows.", numUpdated);

        return numUpdated;
    }

    private long retrieveMetricValueRowCount() {
        ZiggyQuery<MetricValue, Long> query = createZiggyQuery(MetricValue.class, Long.class);
        query.select(query.getBuilder().count(query.getRoot()));
        return uniqueResult(query);
    }

    private long retrieveMinimumId() {
        ZiggyQuery<MetricValue, Object[]> query = createZiggyQuery(MetricValue.class,
            Object[].class);
        query.column(MetricValue_.id).minMax();
        Object[] minMax = uniqueResult(query);
        return (long) minMax[0];
    }

    public List<Long> retrieveAllMetricIds() {
        ZiggyQuery<MetricValue, Long> query = createZiggyQuery(MetricValue.class, Long.class);
        query.column(MetricValue_.id).select();
        return list(query);
    }

    @Override
    public Class<MetricType> componentClass() {
        return MetricType.class;
    }
}
