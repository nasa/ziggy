package gov.nasa.ziggy.services.alert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.criterion.Disjunction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import gov.nasa.ziggy.collections.ListChunkIterator;
import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * This class provides CRUD methods for the AlertService
 *
 * @author Bill Wohler
 * @author Todd Klaus
 */
public class AlertLogCrud extends AbstractCrud {
    /**
     * Creates an {@link AlertLogCrud} object.
     */
    public AlertLogCrud() {
    }

    /**
     * Creates an {@link AlertLogCrud} object.
     *
     * @param databaseService the database service
     */
    public AlertLogCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    /**
     * Retrieves the names of all components that have logged alerts to the database.
     *
     * @return a non-{@code null} list of matching components names
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<String> retrieveComponents() {
        Criteria query = createCriteria(AlertLog.class);
        query.setProjection(Projections
            .distinct(Projections.groupProperty("alertData.sourceComponent").as("component")));
        query.addOrder(Order.asc("component"));
        return list(query);
    }

    /**
     * Retrieves the names of all severities that have been logged in the database.
     *
     * @return a non-{@code null} list of matching severities
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<String> retrieveSeverities() {
        Criteria query = createCriteria(AlertLog.class);
        query.setProjection(
            Projections.distinct(Projections.groupProperty("alertData.severity").as("severity")));
        query.addOrder(Order.asc("severity"));
        return list(query);
    }

    /**
     * Retrieves all {@link AlertLog} objects during the given time range.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @return a non-{@code null} list of matching {@link AlertLog} objects
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<AlertLog> retrieve(Date startDate, Date endDate) {
        return retrieve(startDate, endDate, new String[0], new String[0]);
    }

    /**
     * Retrieves all {@link AlertLog} objects associated with the given {@code components} and
     * {@code severities} during the given time range.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @param components the components; if empty, all components are considered
     * @param severities severities; if empty, all severities are considered
     * @return a non-{@code null} list of matching {@link AlertLog} objects
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<AlertLog> retrieve(Date startDate, Date endDate, String[] components,
        String[] severities) {
        if (startDate == null) {
            throw new NullPointerException("startDate can't be null");
        }
        if (endDate == null) {
            throw new NullPointerException("endDate can't be null");
        }
        if (components == null) {
            throw new NullPointerException("components can't be null");
        }
        if (severities == null) {
            throw new NullPointerException("severities can't be null");
        }

        Criteria query = createCriteria(AlertLog.class);
        query.add(Restrictions.ge("alertData.timestamp", startDate));
        query.add(Restrictions.le("alertData.timestamp", endDate));

        if (components.length > 0) {
            Disjunction componentCriteria = Restrictions.disjunction();
            for (String component : components) {
                componentCriteria.add(Restrictions.eq("alertData.sourceComponent", component));
            }
            query.add(componentCriteria);
        }

        if (severities.length > 0) {
            Disjunction severityCriteria = Restrictions.disjunction();
            for (String severity : severities) {
                severityCriteria.add(Restrictions.eq("alertData.severity", severity));
            }
            query.add(severityCriteria);
        }

        query.addOrder(Order.asc("alertData.sourceComponent"));
        query.addOrder(Order.asc("alertData.severity"));
        query.addOrder(Order.asc("alertData.timestamp"));

        return list(query);
    }

    /**
     * Retrieve all alerts for the specified pipeline instance.
     *
     * @param pipelineInstanceId
     * @return
     */
    public List<AlertLog> retrieveForPipelineInstance(long pipelineInstanceId) {
        SQLQuery query = createSQLQuery("select * from PI_ALERT a, PI_PIPELINE_TASK t "
            + "where t.PI_PIPELINE_INSTANCE_ID = :pipelineInstanceId and t.ID = a.SOURCE_TASK_ID "
            + "order by a.SOURCE_TASK_ID");
        query.addEntity(AlertLog.class);
        query.setLong("pipelineInstanceId", pipelineInstanceId);

        return list(query);
    }

    /**
     * Retrieve all alerts for the specified list of pipeline task ids.
     */
    public List<AlertLog> retrieveByPipelineTaskIds(Collection<Long> taskIds) {
        List<AlertLog> rv = new ArrayList<>();
        ListChunkIterator<Long> idIt = new ListChunkIterator<>(taskIds.iterator(), 50);
        for (List<Long> idChunk : idIt) {
            rv.addAll(retrieveChunk(idChunk));
        }
        return rv;
    }

    private List<AlertLog> retrieveChunk(List<Long> taskIds) {
        Criteria query = createCriteria(AlertLog.class);
        query.add(Restrictions.in("alertData.sourceTaskId", taskIds));

        return list(query);
    }
}
