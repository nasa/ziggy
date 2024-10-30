package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrudInterface;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.HasExternalId;

/**
 * Contains {@link HasExternalId} data access operations.
 *
 * @author Miles Cote
 */
public interface HasExternalIdCrud<T extends HasExternalId> extends AbstractCrudInterface<T> {
    /**
     * All receivables are written by DR, and the origin for DR writes is 0.
     */
    long RECEIVABLE_ORIGIN = 0;

    /**
     * @return the name of the field containing the externalId.
     */
    String getExternalIdFieldName();

    /**
     * Retrieves the receivable for the input externalId. The external id should be in many cases
     * the primary key in the database for this object.
     */
    default T retrieveByExternalId(int externalId) {
        Logger log = LoggerFactory.getLogger(HasExternalIdCrud.class);
        log.info("{} retrieving for externalId {}", getClass().getSimpleName(), externalId);

        ZiggyQuery<T, T> query = createZiggyQuery(componentClass());
        query.column(getExternalIdFieldName()).in(externalId);

        return finalize(uniqueResult(query));
    }

    /**
     * Retrieves the latest version of a receivable.
     */
    default T retrieveByMaxExternalId() {
        Logger log = LoggerFactory.getLogger(HasExternalIdCrud.class);
        log.info("{} retrieving for max externalId", getClass().getSimpleName());
        ZiggyQuery<T, T> query = createZiggyQuery(componentClass());
        query.column(getExternalIdFieldName()).in(retrieveMaxExternalId());
        return finalize(uniqueResult(query));
    }

    /**
     * Retrieves the maximum ID value currently in the database.
     *
     * @return max ID of the object type, or -1 if no objects of the type are currently in the
     * database.
     */
    default int retrieveMaxExternalId() {
        ZiggyQuery<T, Integer> maxIdQuery = createZiggyQuery(componentClass(), Integer.class);
        maxIdQuery.column(getExternalIdFieldName()).max();
        Integer maxId = uniqueResult(maxIdQuery);

        if (maxId == null) {
            return -1;
        }
        return maxId;
    }

    default boolean idInDatabase(int idNumber) {
        ZiggyQuery<T, Integer> query = createZiggyQuery(componentClass(), Integer.class);
        query.column(getExternalIdFieldName()).select();
        List<Integer> ids = list(query);
        if (CollectionUtils.isEmpty(ids)) {
            return false;
        }
        return ids.contains(Integer.valueOf(idNumber));
    }
}
