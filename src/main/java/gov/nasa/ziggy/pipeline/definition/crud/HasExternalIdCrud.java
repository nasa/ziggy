package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;

import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrudInterface;
import gov.nasa.ziggy.crud.HibernateClassCrud;
import gov.nasa.ziggy.pipeline.definition.HasExternalId;

/**
 * Contains {@link HasExternalId} data access operations.
 *
 * @author Miles Cote
 */
public interface HasExternalIdCrud<T extends HasExternalId>
    extends AbstractCrudInterface, HibernateClassCrud<T> {
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
        log.info(getClass().getSimpleName() + " retrieving for externalId " + externalId);

        Query q = createQuery("from " + getHibernateClassName() + " where "
            + getExternalIdFieldName() + " = :externalIdParam ");
        q.setParameter("externalIdParam", externalId);

        return uniqueResult(q);
    }

    /**
     * Retrieves the latest version of a receivable.
     */
    default T retrieveByMaxExternalId() {
        Logger log = LoggerFactory.getLogger(HasExternalIdCrud.class);
        log.info(getClass().getSimpleName() + " retrieving for max externalId");

        Query q = createQuery("from " + getHibernateClassName() + " where "
            + getExternalIdFieldName() + " in (select max(" + getExternalIdFieldName() + ") from "
            + getHibernateClassName() + ")");

        return uniqueResult(q);
    }

    /**
     * Retrieves the maximum ID value currently in the database.
     *
     * @return max ID of the object type, or -1 if no objects of the type are currently in the
     * database.
     */
    default int retriveMaxExternalId() {
        Query q = createQuery("select " + getExternalIdFieldName() + " from "
            + getHibernateClassName() + " where " + getExternalIdFieldName() + " in (select max("
            + getExternalIdFieldName() + ") from " + getHibernateClassName() + ")");

        Integer maxId = uniqueResult(q);
        if (maxId == null) {
            return -1;
        }
        return maxId;
    }

    default boolean idInDatabase(int idNumber) {
        Query q = createQuery(
            "select " + getExternalIdFieldName() + " from " + getHibernateClassName());
        List<Integer> ids = list(q);
        if (ids == null || ids.isEmpty()) {
            return false;
        }
        return ids.contains(Integer.valueOf(idNumber));
    }

}
