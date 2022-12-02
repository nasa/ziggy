package gov.nasa.ziggy.crud;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.SQLQuery;

/**
 * @author Miles Cote
 */
public interface AbstractCrudInterface {
    /**
     * Makes an object persistent. The object must not have previously been persisted with the same
     * key.
     *
     * @param o the object to persist
     */
    void create(Object o);

    /**
     * Updates a persistent object with new values.
     *
     * @param o the object to update
     */
    void update(Object o);

    /**
     * Makes an object persistent or updates an exsiting persistent object, depending on whether the
     * object has already been persisted.
     *
     * @param o the object to create or update
     */
    void createOrUpdate(Object o);

    /**
     * Deletes an object.
     *
     * @param o the object to be deleted.
     */
    void delete(Object o);

    Query createQuery(String queryString);

    SQLQuery createSQLQuery(String queryString);

    <E> List<E> list(Query query);

    <T> T uniqueResult(Query query);
}
