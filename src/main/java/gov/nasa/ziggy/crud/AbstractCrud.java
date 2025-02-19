package gov.nasa.ziggy.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gov.nasa.ziggy.services.database.DatabaseController;
import gov.nasa.ziggy.services.database.DatabaseService;
import jakarta.persistence.LockModeType;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaUpdate;

/**
 * The parent class for all CRUD classes.
 * <p>
 * NB: for CRUD operations on classes that include an originator, use AbstractProducerConsumerCrud
 * as the parent class, since that class automates the management of the producer-consumer
 * relationship.
 *
 * @author Bill Wohler
 * @author PT
 */
public abstract class AbstractCrud<U> implements AbstractCrudInterface<U> {

    private static final Logger log = LoggerFactory.getLogger(AbstractCrud.class);

    private DatabaseService databaseService;

    /**
     * Creates a {@link AbstractCrud} whose read-only property is set to {@code false}.
     */
    protected AbstractCrud() {
    }

    /**
     * Creates a {@link AbstractCrud} with the given database service whose read-only property is
     * set to {@code false}.
     */
    protected AbstractCrud(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    /**
     * Returns the database service used by this CRUD object.
     */
    protected final DatabaseService getDatabaseService() {
        if (databaseService == null) {
            databaseService = DatabaseService.getInstance();
        }

        return databaseService;
    }

    /**
     * Convenience method that returns the current persistence session. Do not cache this locally as
     * it can vary between threads.
     *
     * @return the persistence session
     */
    protected final Session getSession() {
        return getDatabaseService().getSession();
    }

    /**
     * Retrieves an object from the database with given class and ID.
     *
     * @param clazz the object class
     * @param id the object ID
     * @return the retrieved object
     */
    public final <T> T get(Class<T> clazz, Object id) {
        return getSession().get(clazz, id);
    }

    /**
     * Removes an object from the database layer session cache.
     *
     * @param o the object to remove from the session cache
     */
    public void evict(Object o) {
        getSession().evict(o);
    }

    /**
     * Makes an object persistent.
     */
    @Override
    public void persist(Object o) {
        getSession().persist(o);
    }

    public <T> T merge(T o) {
        return getSession().merge(o);
    }

    /**
     * Persists changes to all elements of a collection.
     *
     * @param collection the collection of items to persist
     */
    @Override
    public void persist(Collection<?> collection) {
        for (Object item : collection) {
            persist(item);
        }
    }

    public void remove(Object o) {
        getSession().remove(o);
    }

    /**
     * Deletes all items in a collection.
     *
     * @param collection the collection of items to delete
     */
    public void remove(Collection<?> collection) {
        for (Object item : collection) {
            remove(item);
        }
    }

    /** Returns a {@link CriteriaBuilder} object for use in building queries. */
    public HibernateCriteriaBuilder createCriteriaBuilder() {
        return getSession().getCriteriaBuilder();
    }

    public <R> List<R> list(Query<R> query) {
        return query.list();
    }

    protected <R> int executeUpdate(CriteriaDelete<R> criteria) {
        return getSession().createMutationQuery(criteria).executeUpdate();
    }

    protected <R> int executeUpdate(CriteriaUpdate<R> criteria) {
        return getSession().createMutationQuery(criteria).executeUpdate();
    }

    protected void lock(Object entity, LockModeType lockMode) {
        getSession().lock(entity, lockMode);
    }

    /**
     * Creates an instance of {@link ZiggyQuery} in which the user wants to return entire rows of
     * the table (or, on the Java side, the user wants to return whole instances of the class that's
     * the target of the query).
     */
    @Override
    public <R> ZiggyQuery<R, R> createZiggyQuery(Class<R> returnClass) {
        return createZiggyQuery(returnClass, returnClass);
    }

    /**
     * Creates an instance of {@link ZiggyQuery} in which the user returns column values from the
     * query result rather than entire rows (or, on the Java side, the user returns fields rather
     * than entire instances of the class that's the target of the query).
     */
    @Override
    public <T, R> ZiggyQuery<T, R> createZiggyQuery(Class<T> databaseClass, Class<R> returnClass) {
        return new ZiggyQuery<>(databaseClass, returnClass, this);
    }

    @Override
    public <T, R> R uniqueResult(ZiggyQuery<T, R> query) {
        return getSession()
            .createQuery(query.constructSelectClause().constructWhereClause().getCriteriaQuery())
            .uniqueResult();
    }

    @Override
    public <T, R> List<R> list(ZiggyQuery<T, R> query) {
        return getSession()
            .createQuery(query.constructSelectClause().constructWhereClause().getCriteriaQuery())
            .getResultList();
    }

    /**
     * Performs a query in which the query must be broken into multiple discrete queries due to
     * database query language limitations, the results of which are then combined and returned.
     * <p>
     * For example:
     *
     * <pre>
     * chunkedQuery(pipelineTaskIds,
     *     chunk -> list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.id)
     *         .ascendingOrder()
     *         .in(chunk)));
     * </pre>
     *
     * The variable constraintsCollection is the collection of objects of class T that constrain the
     * query and queryWithRestraints is a method that returns a query that applies the constraints.
     *
     * @param <T> class of the objects in the list that constrain the query
     * @param <R> class of the objects in the list returned by the query
     * @param source the list of elements of type T to use in the query
     * @param query returns a list of results of type R based upon the collection of type T
     * @return list of type R
     */
    protected <T, R> List<R> chunkedQuery(List<T> source, Function<List<T>, List<R>> query) {
        if (source.isEmpty()) {
            return Collections.emptyList();
        }
        int maxExpressions = maxExpressions();
        List<R> results = new ArrayList<>(maxExpressions * 2);
        for (List<T> chunk : Lists.partition(source, maxExpressions)) {
            log.info("Created chunk of size {}", chunk.size());
            results.addAll(query.apply(chunk));
        }
        return results;
    }

    /**
     * Maximum expressions allowed in a query.
     */
    int maxExpressions() {
        return DatabaseController.newInstance().maxExpressions();
    }

    /**
     * Flush any changes to persistent objects to the underlying database.
     */
    protected void flush() {
        getSession().flush();
    }

    /**
     * Clear the Hibernate cache.
     */
    protected void clear() {
        getSession().clear();
    }

    /**
     * Clear the hibernate cache after first flushing all changes.
     */
    public void clearHibernateCache() {
        clearHibernateCache(false);
    }

    /**
     * Clear the hibernate cache.
     *
     * @param skipFlush if true, the cached changes are not first flushed to the database; if false,
     * the changes are flushed before the clear. Both of these options present risks: with a flush,
     * local changes to objects that were never intended to be persisted will get persisted; without
     * it, local changes to objects that were intended to be persisted will be lost. Use with
     * caution!
     */
    public void clearHibernateCache(boolean skipFlush) {
        if (!skipFlush) {
            flush();
        }
        clear();
    }
}
