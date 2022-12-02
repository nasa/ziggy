package gov.nasa.ziggy.crud;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.criterion.Disjunction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gov.nasa.ziggy.collections.ListChunkIterator;
import gov.nasa.ziggy.pipeline.definition.ExternalIdAssignable;
import gov.nasa.ziggy.services.database.DatabaseService;

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
public abstract class AbstractCrud implements AbstractCrudInterface {
    private static final Logger log = LoggerFactory.getLogger(AbstractCrud.class);

    /**
     * This is the maximum number of dynamically-created expressions sent to the database. This
     * limit is 1000 in Oracle. A setting of 950 leaves plenty of room for other expressions in the
     * query.
     *
     * @see ListChunkIterator
     */
    public static final int MAX_EXPRESSIONS = 950;

    private DatabaseService databaseService;
    private boolean readOnly;

    /**
     * Creates a {@link AbstractCrud} whose read-only property is set to {@code false}.
     */
    protected AbstractCrud() {
    }

    /**
     * Creates a {@link AbstractCrud} with the given read-only property. CRUD classes can
     * instantiate themselves with this parameter set to {@code true} to avoid dirty checking and
     * therefore save on CPU usage.
     * <p>
     * Use {@link #createQuery(String)} to take advantage of this property.
     */
    protected AbstractCrud(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Creates a {@link AbstractCrud} with the given database service whose read-only property is
     * set to {@code false}.
     */
    protected AbstractCrud(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    /**
     * Creates a {@link AbstractCrud} with the given database service and read-only property. CRUD
     * classes can instantiate themselves this parameter set to {@code true} to avoid dirty checking
     * and therefore save on CPU usage.
     * <p>
     * Use {@link #createQuery(String)} to take advantage of this property.
     */
    protected AbstractCrud(DatabaseService databaseService, boolean readOnly) {
        this(databaseService);
        this.readOnly = readOnly;
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
     * @return the persistence session.
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
    @SuppressWarnings("unchecked")
    public final <T> T get(Class<T> clazz, Serializable id) {
        Object r = getSession().get(clazz, id);
        return (T) r;
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
     * <p>
     * <strong>Note:</strong> This should be &lsquo;final&rsquo;, but cannot be, because unit tests
     * want to mock it.
     */
    @Override
    public void create(Object o) {
        getSession().save(o);
    }

    @Override
    public void update(Object o) {
        getSession().update(o);
    }

    @Override
    public void createOrUpdate(Object o) {
        getSession().saveOrUpdate(o);
    }

    /**
     * Persists changes to all elements of a collection.
     *
     * @param collection the collection of items to persist
     */
    public void create(Collection<?> collection) {
        for (Object item : collection) {
            create(item);
        }
    }

    @Override
    public final void delete(Object o) {
        getSession().delete(o);
    }

    /**
     * Deletes all items in a collection.
     *
     * @param collection the collection of items to delete
     */
    public void delete(Collection<?> collection) {
        for (Object item : collection) {
            delete(item);
        }
    }

    /**
     * Creates a new instance of {@link Query} for the given HQL query string using this object's
     * local properties.
     * <p>
     * Note that the read-only property only extends to the queried objects. Objects that are lazily
     * loaded later must be explicitly marked read-only.
     *
     * @param queryString the HQL query string
     * @return a {@link Query} object
     */
    @Override
    public Query createQuery(String queryString) {
        Query query = getSession().createQuery(queryString);
        query.setReadOnly(readOnly);

        return query;
    }

    /**
     * Creates a new instance of {@link SQLQuery} for the given SQL query string using this object's
     * local properties.
     * <p>
     * Note that the read-only property only extends to the queried objects. Objects that are lazily
     * loaded later must be explicitly marked read-only.
     *
     * @param queryString the SQL query string
     * @return a {@link SQLQuery} object
     */
    @Override
    public SQLQuery createSQLQuery(String queryString) {
        SQLQuery query = getSession().createSQLQuery(queryString);
        query.setReadOnly(readOnly);

        return query;
    }

    /**
     * Creates a new {@link Criteria} instance, for the given entity class, or a superclass of an
     * entity class using this object's local properties.
     * <p>
     * Note that the read-only property does not apply to this method. However, use of this method
     * will allow the CRUD class to take advantage of future properties.
     *
     * @param persistentClass the persistent class
     * @return a {@link Criteria} object
     */
    public Criteria createCriteria(Class<?> persistentClass) {
        Criteria query = getSession().createCriteria(persistentClass);

        return query;
    }

    @Override
    public <E> List<E> list(Query query) {
        @SuppressWarnings("unchecked")
        List<E> list = query.list();
        return list;
    }

    public <E> List<E> list(Criteria criteria) {
        @SuppressWarnings("unchecked")
        List<E> list = criteria.list();
        return list;
    }

    @Override
    public <T> T uniqueResult(Query query) {
        @SuppressWarnings("unchecked")
        T t = (T) query.uniqueResult();
        return t;
    }

    protected <T> T uniqueResult(Criteria criteria) {
        @SuppressWarnings("unchecked")
        T t = (T) criteria.uniqueResult();
        return t;
    }

    /**
     * Produce a query for the next chunk. Used by aggregateResults() to collect all the results
     * needed when a single query would have too many expressions.
     *
     * @param <T> The type used in the expression used to build the query.
     * @param <R> The result type of the query.
     */
    @FunctionalInterface
    public interface QueryFactory<T, R> {
        Query produceQuery(List<T> nextChunk);
    }

    /**
     * Use this to produce a complete list of results when queries must be broken into many distinct
     * queries in order to satisify database query language limitations.
     *
     * @param source The complete list of elements to query.
     * @param queryFactory creates a new query or sets query parameters for the next chunk of
     * expressions to evaluate.
     * @return list of type R, the result type.
     */
    protected <T, R> List<R> aggregateResults(Collection<T> source,
        QueryFactory<T, R> queryFactory) {
        if (source.isEmpty()) {
            return Collections.emptyList();
        }
        List<R> results = Lists.newArrayListWithCapacity(MAX_EXPRESSIONS * 2);
        ListChunkIterator<T> it = new ListChunkIterator<>(source.iterator(), MAX_EXPRESSIONS);
        for (List<T> chunk : it) {
            Query q = queryFactory.produceQuery(chunk);
            List<R> resultChunk = list(q);
            results.addAll(resultChunk);
        }
        return results;
    }

    /**
     * Use this to produce a Critieron query Restrictions.in() when the number of values in the in()
     * is potentially too large for the database to manage unless it is first chunked up. Based on:
     * https://stackoverflow.com/a/59828331 .
     */
    protected <T> Disjunction restrictionPropertyIn(String property, Collection<T> values) {
        Disjunction criterion = Restrictions.disjunction();
        List<T> valuesList = new ArrayList<>(values);
        for (List<T> idSubset : Lists.partition(valuesList, MAX_EXPRESSIONS)) {
            criterion.add(Restrictions.in(property, idSubset));
        }
        return criterion;
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

    /**
     * Gets the maximum ID for existing database entries on a given table.
     *
     * @return Max ID value for existing database entries, as a long, or -1 if no entries are
     * present.
     */
    @SuppressWarnings("unchecked")
    private <T extends Number> T retrieveMaxDatabaseId(ExternalIdAssignable table) {
        Class<? extends ExternalIdAssignable> clazz = table.getClass();
        log.info("Class name: " + clazz.getName());
        Criteria criteria = createCriteria(clazz);
        criteria.setProjection(Projections.max("id"));
        T maxDatabaseId = uniqueResult(criteria);
        T maxId = maxDatabaseId != null ? maxDatabaseId : (T) Integer.valueOf(-1);
        return maxId;
    }

    /**
     * Determines a model ID for assignment.
     *
     * @return A valid model ID that can be assigned. If there are no entries in the relevant
     * database table, or all the entries are smaller than the min permitted assigned value, the min
     * permitted assigned value will be returned. If there are values in the database greater than
     * the min assigned value, the max value in the database + 1 will be returned.
     */
    @SuppressWarnings("unchecked")
    private <T extends Number> T tableIdForAssignment(ExternalIdAssignable table) {

        T maxDatabaseId = retrieveMaxDatabaseId(table);
        T assignedId = maxDatabaseId.intValue() >= table.minAllowedAssignedIdNumber()
            ? (T) Integer.valueOf(maxDatabaseId.intValue() + 1)
            : (T) Integer.valueOf(table.minAllowedAssignedIdNumber());
        return assignedId;
    }

    /**
     * Returns a valid table ID for a table that implements the ExternalIdAssignable interface
     *
     * @param table Table that requires a valid ID
     * @return the existing table ID if it is valid, otherwise an assigned ID based on values in the
     * database and the table-specific min valid assigned ID value.
     */
    protected int validTableId(ExternalIdAssignable table) {

        // default is to use the table's ID
        int tableId = table.externalId();
        int id = tableId != 0 ? tableId : tableIdForAssignment(table).intValue();
        return id;
    }
}
