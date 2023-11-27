package gov.nasa.ziggy.services.database;

import java.sql.Connection;
import java.util.Collection;

import org.hibernate.HibernateException;
import org.hibernate.Session;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Interface used by CRUD classes to initialize and access the persistence layer.
 *
 * @author Sean McCauliff
 * @author Todd Klaus
 * @author PT
 */
public abstract class DatabaseService {

    static DatabaseService instance = null;

    /**
     * Find a database service implementation. Note that there is a singleton instance of the
     * database service that is used by all threads; however, the HibernateDatabaseService has a
     * ThreadLocal&lt;Session&gt;. Thus all objects in a given instance use the same session for
     * their database access.
     */
    public static synchronized DatabaseService getInstance() {

        if (!DatabaseService.usingLocalService.get()) {
            throw new PipelineException(
                "A transaction was started but the " + "DatabaseService was not included.");
        }

        if (instance != null) {
            return instance;
        }

        instance = new HibernateDatabaseService();
        instance.initialize();
        return instance;
    }

    /**
     * When true getInstance() can be called because it is involved with a transaction associated
     * with the current thread (or could be) else an exception should be thrown.
     */
    static final ThreadLocal<Boolean> usingLocalService = new DatabaseService.BooleanThreadLocal(
        Boolean.TRUE);

    /**
     * Initialize the {@link DatabaseService} implementation. This method should only be called by
     * the {@link DatabaseService#getInstance()} methods.
     */
    public abstract void initialize();

    /**
     * Start a new {@link Session}/local transaction for the current thread
     */
    public abstract void beginTransaction();

    /**
     * Commit the existing local transaction for the current {@link Session}
     */
    public abstract void commitTransaction();

    /**
     * Rollback the existing local transaction for the current {@link Session}, if active
     */
    public abstract void rollbackTransactionIfActive();

    /**
     * Indicates whether a transaction is currently active.
     *
     * @return
     */
    public abstract boolean transactionIsActive();

    /**
     * Turns auto-flushing of SQL to the underlying database on or off. This is necessary if you
     * plan to modify persisted objects outside of the context of a transaction (UIs). If auto-flush
     * is off, the caller is responsible for calling flush() explicitly within the context of a
     * transaction.
     */
    public abstract void setAutoFlush(boolean active);

    /**
     * Manually flush SQL to the underlying database. Must be called within the context of a
     * transaction. Only necessary if auto-flush is turned OFF.
     */
    public abstract void flush();

    /**
     * Evict all instances of the given class from the Session. This ensures that subsequent queries
     * for these objects will go to the database instead of possibly retrieving them from the cache.
     * Use this method before executing a query if it is possible that another process or thread has
     * updated the objects in the database. A transaction context is not required.
     *
     * @param clazz the class for which to evict
     */
    public abstract void evictAll(Class<?> clazz);

    /**
     * Evict the specified objects from the Session. This ensures that subsequent queries for these
     * objects will go to the database instead of possibly retrieving them from the cache. Use this
     * method before executing a query if it is possible that another process or thread has updated
     * the objects in the database. A transaction context is not required.
     *
     * @param collection
     */
    public abstract void evictAll(Collection<?> collection);

    /**
     * Evict the specified object from the Session. This ensures that subsequent queries for this
     * object will go to the database instead of possibly retrieving it from the cache. Use this
     * method before executing a query if it is possible that another process or thread has updated
     * the object in the database. A transaction context is not required.
     *
     * @param object
     */
    public abstract void evict(Object object);

    /**
     * Completely clear the session. Evict all loaded instances and cancel all pending saves,
     * updates and deletions. Do not close open iterators or instances of ScrollableResults.
     * <p>
     * This is useful for removing any stale cached results from a previous session.
     */
    public abstract void clear();

    /**
     * Export the metadata model to the current database (not yet implemented for Hibernate, use
     * DdlInitializer instead)
     */
    public abstract void doSchemaExport();

    /**
     * Get the underlying JDBC Connection for the current Hibernate {@link Session}
     *
     * @return
     */
    public abstract Connection getConnection();

    /**
     * Create a new {@link Session} The lifecycle of a Session spans a single transaction. They
     * should not be cached by the client, used by multiple threads, or used for multiple
     * transactions.
     */
    public abstract Session getSession();

    /**
     * Close the current {@link Session} associated with the calling Thread. It is the
     * responsibility of the caller to close the session (by calling this method) if the session
     * throws a {@link HibernateException} because this invalidates the session.
     */
    public abstract void closeCurrentSession();

    /**
     * sets the singleton instance to a particular DatabaseService object supplied by the caller.
     * Useful for test purposes, in which case a mocked DatabaseService may be needed.
     *
     * @param dbServiceInstance object to be supplied to all future calls to getInstance().
     */
    public static synchronized void setInstance(DatabaseService dbServiceInstance) {
        instance = dbServiceInstance;
    }

    /**
     * Clears the cached handles so that the next call to {@link getInstance} will initialize the
     * database system. This is usually just useful for feature tests which actually restart the
     * database server.
     */
    public static synchronized void reset() {
        instance = null;
    }

    public static synchronized void clearNotUsingService(boolean xa) {
        usingLocalService.set(Boolean.TRUE);
    }

    /**
     * Marks the current thread as being involved in a transaction, but not utilizing this service.
     */
    public static synchronized void markNotUsingService(boolean xa) {

        usingLocalService.set(Boolean.FALSE);
    }

    /**
     * A thread local that stores a boolean and has an initial value.
     */
    static class BooleanThreadLocal extends ThreadLocal<Boolean> {
        private final Boolean initValue;

        public BooleanThreadLocal(Boolean initValue) {
            this.initValue = initValue;
        }

        @Override
        protected Boolean initialValue() {
            return initValue;
        }
    }
}
