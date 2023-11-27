package gov.nasa.ziggy.services.database;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.ProtectedEntityInterceptor;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Implementation of the {@link DatabaseService} for Hibernate.
 * <p>
 * This class uses {@link Configuration} for configuration, along with {@link AnnotatedPojoList} to
 * auto-scan the class path for annotated classes
 *
 * @author Todd Klaus
 */
public final class HibernateDatabaseService extends HibernateDatabaseServiceBase {
    public static final Logger log = LoggerFactory.getLogger(HibernateDatabaseService.class);

    private SessionFactory sessionFactory;

    protected final ThreadLocal<Session> threadSession = new ThreadLocal<>();

    /**
     * package-protection to prevent instantiation (use {@link DatabaseServiceFactory} instead).
     */
    HibernateDatabaseService() {
    }

    /**
     * @see gov.nasa.ziggy.services.database.DatabaseService#initialize()
     */
    @Override
    public void initialize() {
        log.debug("Building configuration");
        hibernateConfig = ZiggyHibernateConfiguration.buildHibernateConfiguration();

        log.debug("Initializing SessionFactory");
        sessionFactory = hibernateConfig.buildSessionFactory();

        log.debug("Initialization complete - {}", sessionFactory);
    }

    /**
     * Start a new transaction for the current Session
     */
    @Override
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public void beginTransaction() {
        Session session = null;

        try {
            log.debug("Starting Hibernate transaction");
            session = getSession();
            session.beginTransaction();
        } catch (HibernateException e) {
            handleException(e, session);
        }
    }

    /**
     * Commit the current transaction and close the Session
     */
    @Override
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public void commitTransaction() {
        Session session = null;

        try {
            log.debug("Committing Hibernate transaction");
            session = getSession();
            session.getTransaction().commit();
        } catch (HibernateException e) {
            handleException(e, session);
        }
    }

    /**
     * Roll back the existing transaction, if any, and close the Session.
     */
    @Override
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public void rollbackTransactionIfActive() {
        Session session = null;

        try {
            session = getSession();

            Transaction transaction = getSession().getTransaction();
            if (transaction != null && transaction.isActive()) {
                log.debug("Rolling back active Hibernate transaction");
                transaction.rollback();
                threadSession.remove();
                session.close();
            }
        } catch (HibernateException e) {
            handleException(e, session);
        }
    }

    @Override
    public boolean transactionIsActive() {
        Transaction transaction = getSession().getTransaction();
        return transaction != null && transaction.isActive();
    }

    @Override
    public void doSchemaExport() {
        // TODO How do we access the JPA configuration to initialize SchemaExport?
    }

    /**
     * Try to close the current {@link Session} and remove it from the ThreadLocal.
     * {@link #handleException(HibernateException, Session)} is called by all methods that have to
     * handle {@link HibernateException}. The idea here is to ensure that the session gets closed
     * before the {@link HibernateException} passes execution control back to the caller. Think of
     * this as being like a finally block, but implemented as a method so we don't have to duplicate
     * the same finally block in multiple places.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    protected void handleException(HibernateException e, Session session) {
        threadSession.remove();

        if (session != null) {
            try {
                session.close();
            } catch (HibernateException ignored) {
                log.warn("Failed to close Session after previousfailure", ignored);
                // A failure to close the session must not bring down the pipeline.
            }
        }
        // Now that the session is closed, we can throw the exception and pass
        // execution back to the caller.
        throw e;
    }

    /**
     * Create a new {@link Session} The lifecycle of a Session spans a single transaction. They
     * should not be cached by the client, used by multiple threads, or used for multiple
     * transactions.
     */
    @Override
    public Session getSession() {
        Session session = threadSession.get();

        if (session == null) {
            session = sessionFactory().withOptions()
                .interceptor(new ProtectedEntityInterceptor())
                .openSession();
            log.debug("Created Session {} in Thread {}", session, Thread.currentThread().getName());
            threadSession.set(session);
        }

        return session;
    }

    /**
     * Close the current {@link Session} associated with the calling Thread. It is the
     * responsibility of the caller to close the session (by calling this method) if the session
     * throws a {@link HibernateException} because this invalidates the session.
     */
    @Override
    public void closeCurrentSession() {
        Session session = threadSession.get();

        if (session != null) {
            threadSession.remove();
            session.close();
            log.debug("Closed Session {} in Thread {}", session, Thread.currentThread().getName());
        }
    }

    @Override
    protected SessionFactory sessionFactory() {
        return sessionFactory;
    }
}
