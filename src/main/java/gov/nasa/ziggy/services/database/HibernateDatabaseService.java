package gov.nasa.ziggy.services.database;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.ProtectedEntityInterceptor;
import gov.nasa.ziggy.module.PipelineException;

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
    private ServiceRegistry serviceRegistery;

    protected final ThreadLocal<Session> threadSession = new ThreadLocal<>();

    /**
     * package-protection to prevent instantiation (use {@link DatabaseServiceFactory} instead)
     */
    HibernateDatabaseService() {
    }

    /**
     * @see gov.nasa.ziggy.services.database.DatabaseService#initialize()
     */
    @Override
    public void initialize() {
        log.debug("Hibernate Init: Building configuration");
        hibernateConfig = ZiggyHibernateConfiguration
            .buildHibernateConfiguration(alternatePropertiesSource);

        try {
            log.debug("Hibernate Init: Initializing SessionFactory");
            serviceRegistery = new ServiceRegistryBuilder()
                .applySettings(hibernateConfig.getProperties())
                .buildServiceRegistry();
            sessionFactory = hibernateConfig.buildSessionFactory(serviceRegistery);

            log.debug("Hibernate Init: initialization complete - " + sessionFactory);
        } catch (Exception e) {
            log.error("Hibernate Init: " + serviceRegistery + ": failed to create sessionFactory.",
                e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Start a new transaction for the current Session
     */
    @Override
    public void beginTransaction() {
        Session session = null;

        try {
            session = getSession();
            session.beginTransaction();
        } catch (HibernateException e) {
            handleException(e, session);
        }

        log.debug("Hibernate transaction started.");
    }

    /**
     * Commit the current transaction and close the Session
     */
    @Override
    public void commitTransaction() {
        Session session = null;

        try {
            session = getSession();
            session.getTransaction().commit();
        } catch (HibernateException e) {
            handleException(e, session);
        }
    }

    /**
     * Roll back the existing transaction, if any, and close the Session
     */
    @Override
    public void rollbackTransactionIfActive() {
        Session session = null;

        try {
            session = getSession();

            Transaction transaction = getSession().getTransaction();
            if (transaction != null && transaction.isActive()) {
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
     * Try to close the current {@link Session} and remove it from the ThreadLocal
     *
     * @param e
     * @param session
     * @throws PipelineException
     */
    protected void handleException(HibernateException e, Session session) {
        threadSession.remove();

        if (session != null) {
            try {
                session.close();
            } catch (Exception e2) {
                log.warn("Failed to close Session after previousfailure", e2);
            }
        }
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
            log.debug("Creating new Session for Thread: " + Thread.currentThread().getName());
            session = sessionFactory().withOptions()
                .interceptor(new ProtectedEntityInterceptor())
                .openSession();
            log.debug("Created new Session: " + session);
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
        }
    }

    @Override
    protected SessionFactory sessionFactory() {
        return sessionFactory;
    }

}
