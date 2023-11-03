package gov.nasa.ziggy.services.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionImplementor;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.persistence.FlushModeType;

public abstract class HibernateDatabaseServiceBase extends DatabaseService {

    protected Configuration hibernateConfig = null;

    public HibernateDatabaseServiceBase() {
    }

    @Override
    public void flush() {
        getSession().flush();
    }

    @Override
    public void setAutoFlush(boolean active) {
        if (active) {
            getSession().setFlushMode(FlushModeType.AUTO);
        } else {
            getSession().setFlushMode(FlushModeType.COMMIT);
        }
    }

    protected String getPropertyChecked(String name) {
        String value = hibernateConfig.getProperty(name);

        if (value == null) {
            throw new PipelineException("Required property " + name + " not set!");
        }
        return value;
    }

    @Override
    public void evictAll(Class<?> clazz) {
        sessionFactory().getCache().evictEntityData(clazz);
    }

    @Override
    public void evictAll(Collection<?> collection) {
        Session session = getSession();
        for (Object object : collection) {
            session.evict(object);
        }
    }

    @Override
    public void evict(Object object) {
        getSession().evict(object);
    }

    @Override
    public void clear() {
        getSession().clear();
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Connection getConnection() {
        Session session = getSession();
        try {
            if (session instanceof SessionImplementor) {
                SessionImplementor sessionImplementor = (SessionImplementor) session;
                return sessionImplementor.getJdbcConnectionAccess().obtainConnection();
            }
        } catch (SQLException e) {
            throw new PipelineException("cannot get connection from session", e);
        }
        throw new PipelineException("cannot get connection from session");
    }

    /**
     * The underlying Hibernate implementation of jakarta.persistence.EntityManagerFactory. We use
     * this to get direct access to the Hibernate APIs rather than using the JPA (which is a subset
     * of the Hibernate functionality)
     */
    protected abstract SessionFactory sessionFactory();
}
