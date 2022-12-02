package gov.nasa.ziggy.services.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionImplementor;

import gov.nasa.ziggy.module.PipelineException;

public abstract class HibernateDatabaseServiceBase extends DatabaseService {

    /**
     * If set, these properties are used to initialize Hibernate. If null, the properties are
     * fetched from the config service
     */
    protected Properties alternatePropertiesSource = null;

    protected Configuration hibernateConfig = null;

    public HibernateDatabaseServiceBase() {
        super();
    }

    /**
     * @see gov.nasa.ziggy.services.database.DatabaseService#flush()
     */
    @Override
    public void flush() {
        getSession().flush();
    }

    /**
     * @see gov.nasa.ziggy.services.database.DatabaseService#setAutoFlush(boolean)
     */
    @Override
    public void setAutoFlush(boolean active) {
        if (active) {
            getSession().setFlushMode(FlushMode.AUTO);
        } else {
            getSession().setFlushMode(FlushMode.MANUAL);
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
        sessionFactory().getCache().evictEntityRegion(clazz);
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
        Session session = getSession();
        session.evict(object);
    }

    @Override
    public void clear() {
        Session session = getSession();
        session.clear();
    }

    /**
     * @see gov.nasa.ziggy.services.database.DatabaseService#getConnection()
     */
    @Override
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
     * The underlying Hibernate implementation of javax.persistence.EntityManagerFactory. We use
     * this to get direct access to the Hibernate APIs rather than using the JPA (which is a subset
     * of the Hibernate functionality)
     */
    protected abstract SessionFactory sessionFactory();

    @Override
    public void setPropertiesSource(Properties properties) {
        alternatePropertiesSource = properties;
    }
}
