package gov.nasa.ziggy.services.database;

import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_DIALECT_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_DRIVER_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_URL_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_USERNAME_PROP_NAME;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * This class contructs a Hibernate @{link AnnotatedConfiguration} object.
 * <p>
 * It uses {@link AnnotatedPojoList} to scan the class path for annotated classes and adds them to
 * the configuration.
 * <p>
 * It also copies all properties that are prefixed with "hibernate." from the configuration service
 * to the Hibernate @{link AnnotatedConfiguration} object.
 * <p>
 * This class is used by the {@link HibernateDatabaseService} to initialize the
 * {@link DatabaseService}. It is also used by the various ant tasks that create the schema
 * (schema-oracle, schema-hsqldb, etc.)
 *
 * @author Todd Klaus
 */
public class ZiggyHibernateConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ZiggyHibernateConfiguration.class);

    /**
     * Recommended setting for applications that use the @GeneratedValue annotation. The default
     * setting in Hibernate 5 is true, so we can consider deleting this setting when we upgrade.
     */
    public static final boolean HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_DEFAULT = true;

    private static final String ANNOTATED_POJO_PACKAGE_FILTER = "gov.nasa";

    /**
     * Private to prevent instantiation. Static method only
     */
    private ZiggyHibernateConfiguration() {
    }

    /**
     * Build a {@link Configuration} instance using Ziggy-specific resources.
     *
     * @return
     * @throws PipelineException
     */
    public static Configuration buildHibernateConfiguration() {
        return buildHibernateConfiguration(null);
    }

    /**
     * Build a {@link Configuration} instance using Ziggy-specific resources.
     *
     * @return
     * @throws PipelineException
     */
    public static Configuration buildHibernateConfiguration(Properties hibernateProperties) {
        org.apache.commons.configuration.Configuration ziggyConfig = ZiggyConfiguration
            .getInstance();

        log.debug("Initializing Hibernate");

        Configuration hibernateConfig = new Configuration();

        hibernateConfig.setNamingStrategy(ZiggyNamingStrategy.INSTANCE);

        // Copy hibernate-related properties from the props source to the Hibernate configuration.
        if (hibernateProperties != null) {
            // Use the Properties passed in.
            for (Object okey : hibernateProperties.keySet()) {
                String key = (String) okey;
                String value = hibernateProperties.getProperty(key);

                if (value != null) {
                    log.debug("copying property, key=" + key + ", value=" + value);
                    hibernateConfig.setProperty(key, value);
                } else {
                    throw new PipelineException("Property values must not be null, key=" + key);
                }
            }
        } else {
            // Get the props from the from the ConfigurationService.
            for (Iterator<?> iter = ziggyConfig.getKeys("hibernate"); iter.hasNext();) {
                String key = (String) iter.next();
                String value = ziggyConfig.getString(key);

                log.debug("copying property, key=" + key + ", value=" + value);
                hibernateConfig.setProperty(key, value);
            }
        }

        // Inject properties defined by our code.
        hibernateConfig.setProperty(HIBERNATE_DIALECT_PROP_NAME, hibernateDialect());
        hibernateConfig.setProperty(HIBERNATE_DRIVER_PROP_NAME, driverClassName());
        if (hibernateConfig.getProperty(HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_PROP_NAME) == null) {
            hibernateConfig.setProperty(HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_PROP_NAME,
                Boolean.toString(HIBERNATE_ID_NEW_GENERATOR_MAPPINGS_DEFAULT));
        }

        log.info("Database URL: " + hibernateConfig.getProperty(HIBERNATE_URL_PROP_NAME));
        log.debug("Database User: " + hibernateConfig.getProperty(HIBERNATE_USERNAME_PROP_NAME));

        AnnotatedPojoList annotatedPojoList = new AnnotatedPojoList();
        annotatedPojoList.getPackageFilters().add(ANNOTATED_POJO_PACKAGE_FILTER);

        Set<Class<?>> detectedClasses;

        try {
            log.debug("Scanning for annotated POJOs");
            detectedClasses = annotatedPojoList.scanForClasses();
        } catch (Exception e) {
            throw new PipelineException(
                "failed to auto-scan for annotated classes, caught e = " + e, e);
        }

        log.debug("Adding " + detectedClasses.size() + " annotated POJOs to Hibernate");

        for (Class<?> clazz : detectedClasses) {
            hibernateConfig.addAnnotatedClass(clazz);
        }

        // Include Hibernate configuration that can't be handled by annotations.
        // Also uncomment associated line in copy-metadata build.xml target.
        // hibernateConfig.addResource("hbm.cfg.xml");
        // hibernateConfig.addResource("orm.xml");

        return hibernateConfig;
    }

    /**
     * Returns the name of the Java class to be used as the database driver. If the property
     * hibernate.connection.driver_class is specified in the properties file, its value is used. If
     * not, the database controller's value for the driver class is used.
     *
     * @throws PipelineException if the class can not be found in either the property or database
     * controller
     */
    public static String driverClassName() {
        String driverClassName = ZiggyConfiguration.getInstance()
            .getString(HIBERNATE_DRIVER_PROP_NAME, null);
        if (driverClassName == null) {
            DatabaseController controller = DatabaseController.newInstance();
            if (controller == null) {
                throw new PipelineException(
                    "Cannot determine database driver class from hibernate.connection.driver_class property or DatabaseController");
            }
            driverClassName = controller.driver();
        }
        return driverClassName;
    }

    /**
     * Returns the name of the SQL dialect to be used for database activities. If the property
     * hibernate.dialect is specified in the properties file, its value is used. If not, the
     * database controller's value for the dialect is used.
     *
     * @throws PipelineException if the dialect can not be found in either the property or database
     * controller
     */
    public static String hibernateDialect() {
        String hibernateDialect = ZiggyConfiguration.getInstance()
            .getString(HIBERNATE_DIALECT_PROP_NAME, null);
        if (hibernateDialect == null) {
            DatabaseController controller = DatabaseController.newInstance();
            if (controller == null) {
                throw new PipelineException(
                    "Cannot determine database dialect from hibernate.dialect property or DatabaseController");
            }
            hibernateDialect = controller.sqlDialect().dialect();
        }
        return hibernateDialect;
    }
}
