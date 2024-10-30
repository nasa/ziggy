package gov.nasa.ziggy.services.database;

import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_DIALECT;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_DRIVER;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_URL;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_USERNAME;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * This class contructs a Hibernate @{link AnnotatedConfiguration} object.
 * <p>
 * It uses {@link AnnotatedPojoList} to scan the class path for annotated classes and adds them to
 * the configuration.
 * <p>
 * It also copies all properties that are prefixed with "hibernate." from the Ziggy configuration to
 * the Hibernate @{link AnnotatedConfiguration} object.
 * <p>
 * This class is used by the {@link HibernateDatabaseService} to initialize the
 * {@link DatabaseService}. It is also used by the various ant tasks that create the schema
 * (schema-oracle, schema-hsqldb, etc.)
 *
 * @author Todd Klaus
 */
public class ZiggyHibernateConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ZiggyHibernateConfiguration.class);

    private static final String ANNOTATED_POJO_PACKAGE_FILTER = "^gov\\.nasa\\..*";

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
        ImmutableConfiguration ziggyConfig = ZiggyConfiguration.getInstance();

        log.debug("Initializing Hibernate");

        Configuration hibernateConfig = new Configuration();

        // Get the props from the from the Ziggy configuration.
        for (Iterator<?> iter = ziggyConfig.getKeys("hibernate"); iter.hasNext();) {
            String key = (String) iter.next();
            String value = ziggyConfig.getString(key);

            log.debug("copying property, key={}, value={}", key, value);
            hibernateConfig.setProperty(key, value);
        }

        // Inject properties defined by our code.
        hibernateConfig.setProperty(HIBERNATE_DIALECT.property(), hibernateDialect());
        hibernateConfig.setProperty(HIBERNATE_DRIVER.property(), driverClassName());

        log.info("Database URL {}", hibernateConfig.getProperty(HIBERNATE_URL.property()));
        log.debug("Database user {}", hibernateConfig.getProperty(HIBERNATE_USERNAME.property()));

        Set<Class<?>> detectedClasses = annotatedClasses();

        log.debug("Adding {} annotated POJOs to Hibernate", detectedClasses.size());

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
     * Returns a non-null set of classes in the classpath with Hibernate annotations. The classes
     * are limited to packages starting with {@value #ANNOTATED_POJO_PACKAGE_FILTER}.
     */
    public static Set<Class<?>> annotatedClasses() {
        AnnotatedPojoList annotatedPojoList = new AnnotatedPojoList();
        annotatedPojoList.getPackageFilters().add(ANNOTATED_POJO_PACKAGE_FILTER);

        return annotatedPojoList.scanForClasses();
    }

    /**
     * Returns the name of the Java class to be used as the database driver. If the property
     * {@link PropertyName#HIBERNATE_DRIVER} is specified in the properties file, its value is used.
     * If not, the database controller's value for the driver class is used.
     *
     * @throws PipelineException if the class can not be found in either the property or database
     * controller
     */
    public static String driverClassName() {
        String driverClassName = ZiggyConfiguration.getInstance()
            .getString(HIBERNATE_DRIVER.property(), null);
        if (driverClassName == null) {
            DatabaseController controller = DatabaseController.newInstance();
            if (controller == null) {
                throw new PipelineException("Cannot determine database driver class from "
                    + HIBERNATE_DRIVER + " property or DatabaseController");
            }
            driverClassName = controller.driver();
        }
        return driverClassName;
    }

    /**
     * Returns the name of the SQL dialect to be used for database activities. If the property
     * {@link PropertyName#HIBERNATE_DIALECT} is specified in the properties file, its value is
     * used. If not, the database controller's value for the dialect is used.
     *
     * @throws PipelineException if the dialect can not be found in either the property or database
     * controller
     */
    public static String hibernateDialect() {
        String hibernateDialect = ZiggyConfiguration.getInstance()
            .getString(HIBERNATE_DIALECT.property(), null);
        if (hibernateDialect == null) {
            DatabaseController controller = DatabaseController.newInstance();
            if (controller == null) {
                throw new PipelineException("Cannot determine database dialect from "
                    + HIBERNATE_DIALECT + " property or DatabaseController");
            }
            hibernateDialect = controller.sqlDialect().dialect();
        }
        return hibernateDialect;
    }
}
