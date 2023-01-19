package gov.nasa.ziggy;

import static gov.nasa.ziggy.ZiggyPropertyRule.resetSystemProperty;
import static gov.nasa.ziggy.services.config.PropertyNames.DATABASE_SOFTWARE_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_DIALECT_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_DRIVER_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_PASSWD_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_URL_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_USERNAME_PROP_NAME;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

import gov.nasa.ziggy.services.database.DatabaseController;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.database.HsqldbController;

/**
 * Implements a {@link TestRule} for the set up and tear down of databases for use by unit tests. To
 * use, declare a field that refers to this rule as shown.
 *
 * <pre>
 * &#64;Rule
 * public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();
 * </pre>
 *
 * The test doesn't need to use this field as database actions are typically performed within a
 * DatabaseTransactionFactory.performTransaction() lambda, and the current implementation does not
 * provide any additional informational methods.
 * <p>
 * The {@code before()} method of this rule runs before the {@code @Before} method of the test so
 * that the database can be safely populated there.
 * <p>
 * This class is marked {@code @NotThreadSafe} as it manipulates system properties that are global
 * across the JVM.
 *
 * @author Bill Wohler
 */
@NotThreadSafe
public class ZiggyDatabaseRule extends ExternalResource {

    private String databaseSoftwareName;
    private String hibernateConnectionPassword;
    private String hibernateConnectionUrl;
    private String hibernateConnectionUsername;
    private String hibernateDialect;
    private String hibernateJdbcBatchSize;
    private String hibernateShowSql;
    private String hibernateDriverClass;

    @Override
    protected void before() throws Throwable {
        databaseSoftwareName = System.setProperty(DATABASE_SOFTWARE_PROP_NAME, "hsqldb");
        DatabaseController databaseController = new HsqldbController();

        hibernateConnectionPassword = System.setProperty(HIBERNATE_PASSWD_PROP_NAME, "");
        hibernateConnectionUrl = System.setProperty(HIBERNATE_URL_PROP_NAME,
            "jdbc:hsqldb:mem:hsqldb-ziggy");
        hibernateConnectionUsername = System.setProperty(HIBERNATE_USERNAME_PROP_NAME, "sa");

        // For some reason, the unit tests won't run successfully without the Hibernate
        // dialect being set as a system property, even though the actual pipelines
        // function just fine without any dialect in the properties. Something to figure
        // out and fix when possible.
        hibernateDialect = System.setProperty(HIBERNATE_DIALECT_PROP_NAME,
            databaseController.sqlDialect().dialect());
        hibernateDriverClass = System.setProperty(HIBERNATE_DRIVER_PROP_NAME,
            databaseController.driver());

        hibernateJdbcBatchSize = System.setProperty("hibernate.jdbc.batch_size", "0");
        hibernateShowSql = System.setProperty("hibernate.show_sql", "false");

        DatabaseTransactionFactory.performTransaction(() -> {
            databaseController.createDatabase();
            return null;
        });
    }

    @Override
    protected void after() {
        DatabaseTransactionFactory.performTransaction(() -> {
            DatabaseService.getInstance().clear();
            return null;
        });
        DatabaseService.reset();

        resetSystemProperty(DATABASE_SOFTWARE_PROP_NAME, databaseSoftwareName);
        resetSystemProperty(HIBERNATE_PASSWD_PROP_NAME, hibernateConnectionPassword);
        resetSystemProperty(HIBERNATE_URL_PROP_NAME, hibernateConnectionUrl);
        resetSystemProperty(HIBERNATE_USERNAME_PROP_NAME, hibernateConnectionUsername);
        resetSystemProperty(HIBERNATE_DIALECT_PROP_NAME, hibernateDialect);
        resetSystemProperty("hibernate.jdbc.batch_size", hibernateJdbcBatchSize);
        resetSystemProperty("hibernate.show_sql", hibernateShowSql);
        resetSystemProperty(HIBERNATE_DRIVER_PROP_NAME, hibernateDriverClass);
    }
}
