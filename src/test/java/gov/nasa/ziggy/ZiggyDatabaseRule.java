package gov.nasa.ziggy;

import static gov.nasa.ziggy.services.config.PropertyName.DATABASE_SCHEMA_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.DATABASE_SOFTWARE;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_DIALECT;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_DRIVER;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_JDBC_BATCH_SIZE;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_PASSWORD;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_SHOW_SQL;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_URL;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_USERNAME;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;

import java.io.File;

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
    private String homeDir;
    private String databaseSchemaDir;

    private DatabaseController databaseController;

    @Override
    protected void before() throws Throwable {
        databaseSoftwareName = System.setProperty(DATABASE_SOFTWARE.property(), "hsqldb");
        databaseController = new HsqldbController();

        hibernateConnectionPassword = System.setProperty(HIBERNATE_PASSWORD.property(), "");
        hibernateConnectionUrl = System.setProperty(HIBERNATE_URL.property(),
            "jdbc:hsqldb:mem:hsqldb-ziggy");
        hibernateConnectionUsername = System.setProperty(HIBERNATE_USERNAME.property(), "sa");
        homeDir = System.setProperty(ZIGGY_HOME_DIR.property(), "build");
        databaseSchemaDir = System.setProperty(DATABASE_SCHEMA_DIR.property(),
            "build" + File.separator + "schema");

        // For some reason, the unit tests won't run successfully without the Hibernate
        // dialect being set as a system property, even though the actual pipelines
        // function just fine without any dialect in the properties. Something to figure
        // out and fix when possible.
        hibernateDialect = System.setProperty(HIBERNATE_DIALECT.property(),
            databaseController.sqlDialect().dialect());
        hibernateDriverClass = System.setProperty(HIBERNATE_DRIVER.property(),
            databaseController.driver());

        hibernateJdbcBatchSize = System.setProperty(HIBERNATE_JDBC_BATCH_SIZE.property(), "0");
        hibernateShowSql = System.setProperty(HIBERNATE_SHOW_SQL.property(), "false");

        DatabaseTransactionFactory.performTransaction(() -> {
            databaseController.createDatabase();
            return null;
        });
    }

    @Override
    protected void after() {
        DatabaseTransactionFactory.performTransaction(() -> {
            databaseController.dropDatabase();
            DatabaseService.getInstance().clear();
            return null;
        });
        DatabaseService.reset();

        resetSystemProperty(DATABASE_SOFTWARE.property(), databaseSoftwareName);
        resetSystemProperty(HIBERNATE_PASSWORD.property(), hibernateConnectionPassword);
        resetSystemProperty(HIBERNATE_URL.property(), hibernateConnectionUrl);
        resetSystemProperty(HIBERNATE_USERNAME.property(), hibernateConnectionUsername);
        resetSystemProperty(HIBERNATE_DIALECT.property(), hibernateDialect);
        resetSystemProperty(HIBERNATE_JDBC_BATCH_SIZE.property(), hibernateJdbcBatchSize);
        resetSystemProperty(HIBERNATE_SHOW_SQL.property(), hibernateShowSql);
        resetSystemProperty(HIBERNATE_DRIVER.property(), hibernateDriverClass);
        resetSystemProperty(ZIGGY_HOME_DIR.property(), homeDir);
        resetSystemProperty(DATABASE_SCHEMA_DIR.property(), databaseSchemaDir);
    }

    /**
     * Sets the given property to the given value. If {@code value} is {@code null}, the property is
     * cleared.
     *
     * @param property the property to set
     * @param value the value to set the property to, or {@code null} to clear the property
     */
    public static void resetSystemProperty(String property, String value) {
        if (value != null) {
            System.setProperty(property, value);
        } else {
            System.clearProperty(property);
        }
    }
}
