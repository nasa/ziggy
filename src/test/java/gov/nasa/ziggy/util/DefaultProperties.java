package gov.nasa.ziggy.util;

import java.io.File;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * This class has methods to set various properties for various tests.
 *
 * @author Miles Cote
 * @author Bill Wohler
 * @author Forrest Girouard
 */
public class DefaultProperties {
    private static final Logger log = LoggerFactory.getLogger(DefaultProperties.class);

    // Directories
    public static final String XML = "xml/";

    public static final String SEED_DATA = DirectoryProperties.pipelineHomeDir() + "/seed-data";

    public static final String HSQLDB = "hsqldb";

    public static final String HSQLDB_SCHEMA = "schema/" + HSQLDB;

    // Properties
    private static final String UNIT_TEST_DATA_DIR_PROP = "soc.test.data.dir";

    private static final String UNIT_TEST_LOCAL_DATA_DIR_PROP = "soc.test.local.data.dir";

    private static final String USE_LOCAL_DATA_PROP = "use.local.data";

    public static final String TEST_SCHEMA_DIR_PROP = "soc.test.schema.dir";

    private static final String HSQLDB_SCHEMA_DIR_PROP = "soc.hsqldb.schema.dir";

    private static final String HSQLDB_SCHEMA_DIR_DEFAULT = Filenames.BUILD_TEST + "/"
        + HSQLDB_SCHEMA;

    /**
     * Name of the property that indicates where the XML files are stored
     */
    public static final String MODULE_XML_DIR_PROPERTY_NAME = "pi.worker.module.xmlDir";

    public enum DatabaseType {
        HSQLDB, HSQLDB_FILE, ORACLE, DERBY, DERBY_EMBEDDED
    }

    /**
     * Returns the location of the unit test data repository for the given CSCI.
     * <p>
     * The basename for this directory is defined by the system property {@code soc.test.data.dir}
     * which is typically set by {@code ant test}. Otherwise, the value of
     * {@link UNIT_TEST_DATA_DIR_DEFAULT}, or {@value UNIT_TEST_DATA_DIR_DEFAULT} is used. The CSCI
     * name is appended to this directory name and this result is returned.
     *
     * @return the unit test data directory for the given CSCI.
     */
    public static String getUnitTestDataDir(String csci) {
        String unitTestDataDir = null;
        boolean useLocalData = false;
        String useLocalDataStr = System.getProperty(USE_LOCAL_DATA_PROP);
        if (useLocalDataStr != null && useLocalDataStr.length() > 0) {
            useLocalData = Boolean.valueOf(useLocalDataStr);
            log.debug(String.format("%s=%s", USE_LOCAL_DATA_PROP, useLocalData));
        }
        if (useLocalData) {
            unitTestDataDir = System.getProperty(UNIT_TEST_LOCAL_DATA_DIR_PROP);
            log.debug(String.format("%s=%s", "unitTestDataDir", unitTestDataDir));
        }

        if (unitTestDataDir == null) {
            unitTestDataDir = System.getProperty(UNIT_TEST_DATA_DIR_PROP);
            log.debug(String.format("%s=%s", "unitTestDataDir", unitTestDataDir));
        }

        return unitTestDataDir + File.separator + csci;
    }

    /**
     * Returns the location of the HSQLDB schema directory.
     * <p>
     * The basename for this directory is defined by the system property
     * {@code soc.hsqldb.schema.dir} which is typically set by {@code ant test}. Otherwise, the
     * value of {@link HSQLDB_SCHEMA_DIR_DEFAULT}, or {@value HSQLDB_SCHEMA_DIR_DEFAULT} is used.
     *
     * @return the test schema directory.
     */
    public static synchronized String getHsqldbSchemaDir() {
        String hdqldbSchemaDir = System.getProperty(HSQLDB_SCHEMA_DIR_PROP);
        if (hdqldbSchemaDir == null) {
            hdqldbSchemaDir = HSQLDB_SCHEMA_DIR_DEFAULT;
        }
        return hdqldbSchemaDir;
    }

    public static void setPropsPipelineModule(String dataDir, String workingDir,
        Properties databaseProperties) {
        setPropsPipelineModule(dataDir, SEED_DATA, XML, databaseProperties);
    }

    public static void setPropsPipelineModule(String dataDir, String seedDataDir, String xmlDir,
        Properties databaseProperties) {

        databaseProperties.setProperty(MODULE_XML_DIR_PROPERTY_NAME, xmlDir);
        databaseProperties.setProperty("seedData.dir", seedDataDir);
    }

    public static void setPropsOracle() {
        Properties systemProperties = System.getProperties();
        setPropsOracle(systemProperties);
        System.setProperties(systemProperties);
    }

    public static void setPropsOracle(Properties databaseProperties) {
        databaseProperties.setProperty("hibernate.connection.driver_class",
            "oracle.jdbc.driver.OracleDriver");
        databaseProperties.setProperty("hibernate.connection.url",
            "jdbc:oracle:thin:@host.example.com:4242:database");
        databaseProperties.setProperty("hibernate.connection.username",
            databaseProperties.getProperty("user.name"));
        databaseProperties.setProperty("hibernate.connection.password",
            databaseProperties.getProperty("user.name"));
        databaseProperties.setProperty("hibernate.dialect", "org.hibernate.dialect.OracleDialect");
        databaseProperties.setProperty("hibernate.jdbc.batch_size", "0");
        databaseProperties.setProperty("hibernate.show_sql", "false");
    }

    public static void setPropsDerby(Properties databaseProperties) {
        databaseProperties.setProperty("hibernate.connection.driver_class",
            "org.apache.derby.jdbc.ClientDriver");
        databaseProperties.setProperty("hibernate.connection.url",
            "jdbc:derby://localhost:1527/schema/derbydb;create=true");
        databaseProperties.setProperty("hibernate.connection.username", "ziggy");
        databaseProperties.setProperty("hibernate.connection.password", "ziggy");
        databaseProperties.setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect");
    }

    public static void setPropsDerbyEmbedded(Properties databaseProperties) {
        databaseProperties.setProperty("hibernate.connection.driver_class",
            "org.apache.derby.jdbc.EmbeddedDriver");
        databaseProperties.setProperty("hibernate.connection.url",
            "jdbc:derby:schema/derbydb;create=true");
        databaseProperties.setProperty("hibernate.connection.username", "ziggy");
        databaseProperties.setProperty("hibernate.connection.password", "");
        databaseProperties.setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect");
    }

    /**
     * Sets up the properties for a feature test using Oracle. Use
     * {@link #getUnitTestDataDir(String)} to generate a good starting point for {@code dataDir}.
     */
    public static void setPropsForFeatureTest(String dataDir) {
        setPropsForFeatureTest(dataDir, DatabaseType.ORACLE);
    }

    /**
     * Sets up the properties for a feature test using the given directory and database type. Use
     * {@link #getUnitTestDataDir(String)} to generate a good starting point for {@code dataDir}.
     */
    public static void setPropsForFeatureTest(String dataDir, DatabaseType databaseType) {
        Properties systemProperties = System.getProperties();
        setPropsForFeatureTest(dataDir, XML, databaseType, systemProperties);
        System.setProperties(systemProperties);
    }

    public static void setPropsForFeatureTest(String dataDir, String xmlDir,
        DatabaseType databaseType, Properties databaseProperties) {
        setPropsPipelineModule(dataDir, SEED_DATA, xmlDir, databaseProperties);

        switch (databaseType) {
            case ORACLE:
                setPropsOracle(databaseProperties);
                break;
            case DERBY:
                setPropsDerby(databaseProperties);
                break;
            case DERBY_EMBEDDED:
                setPropsDerbyEmbedded(databaseProperties);
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + databaseType);
        }
    }

    /**
     * These are used by RMI servers to establish an end point.
     */
    public static void setLocalRmiProps() {
        System.getProperties().setProperty("com.sun.management.jmxremote", "");
        System.getProperties().setProperty("com.sun.management.jmxremote.authenticate", "false");
        System.getProperties().setProperty("com.sun.management.jmxremote.port", "1234");
        System.getProperties().setProperty("com.sun.management.jmxremote.ssl", "false");
        System.getProperties().setProperty("java.rmi.server.hostname", "127.0.0.1");
    }
}
