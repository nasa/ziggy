package gov.nasa.ziggy.services.database;

import java.nio.file.Path;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Provides an abstraction of the database used by Ziggy. The user specifies the database from one
 * of the supported flavors, and a subclass of this class will then be provided by the
 * {@link DatabaseController#newInstance()} method.
 *
 * @see SqlDialect
 * @author PT
 * @author Bill Wohler
 */
public abstract class DatabaseController {

    public static final int NOT_SUPPORTED = -1;

    /**
     * Obtains an instance of the correct subclass of {@link DatabaseController}.
     *
     * @return a new instance of the appropriate database controller, or {@code null} if the user
     * has not specified a value for property {@link PropertyName#DATABASE_SOFTWARE} or the database
     * associated with the given value is unsupported
     */
    public static DatabaseController newInstance() {

        String databaseName = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_SOFTWARE.property(), null);
        if (databaseName == null || databaseName.trim().isEmpty()) {
            return null;
        }

        SqlDialect dialect = SqlDialect.valueOf(databaseName.toUpperCase());
        return switch (dialect) {
            case HSQLDB -> new HsqldbController();
            case POSTGRESQL -> new PostgresqlController();
            default -> null;
        };
    }

    /**
     * Absolute location of the data directory.
     *
     * @return non-null path, or null if system database prevents access
     */
    public abstract Path dataDir();

    /**
     * Returns whether the database is embedded ({@code dataDir() != null}) or whether it is an
     * external, system database ({@code dataDir() == null}).
     *
     * @return true if a system database is being used; false if an embedded database is in use
     */
    public boolean isSystemDatabase() {
        return dataDir() == null;
    }

    /**
     * Absolute location of the log directory.
     *
     * @return non-null path, null if system database prevents access
     */
    public abstract Path logDir();

    /**
     * Returns the name of the log file used by the database. This is used during database
     * initialization to set the location used by the database for its logging needs.
     */
    public abstract Path logFile();

    /**
     * SQL dialect.
     *
     * @return non-null
     */
    public abstract SqlDialect sqlDialect();

    /**
     * Returns the driver class for the database (for example, "org.postgresql.Driver").
     */
    public abstract String driver();

    /**
     * Returns the maximum number of connections to the database. See
     * {@link PropertyName#DATABASE_CONNECTIONS}.
     */
    public String maxConnections() {
        return ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_CONNECTIONS.property());
    }

    /**
     * Creates the database tables.
     */
    public abstract void createDatabase();

    /**
     * Deletes the database tables.
     */
    public abstract void dropDatabase();

    /**
     * Starts the database.
     *
     * @return 0 if successful, &gt; 0 on error, or {@link #NOT_SUPPORTED} if not supported
     */
    public abstract int start();

    /**
     * Stops the database.
     *
     * @return 0 if successful, &gt; 0 on error, or {@link #NOT_SUPPORTED} if not supported
     */
    public abstract int stop();

    /**
     * Gets the database status.
     *
     * @return 0 for database running, &gt; 0 for any "not running" outcome, or
     * {@link #NOT_SUPPORTED} if not supported
     */
    public abstract int status();

    /**
     * Returns the host name of the database. See {@link PropertyName#DATABASE_HOST}.
     */
    public String host() {
        return ZiggyConfiguration.getInstance().getString(PropertyName.DATABASE_HOST.property());
    }

    /**
     * Returns the port number for the database. See {@link PropertyName#DATABASE_PORT}.
     */
    public int port() {
        return ZiggyConfiguration.getInstance().getInt(PropertyName.DATABASE_PORT.property());
    }

    /**
     * Returns the database name. See {@link PropertyName#DATABASE_NAME}.
     */
    public String dbName() {
        return ZiggyConfiguration.getInstance().getString(PropertyName.DATABASE_NAME.property());
    }

    /**
     * Checks whether the database specified by {@link #dbName()} exists.
     */
    public abstract boolean dbExists();
}
