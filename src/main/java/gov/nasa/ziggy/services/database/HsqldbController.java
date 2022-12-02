package gov.nasa.ziggy.services.database;

import java.nio.file.Path;
import java.sql.SQLException;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Implementation of {@link DatabaseController} for HSQLDB databases. At the moment, HSQLDB is used
 * only for unit tests that require a database (we don't want to use the same RDBMS as what's used
 * in production, as that could potentially lead to damage to a production database when unit tests
 * are run). For this reason most of the methods are stubs. If and when a use-case arises that
 * requires a more complete implementation, we'll make one.
 *
 * @author PT
 */
public class HsqldbController extends ScriptedDatabaseController {

    private static final String SCHEMA_CREATE_FILE = "ddl.hsqldb-create.sql";
    private static final String SCHEMA_DROP_FILE = "ddl.hsqldb-drop.sql";
    private static final String DRIVER_CLASS_NAME = "org.hsqldb.jdbcDriver";

    @Override
    protected SqlRunner initializeSqlRunner() {
        try {
            return SqlRunner.newInstance(driver());
        } catch (Exception e) {
            throw new PipelineException("Unable to initialize SqlRunner", e);
        }
    }

    @Override
    protected String createScript() {
        return SCHEMA_CREATE_FILE;
    }

    @Override
    protected String dropScript() {
        return SCHEMA_DROP_FILE;
    }

    @Override
    public Path dataDir() {
        return null;
    }

    @Override
    public Path logDir() {
        return null;
    }

    @Override
    public Path logFile() {
        return null;
    }

    @Override
    public SqlDialect sqlDialect() {
        return SqlDialect.HSQLDB;
    }

    @Override
    public String driver() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public void createDatabase() {
        try {
            initDB();
        } catch (SQLException e) {
            throw new PipelineException("Unable to create database", e);
        }
    }

    @Override
    public void dropDatabase() {
        cleanDB();
    }

    @Override
    public boolean dbExists() {
        return false;
    }

    @Override
    public int start() {
        return 0;
    }

    @Override
    public int stop() {
        return 0;
    }

    @Override
    public int status() {
        return 0;
    }
}
