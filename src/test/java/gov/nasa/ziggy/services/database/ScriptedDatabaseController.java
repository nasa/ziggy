package gov.nasa.ziggy.services.database;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.module.PipelineException;

/**
 * Intermediate abstract class that provides support for {@link DatabaseController} implementations
 * that make use of a script and an {@link SqlRunner} instance to perform their initializations.
 * This is a reinterpretation of the now-obsolete ScriptedDdlInitializer into the context of the
 * {@link DatabaseController}.
 *
 * @author PT
 */
public abstract class ScriptedDatabaseController extends DatabaseController {

    private static final Logger log = LoggerFactory.getLogger(ScriptedDatabaseController.class);

    static final String INIT_TABLE_NAME = "DDL_INITIALIZER_CREATED";
    private static final String TABLE_COUNT = "select count(*) from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name != '%s'";
    private static final String TABLE_EXISTS = "select count(*) from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name = '%s'";
    static final String DROP_TABLE_SQL = "drop table %s";
    static final String SELECT_COUNT_SQL = "select count(*) from %s";
    private static final String CREATE_INIT_TABLE_SQL = "create memory table PUBLIC.%s ( message varchar(256) )";
    private static final String TABLE_NAMES = "select table_name from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name != '%s'";
    private static final String INSERT_INIT_TABLE_SQL = "insert into %s values('This database schema was automatically created on %s.')";

    protected SqlRunner sqlRunner;

    protected SqlRunner sqlRunner() {
        if (sqlRunner == null) {
            sqlRunner = initializeSqlRunner();
        }
        return sqlRunner;
    }

    protected abstract SqlRunner initializeSqlRunner();

    protected abstract String createScript();

    protected abstract String dropScript();

    protected void executeScript(String scriptName, boolean continueOnError) {
        if (scriptName == null) {
            throw new NullPointerException("scriptName can't be null");
        }

        String path = null;
        try {
            path = "build/schema/" + scriptName;

            File scriptFile = new File(path);
            log.info("scriptFile: " + scriptFile.getAbsolutePath());
            log.info("Script exists: " + scriptFile.exists());
            sqlRunner().executeSql(scriptFile, continueOnError);
            sqlRunner().getCachedConnection().commit();
        } catch (SQLException sqle) {
            log.error("failed to execute: " + path, sqle);
            throw new PipelineException(sqle.getMessage(), sqle);
        }
    }

    public synchronized void initDB() throws SQLException {
        sqlRunner().connect();

        long tableCount = tableCount();
        if (tableCount > 0) {
            try {
                // Try to run the drop script (ignoring errors), just in
                // case someone didn't clean up after themselves...
                log.info("trying clean script: " + dropScript());
                cleanDbInternal();
            } catch (Throwable t) {
                log.warn(t.getMessage(), t);
            }
            tableCount = tableCount();
            if (tableCount > 0) {
                log.warn("Failed to clean database");
            }
        }

        if (tableCount <= 1) {
            log.info("executing create script: " + createScript());
            executeScript(createScript(), false);

            if (!tableExists(INIT_TABLE_NAME)) {
                createInitTable();
            }

            if (tableExists(INIT_TABLE_NAME)) {
                updateInitTable();
            }
        } else {
            log.error("Failed to create database");
        }
    }

    public void cleanDb() {
        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }
        cleanDbInternal();
    }

    /**
     * Make best effort to run drop script (errors are ignored)
     */
    private void cleanDbInternal() {
        if (sqlRunner().getCachedConnection() == null) {
            return;
        }

        long rowCount = -1;
        long tableCount = -1;
        try {
            rowCount = rowCount();
            if (rowCount == 0) {
                log.info("No rows in any tables!");
            } else {
                log.warn("Database contains data.");
            }

            tableCount = tableCount();
            if (tableCount > 0) {
                boolean initTableExists = tableExists(INIT_TABLE_NAME);

                if (!initTableExists) {
                    throw new PipelineException("Database was not created by DDLInitalizer."
                        + " Refusing to clean a potential live database.");
                }
            } else {
                log.info("Database contains no tables!");
            }
        } catch (SQLException sqle) {
            throw new PipelineException("Database may not have been created by DDLInitalizer.",
                sqle);
        }

        if (tableCount > 0) {
            try {
                log.info("executing clean script: " + dropScript());
                executeScript(dropScript(), true);
            } catch (Exception e) {
                throw new PipelineException("clean script failed: ", e);
            }
        }
    }

    public void createInitTable() throws SQLException {
        String sqlString = String.format(CREATE_INIT_TABLE_SQL, INIT_TABLE_NAME);
        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        Statement stmt = null;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            stmt.executeUpdate(sqlString);
        } catch (Throwable ignore) {
            log.debug(String.format("create %s table failed", INIT_TABLE_NAME), ignore);
        } finally {
            stmt.getConnection().commit();
            close(stmt);
        }
    }

    public void updateInitTable() throws SQLException {
        String sqlString = String.format(INSERT_INIT_TABLE_SQL, INIT_TABLE_NAME, new Date());
        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        Statement stmt = null;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            stmt.executeUpdate(sqlString);
        } catch (Throwable ignore) {
            log.debug(String.format("insert into %s table failed", INIT_TABLE_NAME), ignore);
        } finally {
            stmt.getConnection().commit();
            close(stmt);
        }
    }

    public boolean tableExists(String tableNameMixedCase) {
        String tableName = tableNameMixedCase.trim().toUpperCase();
        String sqlString = String.format(TABLE_EXISTS, tableName);
        if (sqlString == null) {
            throw new NullPointerException("sqlString can't be null");
        }
        if (tableName == null) {
            throw new NullPointerException("tableName can't be null");
        }

        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        Statement stmt = null;
        ResultSet rs = null;
        int count = -1;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            rs = stmt.executeQuery(sqlString);
            if (rs.next()) {
                log.debug(String.format("%s => %d", rs.getStatement().toString(), rs.getLong(1)));
                count = rs.getInt(1);
            } else {
                log.warn(String.format("cannot determine if the '%s' table exists in the database!",
                    tableName));
            }
        } catch (SQLException sqle) {
            log.warn(String.format("cannot determine if the '%s' table exists in the database: ",
                tableName), sqle);
        } finally {
            close(rs);
            close(stmt);
        }

        return count == 1;
    }

    public long tableCount() {
        String sqlString = String.format(TABLE_COUNT, INIT_TABLE_NAME);
        if (sqlString == null) {
            throw new NullPointerException("sqlString can't be null");
        }

        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        int tableCount = -1;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            rs = stmt.executeQuery(sqlString);
            if (rs.next()) {
                log.debug(String.format("%s => %d", rs.getStatement().toString(), rs.getLong(1)));
                tableCount = rs.getInt(1);
            } else {
                log.warn("cannot determine the number of tables in the database!");
            }
        } catch (Throwable t) {
            log.warn("cannot determine the number of tables in the database: ", t);
        } finally {
            close(rs);
            close(stmt);
        }

        return tableCount;
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests, thus the production database
        cannot be compromised by SQL injection.
        """)
    public List<String> tableNames() throws SQLException {
        String sqlString = String.format(TABLE_NAMES, INIT_TABLE_NAME);
        if (sqlString == null) {
            throw new NullPointerException("sqlString can't be null");
        }

        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        List<String> tableNames = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            rs = stmt.executeQuery(sqlString);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tableNames.add(tableName);
            }

            return tableNames;
        } finally {
            close(rs);
            close(stmt);
        }
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests, thus the production database
        cannot be compromised by SQL injection.
        """)
    public long rowCount() throws SQLException {
        List<String> tableNamesList = tableNames();
        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        Statement stmt = null;
        ResultSet rs = null;
        long rowCount = 0;
        try {
            for (String tableName : tableNamesList) {
                String sqlString = String.format(SELECT_COUNT_SQL, tableName);
                stmt = sqlRunner().getCachedConnection().createStatement();
                rs = stmt.executeQuery(sqlString);
                if (rs.next()) {
                    rowCount += rs.getLong(1);
                }
            }

            return rowCount;
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            close(rs);
            close(stmt);
        }
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests, thus the production database
        cannot be compromised by SQL injection.
        """)
    protected boolean dropTable(String tableName) throws SQLException {
        if (tableName == null) {
            throw new NullPointerException("tableName can't be null");
        }

        if (sqlRunner().getCachedConnection() == null) {
            sqlRunner().connect();
        }

        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("No such table: " + tableName);
        }

        String sqlString = String.format(DROP_TABLE_SQL, tableName);
        Statement stmt = null;
        try {
            log.debug("executing SQL: " + sqlString);
            stmt = sqlRunner().getCachedConnection().createStatement();
            stmt.executeUpdate(sqlString);
            return true;
        } catch (Throwable ignore) {
            log.debug(String.format("drop %s table failed", tableName), ignore);
        } finally {
            stmt.getConnection().commit();
            close(stmt);
        }

        return false;
    }

    protected static void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignore) {
            }
        }
    }

    protected static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignore) {
            }
        }
    }

}
