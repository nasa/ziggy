package gov.nasa.ziggy.services.database;

import static com.google.common.base.Preconditions.checkNotNull;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_URL;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_CLASSPATH_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX;
import static gov.nasa.ziggy.util.WrapperUtils.WRAPPER_LOG_FILE_PROP_NAME;
import static gov.nasa.ziggy.util.WrapperUtils.wrapperParameter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.hsqldb.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.services.process.ExternalProcessUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.WrapperUtils.WrapperCommand;

/**
 * Implementation of {@link DatabaseController} for HSQLDB databases.
 * <p>
 * Since we don't want to use the same RDBMS for unit tests as what's used in production, as that
 * could potentially lead to damage to a production database when unit tests are run, we use HSQLDB
 * for the tests.
 * <p>
 * Important: This class makes use of code that might be vulnerable to SQL injection. If it's used
 * for anything more than unit tests and the sample pipeline, evaluate all code marked with
 * SQL_INJECTION_JDBC. Also, the password should be set to something other than an empty string.
 * <p>
 * TODO Now that the manual touts HSQLDB as a viable database, consider the issues posed here
 *
 * @author PT
 * @author Bill Wohler
 */
public class HsqldbController extends DatabaseController {

    private static final Logger log = LoggerFactory.getLogger(HsqldbController.class);

    private static final String SCHEMA_CREATE_FILE = "ddl.hsqldb-create.sql";
    private static final String SCHEMA_DROP_FILE = "ddl.hsqldb-drop.sql";
    private static final String DRIVER_CLASS_NAME = "org.hsqldb.jdbc.JDBCDriver";

    private static final String INIT_TABLE_NAME = "HSQLDB_CONTROLLER_CREATED";
    private static final String TABLE_COUNT = "select count(*) from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name != '%s'";
    private static final String TABLE_EXISTS = "select count(*) from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name = '%s'";
    private static final String SELECT_COUNT_SQL = "select count(*) from %s";
    private static final String CREATE_INIT_TABLE_SQL = "create memory table PUBLIC.%s ( message varchar(256) )";
    private static final String TABLE_NAMES = "select table_name from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'PUBLIC' and table_name != '%s'";
    private static final String INSERT_INIT_TABLE_SQL = "insert into %s values('This database schema was automatically created on %s.')";

    private static final String HSQLDB_BIN_NAME = "hsqldb";
    private static final String LOG_FILE = "hsqldb.log";
    private static final int DATABASE_SETTLE_MILLIS = 1000;

    /**
     * Unit tests specify the {@code mem} protocol in {@link PropertyName#HIBERNATE_URL} and do not
     * use the server; the runtime server uses {@code hsql}.
     */
    private static final String MEMORY_PROTOCOL = ":mem:";

    private SqlRunner sqlRunner;
    private Path databaseDir = DirectoryProperties.databaseDir();

    /** Used by isSystemDatabase(). */
    @Override
    public Path dataDir() {
        return databaseDir;
    }

    @Override
    public Path logDir() {
        return DirectoryProperties.databaseLogDir();
    }

    @Override
    public Path logFile() {
        return logDir().resolve(LOG_FILE);
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
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void createDatabase() {
        try {
            if (start() != 0) {
                throw new PipelineException("Unable to start database");
            }

            log.info("Creating database");
            executeScript(SCHEMA_CREATE_FILE, false);

            if (!tableExists(INIT_TABLE_NAME)) {
                createInitTable();
            }

            if (tableExists(INIT_TABLE_NAME)) {
                updateInitTable();
            }
            log.info("Creating database...done");
        } catch (SQLException e) {
            throw new PipelineException("Unable to create database: " + e.getMessage(), e);
        } finally {
            sqlRunner().close();
            stop();
        }
    }

    private boolean unitTest() {
        return ZiggyConfiguration.getInstance()
            .getString(HIBERNATE_URL.property(), "")
            .contains(MEMORY_PROTOCOL);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void executeScript(String scriptName, boolean continueOnError) {
        checkNotNull(scriptName, "scriptName");

        String path = null;
        try {
            path = DirectoryProperties.databaseSchemaDir().toString();

            File scriptFile = new File(path, scriptName);
            log.debug("scriptFile={}, exists={}", scriptFile.getAbsolutePath(),
                scriptFile.exists());
            sqlRunner().executeSql(scriptFile, continueOnError);
            sqlRunner().connection().commit();
        } catch (SQLException e) {
            throw new PipelineException("Failed to execute SQL script " + path, e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private SqlRunner sqlRunner() {
        if (sqlRunner == null) {
            try {
                sqlRunner = SqlRunner.newInstance(driver());
            } catch (Exception e) {
                throw new PipelineException("Unable to initialize SqlRunner", e);
            }
        }
        return sqlRunner;
    }

    private void createInitTable() throws SQLException {
        try (Statement stmt = sqlRunner().connection().createStatement()) {
            String sqlString = String.format(CREATE_INIT_TABLE_SQL, INIT_TABLE_NAME);
            log.debug("Executing SQL: {}", sqlString);
            stmt.executeUpdate(sqlString);
            stmt.getConnection().commit();
        }
    }

    private void updateInitTable() throws SQLException {
        try (Statement stmt = sqlRunner().connection().createStatement()) {
            String sqlString = String.format(INSERT_INIT_TABLE_SQL, INIT_TABLE_NAME, new Date());
            log.debug("Executing SQL: {}", sqlString);
            stmt.executeUpdate(sqlString);
            stmt.getConnection().commit();
        }
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void dropDatabase() {
        try {
            dropDatabaseInternal();
        } catch (SQLException e) {
            throw new PipelineException("Could not drop database: " + e.getMessage(), e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void dropDatabaseInternal() throws SQLException {
        if (sqlRunner().connection() == null) {
            return;
        }

        long tableCount = -1;
        try {
            long rowCount = rowCount();
            if (rowCount == 0) {
                log.info("Database is empty");
            } else {
                log.warn("Database contains data");
            }

            tableCount = tableCount();
            if (tableCount > 0) {
                if (!tableExists(INIT_TABLE_NAME)) {
                    throw new PipelineException(
                        "Not dropping a potential live database that was not created by us");
                }
            } else {
                log.info("Database does not have any tables");
            }
        } catch (SQLException e) {
            throw new PipelineException(
                "Could not determine if database was created by us: " + e.getMessage(), e);
        }

        if (tableCount > 0) {
            try {
                log.debug("Executing script: {}", SCHEMA_DROP_FILE);
                executeScript(SCHEMA_DROP_FILE, true);
            } catch (Exception e) {
                throw new PipelineException("Drop script failed: " + e.getMessage(), e);
            }

            tableCount = tableCount();
            if (tableCount > 0) {
                throw new PipelineException(
                    "Drop script left " + tableCount + " tables in database");
            }
        }
    }

    private long tableCount() throws SQLException {
        String sqlString = String.format(TABLE_COUNT, INIT_TABLE_NAME);
        try (Statement stmt = sqlRunner().connection().createStatement();
            ResultSet rs = stmt.executeQuery(sqlString)) {
            log.debug("Executing SQL: {}", sqlString);

            if (rs.next()) {
                log.debug("{} -> {}", rs.getStatement().toString(), rs.getLong(1));
                return rs.getInt(1);
            }
            throw new PipelineException("Cannot determine the number of tables in the database");
        }
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests and the sample pipeline.
        As long as it is not used by production databases, this class
        cannot result in damage from SQL injection.
        """)
    private List<String> tableNames() throws SQLException {
        String sqlString = String.format(TABLE_NAMES, INIT_TABLE_NAME);
        List<String> tableNames = new ArrayList<>();

        try (Statement stmt = sqlRunner().connection().createStatement();
            ResultSet rs = stmt.executeQuery(sqlString)) {
            log.debug("Executing SQL: {}", sqlString);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tableNames.add(tableName);
            }

            return tableNames;
        }
    }

    private boolean tableExists(String tableNameMixedCase) throws SQLException {
        checkNotNull(tableNameMixedCase, "tableNameMixedCase");

        String tableName = tableNameMixedCase.trim().toUpperCase();
        String sqlString = String.format(TABLE_EXISTS, tableName);

        log.debug("Executing SQL: {}", sqlString);
        try (Statement stmt = sqlRunner().connection().createStatement();
            ResultSet rs = stmt.executeQuery(sqlString)) {
            if (rs.next()) {
                log.debug("{} -> {}", rs.getStatement().toString(), rs.getLong(1));
                return rs.getInt(1) == 1;
            }
            throw new PipelineException(
                "Cannot determine if the table " + tableName + " exists in the database");
        }
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests and the sample pipeline.
        As long as it is not used by production databases, this class
        cannot result in damage from SQL injection.
        """)
    private long rowCount() throws SQLException {
        long rowCount = 0;
        for (String tableName : tableNames()) {
            String sqlString = String.format(SELECT_COUNT_SQL, tableName);
            try (Statement stmt = sqlRunner().connection().createStatement();
                ResultSet rs = stmt.executeQuery(sqlString)) {
                if (rs.next()) {
                    rowCount += rs.getLong(1);
                }
            }
        }

        return rowCount;
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @Override
    public int start() {
        if (unitTest()) {
            return 0;
        }

        CommandLine supervisorStatusCommand = hsqldbCommand(WrapperCommand.START);
        log.debug("Command line: " + supervisorStatusCommand.toString());
        int exitStatus = ExternalProcess.simpleExternalProcess(supervisorStatusCommand)
            .exceptionOnFailure()
            .execute();
        try {
            Thread.sleep(DATABASE_SETTLE_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return exitStatus;
    }

    @Override
    public int stop() {
        if (unitTest()) {
            return 0;
        }

        CommandLine supervisorStopCommand = hsqldbCommand(WrapperCommand.STOP);
        log.debug("Command line: " + supervisorStopCommand.toString());
        return ExternalProcess.simpleExternalProcess(supervisorStopCommand).execute(true);
    }

    @Override
    public int status() {
        if (unitTest()) {
            return 0;
        }

        CommandLine supervisorStatusCommand = hsqldbCommand(WrapperCommand.STATUS);
        log.debug("Command line: " + supervisorStatusCommand);
        return ExternalProcess.simpleExternalProcess(supervisorStatusCommand).execute();
    }

    @Override
    public boolean dbExists() {
        return Files.exists(databaseDir);
    }

    private CommandLine hsqldbCommand(WrapperCommand cmd) {
        Path hsqldbPath = DirectoryProperties.ziggyBinDir().resolve(HSQLDB_BIN_NAME);
        CommandLine commandLine = new CommandLine(hsqldbPath.toString());
        if (cmd == WrapperCommand.START) {
            // Refer to hsqldb.wrapper.conf for appropriate indices for the parameters specified
            // here.
            String ziggyLibDir = DirectoryProperties.ziggyLibDir().toString();

            commandLine
                .addArgument(wrapperParameter(WRAPPER_LOG_FILE_PROP_NAME, logFile().toString()))
                .addArgument(wrapperParameter(WRAPPER_CLASSPATH_PROP_NAME_PREFIX, 1,
                    DirectoryProperties.ziggyHomeDir().resolve("libs").resolve("*.jar").toString()))
                .addArgument(
                    wrapperParameter(WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX, 1, ziggyLibDir))
                .addArgument(wrapperParameter(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 3,
                    ExternalProcessUtils.log4jConfigString()))
                .addArgument(wrapperParameter(WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX, 4,
                    ExternalProcessUtils.ziggyLog(logFile().toString())));
        }

        return commandLine.addArgument(cmd.toString());
    }

    public static void main(String[] args) {
        new HsqldbController().startServer();
    }

    private void startServer() {
        Server server = new Server();
        server.setDatabaseName(0, dbName());
        server.setDatabasePath(0, dataDir().resolve(dbName()).toString());
        server.start();
    }
}
