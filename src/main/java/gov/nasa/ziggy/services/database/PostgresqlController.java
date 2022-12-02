package gov.nasa.ziggy.services.database;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.commons.exec.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Implementation of {@link DatabaseController} for PostgreSQL use. If the
 * {@value PropertyNames#DATABASE_DIR_PROP_NAME} property is empty, then the system database is
 * assumed. A system database cannot be started or stopped, nor can a database be initialized or
 * created. However, the Ziggy schema and tables can be created.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PostgresqlController extends DatabaseController {

    private static final Logger log = LoggerFactory.getLogger(DatabaseController.class);

    private static final String PG_CTL = "pg_ctl";
    private static final String INITDB = "initdb";
    private static final String CREATEDB = "createdb";
    private static final String PSQL = "psql";
    private static final String SCHEMA_CREATE_FILE = "ddl.postgresql-create.sql";
    private static final String SCHEMA_DROP_FILE = "ddl.postgresql-drop.sql";
    private static final String SCHEMA_EXTRA_PREFIX = "ddl.postgresql-create-extra-";
    private static final String SCHEMA_EXTRA_SUFFIX = ".sql";
    private static final String TIMEOUT_SECONDS = "15";
    private static final String LOG_FILE = "pg.log";
    private static final String PGDATA_DIR = "pgdata";
    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
    private static final String CONFIG_FILE_NAME = "postgresql.conf";
    private static final String LOCK_FILE_DIR_NAME = "lockfiles";

    private Path databaseDir = DirectoryProperties.databaseDir();

    @Override
    public Path dataDir() {
        return databaseDir != null ? databaseDir.resolve(PGDATA_DIR) : null;
    }

    @Override
    public Path logDir() {
        return databaseDir != null ? DirectoryProperties.databaseLogDir() : null;
    }

    @Override
    public Path logFile() {
        return databaseDir != null ? logDir().resolve(LOG_FILE) : null;
    }

    @Override
    public SqlDialect sqlDialect() {
        return SqlDialect.POSTGRESQL;
    }

    @Override
    public String driver() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public void createDatabase() {

        if (databaseDir != null) {
            initializeDatabase();
            try {
                updateConfigFile();
            } catch (IOException e) {
                throw new PipelineException("Unable to update configuration file", e);
            }

            int dbStartRetCode = start();
            if (dbStartRetCode != 0) {
                throw new PipelineException("Unable to start initialized database");
            }
        }

        if (databaseDir != null) {
            log.info("Creating database");
            try {
                CommandLine createCommand = new CommandLine(commandStringWithPath(CREATEDB));
                createCommand.addArgument("-h").addArgument(host());
                createCommand.addArgument("-p");
                createCommand.addArgument(Integer.toString(port()));
                createCommand.addArgument(dbName());
                log.debug("Command line: " + createCommand.toString());
                if (ExternalProcess.simpleExternalProcess(createCommand).execute(true) != 0) {
                    throw new PipelineException("Unable to create database " + dbName());
                }
            } catch (Exception e) {
                throw new PipelineException("Unable to create database", e);
            }
            log.info("Creating database...done");
        }

        log.info("Setting up database");
        try {
            CommandLine schemaCommand = psqlCommand();
            schemaCommand.addArgument("-f");
            schemaCommand.addArgument(
                DirectoryProperties.ziggySchemaDir().resolve(SCHEMA_CREATE_FILE).toString());
            log.debug("Command line: " + schemaCommand.toString());
            ExternalProcess.simpleExternalProcess(schemaCommand).exceptionOnFailure().execute(true);

            File[] extraFiles = DirectoryProperties.ziggySchemaDir()
                .toFile()
                .listFiles((FilenameFilter) (dir, name) -> name.startsWith(SCHEMA_EXTRA_PREFIX)
                    && name.endsWith(SCHEMA_EXTRA_SUFFIX));
            for (File extraFile : extraFiles) {
                CommandLine extraSchemaCommand = psqlCommand();
                extraSchemaCommand.addArgument("-f");
                extraSchemaCommand.addArgument(extraFile.getAbsolutePath());
                log.debug("Command line: " + extraSchemaCommand.toString());
                ExternalProcess.simpleExternalProcess(extraSchemaCommand)
                    .exceptionOnFailure()
                    .execute(true);
            }
        } catch (Exception e) {
            throw new PipelineException("Unable to set up database", e);
        } finally {
            stop();
        }
        log.info("Setting up database...done");
    }

    /**
     * Calls initdb and creates the log directory.
     */
    private void initializeDatabase() {

        log.info("Initializing database");
        CommandLine commandLine = new CommandLine(commandStringWithPath(INITDB));
        commandLine.addArgument("-D");
        commandLine.addArgument(dataDir().toString());
        log.debug("Command line: " + commandLine.toString());
        int retCode = ExternalProcess.simpleExternalProcess(commandLine).execute(true);
        if (retCode != 0) {
            throw new PipelineException("Database initialization failed");
        }

        // Create directory for log files.
        try {
            Files.createDirectories(logDir());
        } catch (IOException e) {
            throw new PipelineException("Unable to create log directory", e);
        }
        log.info("Initializing database...done");
    }

    private void updateConfigFile() throws IOException {

        // Create a directory for lock files. Postgres has a limit of 107 characters for
        // unix_socket_directories, so remove any redundancies from the path.
        Path lockFileDir = DirectoryProperties.databaseDir()
            .resolve(LOCK_FILE_DIR_NAME)
            .normalize();
        Files.createDirectory(lockFileDir);

        // Append to the postgresql.conf file an optional inclusion of the user's
        // config file and a setting of the lock file directory.
        log.info("Creating configuration file in " + dataDir().toString());
        Path configPath = dataDir().resolve(CONFIG_FILE_NAME);
        String newline = System.getProperty("line.separator");
        StringBuilder confFileContents = new StringBuilder();
        confFileContents.append("unix_socket_directories = '" + lockFileDir + "'" + newline);
        if (DirectoryProperties.databaseConfFile() != null) {
            confFileContents
                .append("include '" + DirectoryProperties.databaseConfFile() + "'" + newline);
        }
        Files.write(configPath, confFileContents.toString().getBytes(), StandardOpenOption.APPEND);
        log.info("Creating configuration file in " + dataDir().toString() + "...done");
    }

    @Override
    public void dropDatabase() {
        CommandLine schemaCommand = psqlCommand();
        schemaCommand.addArgument("-f");
        schemaCommand
            .addArgument(DirectoryProperties.ziggySchemaDir().resolve(SCHEMA_DROP_FILE).toString());
        log.debug("Command line: " + schemaCommand.toString());
        ExternalProcess.simpleExternalProcess(schemaCommand).exceptionOnFailure().execute(true);
    }

    @Override
    public int start() {
        if (databaseDir == null) {
            return NOT_SUPPORTED;
        }

        String maxConnections = maxConnections();
        CommandLine startCommand = pgctlCommandLine(true, true);
        startCommand.addArgument("-w");
        startCommand.addArgument("-t");
        startCommand.addArgument(TIMEOUT_SECONDS);
        if (maxConnections != null && !maxConnections.isEmpty()) {
            startCommand.addArgument("-o");
            startCommand.addArgument("\"-N " + maxConnections + "\"");
        }
        startCommand.addArgument("start");
        startCommand.addArgument("-l");
        startCommand.addArgument(logFile().toString());
        log.debug("Command line: " + startCommand.toString());
        return ExternalProcess.simpleExternalProcess(startCommand)
            .exceptionOnFailure(true)
            .execute(true);
    }

    @Override
    public int stop() {
        if (databaseDir == null) {
            return NOT_SUPPORTED;
        }

        CommandLine stopCommand = pgctlCommandLine(false, true);
        stopCommand.addArgument("-w");
        stopCommand.addArgument("-t");
        stopCommand.addArgument(TIMEOUT_SECONDS);
        stopCommand.addArgument("-m");
        stopCommand.addArgument("fast");
        stopCommand.addArgument("stop");
        log.debug("Command line: " + stopCommand.toString());
        return ExternalProcess.simpleExternalProcess(stopCommand).execute(true);
    }

    @Override
    public int status() {
        if (databaseDir == null) {
            return NOT_SUPPORTED;
        }

        CommandLine commandLine = pgctlCommandLine(true, true);
        commandLine.addArgument("status");
        log.debug("Command line: " + commandLine.toString());
        return ExternalProcess.simpleExternalProcess(commandLine).execute(true);
    }

    @Override
    public boolean dbExists() {
        if (databaseDir != null) {
            return true;
        }

        CommandLine dbQueryCommand = psqlCommand().addArgument("--command").addArgument("");
        log.debug("Command line: " + dbQueryCommand.toString());

        return ExternalProcess.simpleExternalProcess(dbQueryCommand).execute(true) == 0;
    }

    private CommandLine pgctlCommandLine(boolean includePort, boolean includeDataDir) {
        CommandLine commandLine = new CommandLine(commandStringWithPath(PG_CTL));
        if (includePort) {
            commandLine.addArgument("-o");
            commandLine.addArgument("\"-p " + Integer.toString(port()) + "\"");
        }
        if (includeDataDir) {
            commandLine.addArgument("-D");
            commandLine.addArgument(dataDir().toString());
        }
        return commandLine;
    }

    private CommandLine psqlCommand() {
        CommandLine psqlCommandLine = new CommandLine(commandStringWithPath(PSQL));
        psqlCommandLine.addArgument("-h").addArgument(host());
        psqlCommandLine.addArgument("-p");
        psqlCommandLine.addArgument(Integer.toString(port()));
        psqlCommandLine.addArgument("-d");
        psqlCommandLine.addArgument(dbName());
        return psqlCommandLine;
    }

    /**
     * Generates an appropriate command string for database commands. If the user specified a
     * database.bin.dir property, that is used as the path to the executable; otherwise, the
     * executable name alone is returned and the executable is assumed to be on the search path.
     */
    private String commandStringWithPath(String command) {
        Path databaseBinDir = DirectoryProperties.databaseBinDir();
        return databaseBinDir != null ? databaseBinDir.resolve(command).toString() : command;
    }
}
