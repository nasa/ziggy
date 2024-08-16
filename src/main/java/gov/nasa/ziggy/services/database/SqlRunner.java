package gov.nasa.ziggy.services.database;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Simple class that loads SQL from a file and executes it using JDBC
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class SqlRunner implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(SqlRunner.class);

    private final ConnectInfo connectInfo;
    private Connection cachedConnection;

    private SqlRunner(ConnectInfo connectInfo) {
        this.connectInfo = connectInfo;
    }

    public static SqlRunner newInstance(String driver) throws Exception {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        String url = config.getString(PropertyName.HIBERNATE_URL.property());
        String username = config.getString(PropertyName.HIBERNATE_USERNAME.property());
        String password = config.getString(PropertyName.HIBERNATE_PASSWORD.property());

        return new SqlRunner(new ConnectInfo(driver, url, username, password));
    }

    public static SqlRunner newInstance() throws Exception {
        String driver = ZiggyHibernateConfiguration.driverClassName();

        return newInstance(driver);
    }

    public Connection connection() {
        if (cachedConnection == null) {
            cachedConnection = connect();
        }
        return cachedConnection;
    }

    /**
     * (Re)connect to the database. Sets autocommit to false.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private Connection connect() {
        log.info("Connecting to: {}", connectInfo);
        try {
            Class.forName(connectInfo.getDriverName());
            cachedConnection = DriverManager.getConnection(connectInfo.getUrl(),
                connectInfo.getUsername(), connectInfo.getPassword());
            cachedConnection.setAutoCommit(false);
        } catch (Exception e) {
            throw new PipelineException("Failed to connect to: " + connectInfo.getUrl(), e);
        }

        return cachedConnection;
    }

    public void commit() throws SQLException {
        connection().commit();
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests and the sample pipeline.
        As long as it is not used by production databases, this class
        cannot result in damage from SQL injection.
        """)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void executeSql(File path, boolean continueOnError) throws SQLException {
        try {
            executeSqlStatements(loadSql(path), continueOnError);
        } catch (IOException e) {
            throw new PipelineException("Failed to load: " + path, e);
        }
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests and the sample pipeline.
        As long as it is not used by production databases, this class
        cannot result in damage from SQL injection.
        """)
    private String[] loadSql(File path) throws FileNotFoundException, IOException {
        BufferedReader fileReader = new BufferedReader(
            new InputStreamReader(new FileInputStream(path), ZiggyFileUtils.ZIGGY_CHARSET));
        StringBuilder bld = new StringBuilder((int) path.length());
        try {
            for (String line = fileReader.readLine(); line != null; line = fileReader.readLine()) {
                bld.append(line);
                bld.append(" ");
            }

            // Delete the last space so it won't be interpreted as a command.
            bld.deleteCharAt(bld.length() - 1);
        } finally {
            ZiggyFileUtils.close(fileReader);
        }

        return bld.toString().split(";");
    }

    @SuppressFBWarnings(value = "SQL_INJECTION_JDBC", justification = """
        This class is used only by unit tests and the sample pipeline.
        As long as it is not used by production databases, this class
        cannot result in damage from SQL injection.
        """)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void executeSqlStatements(String[] commands, boolean continueOnError)
        throws SQLException {
        PreparedStatement stmt = null;
        for (int line = 0; line < commands.length; line++) {
            String command = commands[line];
            if (command.trim().length() == 0) {
                continue;
            }
            try {
                try {
                    stmt = connection().prepareStatement(command);
                    stmt.execute();
                } catch (SQLException e) {
                    if (!continueOnError) {
                        throw e;
                    }
                }

                ResultSet rs = stmt.getResultSet();

                if (rs != null) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int numberOfColumns = rsmd.getColumnCount();

                    while (rs.next()) {
                        for (int colIdx = 1; colIdx <= numberOfColumns; colIdx++) {
                            System.out.print(rs.getObject(colIdx));
                            if (colIdx < numberOfColumns) {
                                System.out.print(",");
                            }
                        }
                        System.out.println();
                    }
                }
            } catch (SQLException e) {
                throw new SQLException(e.getMessage() + ": line " + line + ": " + commands[line],
                    e);
            }
        }
    }

    @Override
    public void close() {
        if (cachedConnection != null) {
            try {
                cachedConnection.close();
                cachedConnection = null;
            } catch (SQLException e) {
                log.warn("Could not close database connection: " + e.getMessage(), e);
            }
        }
    }
}
