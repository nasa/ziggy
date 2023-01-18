package gov.nasa.ziggy.services.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.RegexEditor;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * DbDumper - writes the contents of selected tables to a file
 *
 * @author Jay Gunter
 */
public class DbDumper {
    private static final Logger log = LoggerFactory.getLogger(DbDumper.class);

    private static final String FIELD_SEPARATOR = ",";
    private static final String COLUMN_EXPRESSION_SEPARATOR = "?";
    private static final String[] ALL_TABLES = { "%" };
    private static final String[] NO_COLUMNS = {};

    private final ConnectInfo connectInfo;
    private Connection connection;
    private DatabaseMetaData dmd;
    private String[] tablePrefixes = ALL_TABLES;
    private String[] undesiredColumns = NO_COLUMNS;

    private final Map<String, Pattern[]> patternsByColumn = new HashMap<>();

    private String exFilename;

    private String acFilename;

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = """
        User must be able to specify location of files for use in writing
        out database tables. The db dump files that are read in are then
        compared to one another and never used for any purpose that exposes
        execution to malicious contents of the files.
        """)
    public static void main(String[] args) throws Exception {

        File expectedFile = null;
        File actualFile = null;

        int i;
        for (i = 0; i < args.length; i++) {
            if (args[i].equals("x")) {
                args[i] = "";
            }
            log.info(String.format("args[%s]= " + args[i]));
        }
        i = 0;

        ConnectInfo connectInfo = new ConnectInfo("org.hsqldb.jdbcDriver", args[i++], "sa", "");

        if (args[i].equals("v")) {
            expectedFile = new File(args[++i]);
            actualFile = new File(args[++i]);
            DbDumper dbDumper = new DbDumper();
            dbDumper.validateDatabase(expectedFile, actualFile);
        } else {
            DbDumper dbDumper = new DbDumper(connectInfo);
            dbDumper.setTablePrefixes(nullIfNull(args[i++]));
            dbDumper.setUndesiredColumnNames(nullIfNull(args[i++]));
            dbDumper.writeDesiredTableData(new File(args[i++]));
        }
    }

    /**
     * If passed "null" returns null, otherwise returns String passed. This allows null to be
     * expressed when running DbDumper from command line.
     *
     * @param s
     * @return
     */
    private static String nullIfNull(String s) {
        return s.equals("null") ? null : s;
    }

    /**
     * Constructor used when dumping all tables and columns, or when separate calls to
     * setTablePrefixes and setExcludeColumns makes sense.
     *
     * @param connectInfo
     * @throws Exception
     */
    public DbDumper(ConnectInfo connectInfo) throws Exception {
        this.connectInfo = connectInfo;
        init();
    }

    /**
     * Constructor used when table prefixes and excluded columns are known up front.
     *
     * @param connectInfo
     * @param prefixes
     * @param excludeColumns
     * @throws Exception
     */
    public DbDumper(ConnectInfo connectInfo, String[] prefixes, String[] excludeColumns)
        throws Exception {
        this.connectInfo = connectInfo;
        setTablePrefixes(prefixes);
        setUndesiredColumnNames(excludeColumns);
        init();
    }

    public DbDumper() throws Exception {
        this(new ConnectInfo());
    }

    /**
     * Get database connection and get database metadata.
     *
     * @throws Exception
     */
    private void init() throws Exception {
        connect();
        dmd = connection.getMetaData();
    }

    public void setTablePrefixes(String csv) {
        setTablePrefixes(csv.split(","));
    }

    public void setTablePrefixes(String[] prefixes) {
        tablePrefixes = prefixes;
    }

    public void setUndesiredColumnNames(String csv) {
        setUndesiredColumnNames(csv.split(","));
    }

    public void setUndesiredColumnNames(String[] names) {
        undesiredColumns = names;
    }

    /**
     * Reads database metadata to find tables whose names match table prefixes, then determines
     * which columns should be selected from the database, and performs those queries to generate
     * the output file.
     *
     * @param outFile
     * @throws Exception
     */
    public void writeDesiredTableData(File outFile) throws Exception {
        log.info("Writing " + outFile.getAbsolutePath());
        if (tablePrefixes.equals(ALL_TABLES)) {
            log.info("all tables will be written");
        }
        if (undesiredColumns.equals(NO_COLUMNS)) {
            log.info("all columns will be written");
        }

        try (BufferedWriter output = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outFile), FileUtil.ZIGGY_CHARSET))) {
            updatePatternsByTable();

            // use database metadata to find tables with names matching prefixes
            List<String> tableNames = new ArrayList<>();
            Map<String, List<String>> selects = new HashMap<>();
            for (String element : tablePrefixes) {
                log.debug("table prefix #1 = " + element);
                try (ResultSet tablesRS = dmd.getTables(null, null, element.toUpperCase() + "%",
                    new String[] { "TABLE" })) {
                    log.debug("tablesRS.getRow()=" + tablesRS.getRow());
                    while (tablesRS.next()) {
                        String tableName = tablesRS.getString("TABLE_NAME");
                        log.info("table name: " + tableName);
                        tableNames.add(tableName);

                        // build list of desired (non-excluded) column names
                        try (ResultSet columnsRS = dmd.getColumns(null, null,
                            tableName.toUpperCase(), null)) {
                            log.debug("columnsRS.getRow()=" + columnsRS.getRow());
                            String tableColumns = "";
                            String comma = "";
                            List<String> columnNames = new ArrayList<>();
                            while (columnsRS.next()) {
                                String colName = columnsRS.getString("COLUMN_NAME");
                                log.debug("colName = " + colName);
                                String colSize = columnsRS.getString("COLUMN_SIZE");
                                log.debug("colSize = " + colSize);

                                String fullColName = tableName + "." + colName;
                                int j = 0;
                                log.debug("undesiredColumns.length=" + undesiredColumns.length);
                                for (; j < undesiredColumns.length; j++) {
                                    String column = undesiredColumns[j];
                                    log.debug(String.format("undesiredColumns[%s]=", column));
                                    if (column.indexOf(COLUMN_EXPRESSION_SEPARATOR) == -1
                                        && fullColName.equalsIgnoreCase(column)) {
                                        break;
                                    }
                                }
                                if (j == undesiredColumns.length) {
                                    tableColumns += comma + colName;
                                    comma = ", ";
                                    columnNames.add(colName);
                                }
                            }

                            if (0 == tableColumns.length()) {
                                tableColumns = "*";
                            }
                            if (0 == columnNames.size()) {
                                log.error("all columns filtered away for table " + tableName);
                            } else {
                                // sort the column names to ensure output
                                // comparability
                                Collections.sort(columnNames);
                                selects.put(tableName, columnNames);
                            }
                        }
                    }
                }
            }

            // sort table names to ensure output comparability
            Collections.sort(tableNames);

            // build and run a SELECT statement for each table
            for (String tableName : tableNames) {
                log.debug("table name: " + tableName);
                List<String> columnNames = selects.get(tableName);
                if (null == columnNames || 0 == columnNames.size()) {
                    log.warn("no columns selected from table " + tableName);
                    continue;
                }
                String comma = "";
                String select = "SELECT ";
                for (String columnName : columnNames) {
                    select += comma + columnName;
                    comma = FIELD_SEPARATOR;
                }
                select += " FROM " + tableName;
                try (Statement stmt = connection.createStatement()) {
                    log.info("select statement: " + select);
                    output.write(select + "\n");
                    try (ResultSet rs = stmt.executeQuery(select)) {
                        log.debug("rs.getRow() = " + rs.getRow());
                        List<String> tableLines = new ArrayList<>(rs.getFetchSize());
                        while (rs.next()) {
                            String outLine = "";
                            comma = "";
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int ncols = rsmd.getColumnCount();
                            for (int c = 1; c <= ncols; c++) {
                                outLine += comma + rs.getString(c);
                                comma = FIELD_SEPARATOR;
                            }
                            log.debug("row data = " + outLine);
                            tableLines.add(outLine);
                        }
                        Collections.sort(tableLines);
                        for (String line : tableLines) {
                            output.write(line + "\n");
                        }
                    }
                }
            }
        }
    } // writeDesiredTableData

    /**
     * Compares two files written by writeDesiredTableData.
     */
    public void validateDatabase(File expectedData, File actualData) throws Exception {
        exFilename = expectedData.getAbsolutePath();
        acFilename = actualData.getAbsolutePath();
        log.info("actual=" + acFilename + ", expected=" + exFilename);

        DumpFile ac;
        try {
            ac = new DumpFile(actualData);
        } catch (Exception e) {
            throw exception("Actual data file missing.");
        }
        DumpFile ex;
        try {
            ex = new DumpFile(expectedData);
        } catch (Exception e) {
            throw exception("Expected data file was missing; " + "consider copying "
                + actualData.getAbsolutePath() + " to " + expectedData.getAbsolutePath()
                + " and rerunning this test to validate against the new expected data file.");
        }

        updatePatternsByTable();

        String[] exColumnValues = null;
        String[] acColumnValues = null;
        List<String> diffSelects = new ArrayList<>();
        Set<String> tableNames = new TreeSet<>();
        Set<String> diffColumns = new HashSet<>();
        Map<String, Integer> diffCount = new HashMap<>();
        int diffRows = 0;
        int totalDiffRows = 0;
        int totalDiffValues = 0;
        String allDiffs = "";
        boolean skipToNextSelect = false;

        while (true) {
            ex.read();
            ac.read();
            if (ex.eof && ac.eof) {
                break;
            }

            tableNames.add(ex.tableName);
            tableNames.add(ac.tableName);
            if (ex.select && ac.select) {
                diffRows = 0;
                int tableNameComparison = ex.tableName.compareTo(ac.tableName);
                skipToNextSelect = ex.waiting = ac.waiting = false;
                if (tableNameComparison == 0) {
                    // compare columns in SELECT statements
                    if (!ex.line.equals(ac.line)) {
                        diffSelects.add("Columns differ for table " + ex.tableName
                            + ":\nexpected columns:\n" + ex.line + " (line #)" + ex.lines
                            + ":\nactual columns:\n" + ac.line + " (line #)" + ac.lines);
                        skipToNextSelect = true;
                    }
                } else if (tableNameComparison < 0) {
                    // ex.tableName is alphabetically before current
                    // ac.tableName,
                    // so ex has a table that ac does not.
                    // Loop through the data rows of the current ex table
                    // and wait to see if ac has the next ex table.
                    ac.waiting = true;
                } else { // tableNameComparison > 0
                    ex.waiting = true;
                }
                continue;
            }

            if (ex.select && !ac.select) {
                // ac has more rows in current table than ex.
                // Just wait for next table (select statement).
                // Differing row counts appears in the report summation.
                if (!ac.eof) {
                    ex.waiting = true;
                }
                continue;
            }
            if (!ex.select && ac.select) {
                if (!ex.eof) {
                    ac.waiting = true;
                }
                continue;
            }

            if (!ex.waiting && !ex.eof && !ac.waiting && !ac.eof && !skipToNextSelect) {
                // compare data rows
                exColumnValues = ex.line.split(FIELD_SEPARATOR);
                acColumnValues = ac.line.split(FIELD_SEPARATOR);
                // need columns
                if (exColumnValues.length != acColumnValues.length) {
                    // If the expected vs actual selects matched
                    // but we get a different # of columns in the data
                    // we bail out. This should never happen.
                    throw exception("INTERNAL ERROR IN DBDUMPER, UNEQUAL NUMBER OF COLUMN VALUES:\n"
                        + "expected: table=" + ex.tableName + ", #rows=" + ex.rowsInTable + " line="
                        + ex.line + "\nactual: table=" + ac.tableName + ", #rows=" + ac.rowsInTable
                        + " line=" + ac.line);
                }
                String diffs = ""; // diffs for this row
                boolean diffsInRow = false;
                for (int i = 0; i < ex.columnNames.length; i++) {
                    String tabCol = ex.tableName + "." + ex.columnNames[i];
                    Pattern[] patterns = patternsByColumn.get(tabCol);
                    if ((patterns == null || patterns.length == 0)
                        && !exColumnValues[i].equals(acColumnValues[i])
                        || patterns != null && patterns.length > 0 && !RegexEditor
                            .stringEquals(exColumnValues[i], acColumnValues[i], patterns)) {
                        diffsInRow = true;
                        totalDiffValues++;
                        // To make error reporting concise,
                        // we only report the first difference for a particular
                        // column.
                        if (!diffColumns.contains(tabCol)) {
                            diffColumns.add(tabCol);
                            diffs += "\n   column #" + i + " (column name=" + ex.columnNames[i]
                                + "): \n      expected value = '" + exColumnValues[i] + "'"
                                + "\n      actual   value = '" + acColumnValues[i] + "'";
                        }
                    }
                }
                if (diffsInRow) {
                    diffRows++;
                    totalDiffRows++;
                }
                diffCount.put(ex.tableName, diffRows);
                if (0 != diffs.length()) {
                    allDiffs += "\nTable " + ex.tableName
                        + ", first conflicting row: expected row #" + ex.rowsInTable + " (line #"
                        + ex.lines + ") " + ", actual row #" + ac.rowsInTable + " (line #"
                        + ac.lines + ") " + ": differences:" + diffs;
                }
            }
        }

        // gather differences into a single message to throw in an exception
        String msg = "";
        for (String s : diffSelects) {
            msg += "\n" + s;
        }
        for (String table : tableNames) {
            StringBuilder s = new StringBuilder();
            int exRows = -1;
            int acRows = -1;
            s.append("\nTable ").append(table);
            if (ex.tableNames.contains(table)) {
                exRows = ex.rowCount.get(table);
                s.append(": expected has ").append(exRows).append(" rows");
            } else {
                s.append(" (table missing in expected)");
            }
            if (ac.tableNames.contains(table)) {
                acRows = ac.rowCount.get(table);
                s.append(": actual has ").append(acRows).append(" rows");
            } else {
                s.append(" (table missing in actual)");
            }
            Integer n = diffCount.get(table);
            int diffs = 0;
            if (null != n) {
                diffs = diffCount.get(table);
            }
            if (diffs > 0) {
                s.append(": differences found in ").append(diffs).append(" rows.");
            }
            if (exRows != acRows || diffs > 0) {
                msg += s.toString();
            }
        }
        if (0 != allDiffs.length()) {
            msg += "\n" + allDiffs;
            if (totalDiffRows > 0) {
                msg += "\n\nTotal number of differing rows for all tables = "
                    + String.format("%,d", totalDiffRows);
                msg += "\nTotal number of differing values for all tables = "
                    + String.format("%,d", totalDiffValues);
            }
            String comma = "";
            StringBuilder potentialExcludeColumns = new StringBuilder();
            List<String> tabCols = new ArrayList<>(diffColumns);
            Collections.sort(tabCols);
            for (String tabCol : tabCols) {
                potentialExcludeColumns.append(comma).append("\"").append(tabCol).append("\"");
                comma = ",\n";
            }
            msg += "\n\nColumns to consider for exclusion:\n" + potentialExcludeColumns.toString();
        }
        if (0 != msg.length()) {
            throw exception(msg);
        }
    }

    private Exception exception(String msg) throws Exception {
        String fullMsg = "Validation failed:  data files not identical:" + "\nExpected data file = "
            + exFilename + "\nActual   data file = " + acFilename + "\nDifferences:" + msg;
        log.error(fullMsg);
        return new Exception(fullMsg);
    }

    /**
     * (Re)connect to the database.
     *
     * @throws PipelineException
     */
    public void connect() throws PipelineException {
        try {
            Class.forName(connectInfo.getDriverName());
            connection = DriverManager.getConnection(connectInfo.getUrl(),
                connectInfo.getUsername(), connectInfo.getPassword());
        } catch (Exception e) {
            throw new PipelineException("failed to connect to: " + connectInfo.getUrl(), e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    private void updatePatternsByTable() {
        patternsByColumn.clear();
        if (undesiredColumns != null) {
            for (String column : undesiredColumns) {
                int index = column.indexOf(COLUMN_EXPRESSION_SEPARATOR);
                if (index != -1) {
                    String columnName = column.substring(0, index++);
                    String excludeRegex = column.substring(index);
                    String regexWithNonCaptureGroup = "(.*)(?:" + excludeRegex + ")(.*)";
                    Pattern pattern = Pattern.compile(regexWithNonCaptureGroup);
                    patternsByColumn.put(columnName, new Pattern[] { pattern });
                }
            }
        }
    }

    private static final class DumpFile {
        private BufferedReader in = null;
        private boolean eof = false;
        private boolean select = false;
        private boolean waiting = false;
        private String line = "";
        private final Set<String> tableNames = new TreeSet<>();
        private String tableName = "";
        private String[] columnNames = {};
        private int lines = 0;
        private int rowsInTable = 0;
        private final Map<String, Integer> rowCount = new HashMap<>();

        private DumpFile(File file) throws FileNotFoundException {
            in = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), FileUtil.ZIGGY_CHARSET));
        }

        public String read() {
            if (waiting) {
                return "";
            }
            try {
                line = in.readLine();
                if (null == line) {
                    eof = true;
                    line = "";
                }
            } catch (Exception e) {
                eof = true;
                line = "";
            }
            if (eof) {
                return line;
            }
            lines++;
            // if (tableName.length() > 0) {
            // rowCount.put(tableName, rowsInTable);
            // }
            select = false;
            if (line.startsWith("SELECT ")) {
                select = true;
                int i = line.lastIndexOf(" FROM ");
                // skip the 6 characters of " FROM "
                tableName = line.substring(i + 6);
                tableNames.add(tableName);
                columnNames = line.substring(7, i).split(FIELD_SEPARATOR);
                rowsInTable = 0;
                // rowCount.put(tableName, rowsInTable);
            } else {
                // log.info("line=" + line);
                if (tableName.length() <= 0) {
                    log.error("INTERNAL ERROR: EMPTY TABLE NAME");
                }
                rowsInTable++;
            }
            rowCount.put(tableName, rowsInTable);
            // log.info(inFile.getName() + ": tableName="+tableName+",
            // rowsInTable="+rowsInTable+": line="+line);
            return line;
        }
    }
}
