package gov.nasa.ziggy.util.dispmod;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import gov.nasa.ziggy.util.StringUtils;

/**
 * Superclass for all DisplayModel classes. Contains abstract methods and print logic
 *
 * @author Todd Klaus
 */
public abstract class DisplayModel {
    private static final int COLUMN_SPACING = 2;

    private static SimpleDateFormat dateFormat = new SimpleDateFormat();

    public abstract int getRowCount();

    public abstract int getColumnCount();

    public abstract Object getValueAt(int rowIndex, int columnIndex);

    public abstract String getColumnName(int column);

    public void print(PrintStream ps) {
        print(ps, null);
    }

    public void print(PrintStream ps, String title) {
        // print title if specified
        if (title != null && title.length() > 0) {
            ps.println();
            ps.println(title);
            ps.println();
        }

        // determine column widths
        int[] columnWidths = new int[getColumnCount()];
        for (int column = 0; column < getColumnCount(); column++) {
            columnWidths[column] = Math.max(0, getColumnName(column).length() + COLUMN_SPACING);
            for (int row = 0; row < getRowCount(); row++) {
                columnWidths[column] = Math.max(columnWidths[column],
                    getValueAt(row, column).toString().length() + COLUMN_SPACING);
            }
        }

        // print column headers
        for (int column = 0; column < getColumnCount(); column++) {
            ps.print(StringUtils.pad(getColumnName(column), columnWidths[column]));
        }
        ps.println();

        for (int column = 0; column < getColumnCount(); column++) {
            for (int i = 0; i < columnWidths[column]; i++) {
                ps.print("-");
            }
        }
        ps.println();

        // print table data
        for (int row = 0; row < getRowCount(); row++) {
            for (int column = 0; column < getColumnCount(); column++) {
                ps.print(StringUtils.pad(getValueAt(row, column).toString(), columnWidths[column]));
            }
            ps.println();
        }
    }

    public static double getProcessingMillis(Date start, Date end) {
        long startMillis = start.getTime();
        long endMillis = end.getTime();

        if (endMillis == 0) {
            endMillis = System.currentTimeMillis();
        }

        if (startMillis == 0) {
            startMillis = System.currentTimeMillis();
        }

        return endMillis - startMillis;
    }

    public static double getProcessingHours(Date start, Date end) {
        double processingMillis = getProcessingMillis(start, end);
        return processingMillis / (1000.0 * 60.0 * 60.0);
    }

    protected String formatDouble(double d) {
        if (Double.isNaN(d)) {
            return "-";
        }
        return String.format("%.3f", d);
    }

    public static String formatDate(Date d) {
        if (d.getTime() == 0) {
            return "-";
        }
        return dateFormat.format(d);
    }
}
