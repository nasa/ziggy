package gov.nasa.ziggy.ui.util.table;

import java.awt.Color;
import java.awt.Component;
import java.util.Date;

import javax.swing.BorderFactory;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.util.Iso8601Formatter;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Implementation of {@link TableCellRenderer} that alternates row colors, formats dates and
 * numbers, and aligns columns according to the model's {@code getColumnClass()} method, similar to
 * how {@link JTable} selects its default cell renderers.
 * <p>
 * Dates are formatted as YYYY-MM-DD hh:mm:ss.
 * <p>
 * Columns containing {@code Number}, {@code Float}, and {@code Double} are right-aligned. Columns
 * containing {@code Icon}, {@code ImageIcon}, or {@code Boolean} are centered. Everything else is
 * left-aligned. One exception is that if the value is {@link ZiggyStringUtils#NO_DATA}, which is
 * normally used to indicate no data, the cell is centered to help it stand out.
 *
 * @author Bill Wohler
 */
public class SpreadsheetCellRenderer extends DefaultTableCellRenderer {

    private static final long serialVersionUID = 20260708L;

    private Color unselectedForeground;
    private Color unselectedBackground;

    public SpreadsheetCellRenderer() {
        setBorder(BorderFactory.createEmptyBorder(0, ZiggySwingUtils.HORIZONTAL_PADDING, 0,
            ZiggySwingUtils.HORIZONTAL_PADDING));
    }

    @Override
    public void setForeground(Color c) {
        super.setForeground(c);
        unselectedForeground = c;
    }

    @Override
    public void setBackground(Color c) {
        super.setBackground(c);
        unselectedBackground = c;
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
        boolean hasFocus, int row, int column) {

        if (isSelected) {
            super.setForeground(table.getSelectionForeground());
            super.setBackground(table.getSelectionBackground());
        } else {
            super.setForeground(
                unselectedForeground != null ? unselectedForeground : table.getForeground());
            super.setBackground(
                unselectedBackground != null ? unselectedBackground : table.getBackground());
        }

        String text = valueToText(value);
        setText(text);

        Class<?> columnClass = table.getModel().getColumnClass(modelColumnIndex(table, column));
        setHorizontalAlignment(text.equals(ZiggyStringUtils.NO_DATA) ? SwingConstants.CENTER
            : Number.class.isAssignableFrom(columnClass) ? SwingConstants.RIGHT
                : SwingConstants.LEFT);

        return this;
    }

    public static String valueToText(Object value) {
        String text = "";
        if (value instanceof Date) {
            if (((Date) value).getTime() != 0) {
                text = Iso8601Formatter.javaDateTimeSansMillisLocalFormatter().format(value);
            }
        } else if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                text = value instanceof Double ? formatDouble((double) value)
                    : formatDouble((float) value);
            } else {
                text = value.toString();
            }
        } else if (value instanceof String) {
            text = StringUtils.isBlank(value.toString()) ? ZiggyStringUtils.NO_DATA
                : (String) value;
        } else if (value != null) {
            text = value.toString();
        }

        return text;
    }

    private static String formatDouble(double d) {
        if (Double.isNaN(d)) {
            return ZiggyStringUtils.NO_DATA;
        }
        return String.format("%.3f", d);
    }

    /**
     * Returns the index into the model. This is needed because an {@code ETable} can make columns
     * invisible, which makes the column indices in the view differ from the indices in the model.
     *
     * @param table the table
     * @param viewColumn the column index in the view
     * @return the associated column index in the model
     */
    private int modelColumnIndex(JTable table, int viewColumn) {
        return table.convertColumnIndexToModel(viewColumn);
    }
}
