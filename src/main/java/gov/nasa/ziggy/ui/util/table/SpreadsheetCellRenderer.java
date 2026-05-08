package gov.nasa.ziggy.ui.util.table;

import java.awt.Color;
import java.awt.Component;
import java.util.Date;

import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.util.Iso8601Formatter;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Implementation of {@link TableCellRenderer} that displays dates in a YYYY-MM-DD hh:mm:ss format,
 * right justifies numbers, and centers a dash (-), which is normally used to indicate no data, to
 * help it stand out.
 *
 * @see WrappingCellRenderer
 * @author Bill Wohler
 */
public class SpreadsheetCellRenderer extends DefaultTableCellRenderer {

    private static final long serialVersionUID = 20260327L;
    private Color unselectedForeground;
    private Color unselectedBackground;

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

        if (value instanceof Number) {
            setHorizontalAlignment(SwingConstants.RIGHT);
        } else if (text == ZiggyStringUtils.NO_DATA) {
            setHorizontalAlignment(SwingConstants.CENTER);
        } else {
            setHorizontalAlignment(SwingConstants.LEFT);
        }

        return this;
    }

    public static String valueToText(Object value) {
        String text = ZiggyStringUtils.NO_DATA;
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
            if (!StringUtils.isBlank(value.toString())) {
                text = (String) value;
            }
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
}
