package gov.nasa.ziggy.ui.util.table;

import java.awt.Color;
import java.awt.Component;
import java.util.Date;

import javax.swing.JEditorPane;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.TableCellRenderer;

import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Implementation of {@link TableCellRenderer} that extends {@link JEditorPane} in order to manage
 * word wrapping when column resizing occurs.
 *
 * @author PT
 */
public class WrappingCellRenderer extends JEditorPane implements TableCellRenderer {

    private static final long serialVersionUID = 20230511L;

    private Color unselectedForeground;
    private Color unselectedBackground;

    public WrappingCellRenderer() {
        setContentType("text/html");
        setEditable(false);

        // The following is necessary for the setFont() method to work as expected.
        putClientProperty(JEditorPane.HONOR_DISPLAY_PROPERTIES, Boolean.TRUE);
        setFont(UIManager.getFont("TextField.font"));
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

        if (value instanceof Date) {
            setText(Iso8601Formatter.javaDateTimeSansMillisLocalFormatter().format(value));
        } else {
            setText(value.toString());
        }

        return this;
    }
}
