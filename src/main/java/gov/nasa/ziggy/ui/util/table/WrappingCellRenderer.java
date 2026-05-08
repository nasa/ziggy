package gov.nasa.ziggy.ui.util.table;

import java.awt.Component;

import javax.swing.JEditorPane;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.TableCellRenderer;

/**
 * Implementation of {@link TableCellRenderer} that extends {@link JEditorPane} in order to manage
 * word wrapping when column resizing occurs. The {@link SpreadsheetCellRenderer} determines text
 * and colors. Horizontal text alignment is not supported.
 *
 * @see SpreadsheetCellRenderer
 * @author PT
 * @author Bill Wohler
 */
public class WrappingCellRenderer extends JEditorPane implements TableCellRenderer {

    private static final long serialVersionUID = 20260325L;

    private SpreadsheetCellRenderer spreadsheetCellRenderer = new SpreadsheetCellRenderer();

    public WrappingCellRenderer() {
        setContentType("text/html");
        setEditable(false);

        // The following is necessary for the setFont() method to work as expected.
        putClientProperty(JEditorPane.HONOR_DISPLAY_PROPERTIES, Boolean.TRUE);
        setFont(UIManager.getFont("TextField.font"));
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
        boolean hasFocus, int row, int column) {

        spreadsheetCellRenderer = (SpreadsheetCellRenderer) spreadsheetCellRenderer
            .getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

        setForeground(spreadsheetCellRenderer.getForeground());
        setBackground(spreadsheetCellRenderer.getBackground());
        setText(spreadsheetCellRenderer.getText());

        return this;
    }
}
