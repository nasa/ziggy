package gov.nasa.ziggy.ui.util.table;

import java.awt.Component;
import java.text.MessageFormat;

import javax.swing.BorderFactory;
import javax.swing.JEditorPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.table.TableCellRenderer;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

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

    private static final long serialVersionUID = 20260710L;

    private SpreadsheetCellRenderer spreadsheetCellRenderer = new SpreadsheetCellRenderer();

    public WrappingCellRenderer() {
        setContentType("text/html");
        setEditable(false);

        setBorder(BorderFactory.createEmptyBorder(0, ZiggySwingUtils.HORIZONTAL_PADDING, 0,
            ZiggySwingUtils.HORIZONTAL_PADDING));

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

        int alignment = spreadsheetCellRenderer.getHorizontalAlignment();
        String htmlAlignment = alignment == SwingConstants.RIGHT ? "right"
            : alignment == SwingConstants.CENTER ? "center" : null;
        if (htmlAlignment != null) {
            setText(MessageFormat.format("<html><div style=\"text-align: {0}\">{1}</div></html>",
                htmlAlignment, spreadsheetCellRenderer.getText()));
        } else {
            setText(spreadsheetCellRenderer.getText());
        }

        return this;
    }
}
