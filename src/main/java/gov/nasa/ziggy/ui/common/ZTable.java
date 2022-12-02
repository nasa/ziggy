package gov.nasa.ziggy.ui.common;

import java.awt.Color;
import java.awt.Component;
import java.awt.font.FontRenderContext;
import java.awt.font.LineBreakMeasurer;
import java.awt.font.TextLayout;
import java.text.AttributedCharacterIterator;
import java.text.AttributedString;
import java.text.BreakIterator;
import java.util.Vector;

import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;

import org.netbeans.swing.etable.ETable;

/**
 * Subclass of the NetBeans {@link ETable} class that provides 2 additional features:
 * <ol>
 * <li>Permits the user to select alternate-row shading, which can make the table easier to read.
 * <li>Permits the user to select wrapping of cell text and resizing of rows in which cell text is
 * wrapped.
 * </ol>
 *
 * @author PT
 */
public class ZTable extends ETable {

    private static final long serialVersionUID = 20220926L;
    private static final int SHADED_ROW_INTERVAL = 2;
    private static final Color LIGHTER_GREY = new Color(240, 240, 240);

    private boolean rowShadingEnabled = false;
    private boolean textWrappingEnabled = false;
    private TableCellRenderer textWrapTableCellRenderer;
    boolean resizing = false;

    public ZTable() {
    }

    public ZTable(TableModel dm) {
        super(dm);
    }

    public ZTable(TableModel dm, TableColumnModel cm) {
        super(dm, cm);
    }

    public ZTable(TableModel dm, TableColumnModel cm, ListSelectionModel sm) {
        super(dm, cm, sm);
    }

    public ZTable(int numRows, int numColumns) {
        super(numRows, numColumns);
    }

    public ZTable(Vector<?> rowData, Vector<?> columnNames) {
        super(rowData, columnNames);
    }

    public ZTable(Object[][] rowData, Object[] columnNames) {
        super(rowData, columnNames);
    }

    public void setRowShadingEnabled(boolean rowShadingEnabled) {
        this.rowShadingEnabled = rowShadingEnabled;
    }

    public void setTextWrappingEnabled(boolean textWrappingEnabled) {
        this.textWrappingEnabled = textWrappingEnabled;
        if (textWrappingEnabled && textWrapTableCellRenderer == null) {
            textWrapTableCellRenderer = new TextWrapCellRenderer();
        }
    }

    @Override
    public Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
        Component comp = super.prepareRenderer(renderer, row, column);

        // let ETable handle the shading of the selected row(s)
        if (!isCellSelected(row, column) && rowShadingEnabled) {
            if (row % SHADED_ROW_INTERVAL == 0) {
                comp.setBackground(LIGHTER_GREY);
            } else {
                comp.setBackground(Color.white);
            }
        }

        // If the wrapping changes, the row heights may need to be adjusted. However, when
        // setRowHeight() is called, it apparently also calls prepareRenderer(). To prevent an
        // infinite loop, don't call setRowHeight() again if we're already in the midst of
        // setting the row height.
        if (textWrappingEnabled) {
            int optimalRowHeight = rowHeight(row);
            if (optimalRowHeight != getRowHeight(row) && !resizing) {
                resizing = true;
                setRowHeight(row, optimalRowHeight);
                resizing = false;
            }
        }
        return comp;
    }

    @Override
    public TableCellRenderer getCellRenderer(int row, int column) {
        if (textWrappingEnabled && getColumnClass(column) != Boolean.class) {
            return textWrapTableCellRenderer;
        } else {
            return super.getCellRenderer(row, column);
        }
    }

    private float columnWidth(int columnIndex) {
        return getColumnModel().getColumn(columnIndex).getWidth();
    }

    /**
     * Calculates the height of a table cell.
     * <p>
     * There appears to be some sort of mismatch between the way that the {@link JTextArea} wraps
     * text and the way that {@link LineBreakMeasurer} does, with the result that as the width of a
     * column decreases, the text wants to wrap before the row height wants to increase. This
     * results in an interval in which the line is wrapped but the latter part of the line is
     * unreadable because the row height remains unchanged. To address this, the
     * {@link LineBreakMeasurer} is given a wrapping width that is only 95\% as large as the column
     * width.
     * <p>
     * There is also some problem with the way that the fonts are retrieved in this code, in that
     * the fonts always have a landing of 0, resulting in the combination of ascent + descent +
     * leading being too small. Empirically, it seems that each line of text wants a leading that's
     * about equal to the descent, plus the cell wants an additional descent worth of height.
     */
    private int cellHeight(int row, int column) {
        FontRenderContext fontRendererContext = getFontMetrics(getFont()).getFontRenderContext();
        AttributedCharacterIterator characterIterator = new AttributedString(
            getValueAt(row, column).toString()).getIterator();
        LineBreakMeasurer lineMeasurer = new LineBreakMeasurer(characterIterator,
            BreakIterator.getWordInstance(), fontRendererContext);
        float formatWidth = columnWidth(column);
        lineMeasurer.setPosition(characterIterator.getBeginIndex());
        float height = 0;
        float descent = 0;
        while (lineMeasurer.getPosition() < characterIterator.getEndIndex()) {
            TextLayout layout = lineMeasurer.nextLayout(0.95F * formatWidth);
            height += layout.getAscent() + 2 * layout.getDescent();
            descent = layout.getDescent();
        }
        int intHeight = (int) (height + descent);
        return intHeight;
    }

    /**
     * Calculates the height of a row by finding the maximum height of cells within the row.
     */
    private int rowHeight(int row) {
        int height = 0;
        for (int column = 0; column < getModel().getColumnCount(); column++) {
            int cellHeight = cellHeight(row, column);
            height = Math.max(height, cellHeight);
        }
        return height;
    }

    /**
     * Implementation of {@link TableCellRenderer} that extends {@link JTextArea} in order to manage
     * word wrapping when column resizing occurs.
     *
     * @author PT
     */
    private class TextWrapCellRenderer extends JTextArea implements TableCellRenderer {

        private static final long serialVersionUID = 20220928L;

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
            setText(value.toString());
            setWrapStyleWord(true);
            setLineWrap(true);

            return this;
        }

    }

}
