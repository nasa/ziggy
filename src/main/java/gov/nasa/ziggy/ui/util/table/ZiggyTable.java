package gov.nasa.ziggy.ui.util.table;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.MouseListener;
import java.awt.font.FontRenderContext;
import java.awt.font.LineBreakMeasurer;
import java.awt.font.TextLayout;
import java.text.AttributedCharacterIterator;
import java.text.AttributedString;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;
import javax.swing.tree.AbstractLayoutCache;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.outline.DefaultOutlineModel;
import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.OutlineModel;
import org.netbeans.swing.outline.RowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.HasGroup;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.ConsoleDatabaseModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModelRegistry;
import gov.nasa.ziggy.ui.util.models.TableModelContentClass;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * The {@link ZiggyTable} provides a combination of a two-dimensional table of cells and the data
 * model(s) that support the table.
 * <p>
 * The underlying table representation is the {@link ETable}, an extension of {@link JTable}. The
 * {@link #table} field will be one of two concrete classes:
 * <ol>
 * <li>An {@link ETable}, which has a look and feel similar to {@link JTable} with some extensions.
 * <li>An {@link Outline}, which can represent its contents as a tree (i.e., there is an organizing
 * hierarchy).
 * </ol>
 * The selection of the {@link #table} representation is performed automatically: if the
 * {@link ZiggyTable} is constructed with a {@link TableModel}, then an {@link ETable} will be used;
 * if constructed with a {@link RowModel} and a {@link TreeModel}, then an {@link Outline} will be
 * used.
 * <p>
 * In addition to the typical table functionality, {@link ZiggyTable} provides the following
 * features:
 * <ol>
 * <li>Users can optionally display the table with alternating rows switching from white to light
 * grey backgrounds.
 * <li>Users can optionally configure tables such that text cells will wrap content to more than one
 * line and row heights will be adjusted automatically (note that this is not supported for
 * {@link Outline}-based instances).
 * <li>Users can trigger a reload of the underlying model from the database.
 * <li>Users can obtain the content of one or more rows from the underlying model.
 * </ol>
 *
 * @author PT
 * @param <T> Class of objects stored in the table model. The table is a table of instances of T.
 */
public class ZiggyTable<T> {

    private static final Logger log = LoggerFactory.getLogger(ZiggyTable.class);

    static private final int SHADED_ROW_INTERVAL = 2;
    static private final Color LIGHTER_GREY = new Color(240, 240, 240);

    /**
     * A regular expression for matching on whitespace and punctuation. This is used by
     * {@link #cellHeight(int, int)} to determine word boundaries. The punctuation portion of the
     * regexp should match the one that the {@link JEditorPane} uses to initiate line breaks,
     * whatever that is. It's not the Java Punct pattern, since that pattern contains / and - and we
     * see that the {@link JEditorPane} does not break on these characters. Thus, consider amending
     * the set of punctuation characters if {@link #cellHeight(int, int)} behaves strangely on
     * certain characters.
     */
    static private final Pattern WHITESPACE_PUNCTUATION = Pattern.compile("(\\s|[,.]|$)");

    private ETable table;
    private boolean wrapText;
    private boolean shadeRows = true;
    private boolean resizing;
    private WrappingCellRenderer wrappingCellRenderer = new WrappingCellRenderer();
    private DateCellRenderer dateCellRenderer = new DateCellRenderer();
    private HashMap<String, Boolean> expansionState;
    private boolean defaultGroupExpansionState;
    private TableModel tableModel;
    private ZiggyTreeModel<?> treeModel;
    private final Class<T> modelContentsClass;
    private OutlineModel outlineModel;

    /**
     * Constructor for use when an {@link ETable} is desired. An {@link ETable} supports table cells
     * that wrap text, but doesn't support a tree organization for the table contents.
     */
    @SuppressWarnings("unchecked")
    public ZiggyTable(TableModel tableModel) {
        checkArgument(tableModel instanceof TableModelContentClass,
            "ZiggyTable model must implement TableModelContentClass");
        modelContentsClass = ((TableModelContentClass<T>) tableModel).tableModelContentClass();
        this.tableModel = tableModel;
        table = new ZiggyETable();
        table.setModel(tableModel);
        wrapText = true;
        setPreferredViewportSizeFromModel();
    }

    /**
     * Constructor for use when an {@link Outline} is desired. An {@link Outline} supports a tree
     * organization for the table contents, but doesn't support table cells that wrap text.
     */
    @SuppressWarnings("unchecked")
    public ZiggyTable(RowModel rowModel, ZiggyTreeModel<?> treeModel, String nodesColumnLabel) {
        checkArgument(rowModel instanceof TableModelContentClass,
            "ZiggyTable rowModel must implement TableModelContentClass");
        modelContentsClass = ((TableModelContentClass<T>) rowModel).tableModelContentClass();
        checkArgument(HasGroup.class.isAssignableFrom(modelContentsClass),
            "ZiggyTable model content class must implement HasGroup");
        this.treeModel = treeModel;
        table = new ZiggyOutline();
        outlineModel = DefaultOutlineModel.createOutlineModel(treeModel, rowModel, false,
            nodesColumnLabel);
        ((Outline) table).setModel(outlineModel);
        DefaultMutableTreeNode defaultGroupNode = treeModel.getDefaultGroupNode();
        if (defaultGroupNode != null) {
            ((Outline) table).expandPath(new TreePath(defaultGroupNode.getPath()));
        }
        wrapText = false;
    }

    /**
     * Enables or disables row shading.
     */
    public void setShadeRows(boolean shadeRows) {
        this.shadeRows = shadeRows;
    }

    /**
     * Enables or disables cell text wrapping. Cell text wrapping is not supported for
     * {@link ZiggyTable} instances that contain an {@link Outline}, so those instances will issue a
     * warning if the caller attempts to enable cell text wrapping.
     */
    public void setWrapText(boolean wrapText) {
        if (table instanceof Outline || wrapText) {
            log.warn("Text wrapping not supported for ZiggyTables in Outline mode");
            return;
        }
        this.wrapText = wrapText;
    }

    /**
     * Returns the actual {@link ETable} field. Used when a Swing element takes an argument that is
     * a Swing component (which {@link ZiggyTable} is not).
     *
     * @return
     */
    public ETable getTable() {
        return table;
    }

    public void registerModel() {
        checkState(tableModel instanceof ConsoleDatabaseModel,
            "table model must implement ConsoleDatabaseModel");
        DatabaseModelRegistry.registerModel((ConsoleDatabaseModel) tableModel);
    }

    /**
     * Loads the model contents from the database.
     */
    @SuppressWarnings("unchecked")
    public void loadFromDatabase() {
        if (table instanceof Outline) {
            recordExpansionState(treeModel.getGroupNodes(), treeModel.getDefaultGroupNode());
            treeModel.loadFromDatabase();
            applyExpansionState(treeModel.getGroupNodes(), treeModel.getDefaultGroupNode());
        } else {
            checkState(tableModel instanceof AbstractDatabaseModel,
                "table model must be AbstractDatabaseModel");
            ((AbstractDatabaseModel<T>) tableModel).loadFromDatabase();
        }
    }

    /**
     * Captures the current expansion state of the node tree that supports an {@link Outline}
     * instance in the table field.
     */
    private void recordExpansionState(Map<String, DefaultMutableTreeNode> groupNodes,
        DefaultMutableTreeNode defaultGroupNode) {
        checkState(table instanceof Outline, "table must be an Outline");
        expansionState = new HashMap<>();
        Outline outline = (Outline) table;
        AbstractLayoutCache layoutCache = outline.getLayoutCache();

        for (String groupName : groupNodes.keySet()) {
            DefaultMutableTreeNode node = groupNodes.get(groupName);
            boolean isExpanded = layoutCache.isExpanded(new TreePath(node.getPath()));

            expansionState.put(groupName, isExpanded);
        }

        defaultGroupExpansionState = layoutCache
            .isExpanded(new TreePath(defaultGroupNode.getPath()));
    }

    /**
     * Applies a cached expansion state to the node tree that supports an {@link Outline} instance
     * in the table field.
     */
    private void applyExpansionState(Map<String, DefaultMutableTreeNode> groupNodes,
        DefaultMutableTreeNode defaultGroupNode) {
        checkState(table instanceof Outline, "table must be an Outline");
        Outline outline = (Outline) table;

        for (String groupName : expansionState.keySet()) {
            DefaultMutableTreeNode node = groupNodes.get(groupName);
            if (node != null) {
                boolean shouldExpand = expansionState.get(groupName);
                if (shouldExpand) {
                    outline.expandPath(new TreePath(node.getPath()));
                } else {
                    outline.collapsePath(new TreePath(node.getPath()));
                }
            }
        }

        if (defaultGroupExpansionState) {
            outline.expandPath(new TreePath(defaultGroupNode.getPath()));
        } else {
            outline.collapsePath(new TreePath(defaultGroupNode.getPath()));
        }
    }

    /**
     * Expands all the levels of the {@link Outline} tree.
     */
    public void expandAll() {
        checkState(table instanceof Outline, "table must be an Outline");
        DefaultMutableTreeNode rootNode = treeModel.getRootNode();
        int numKids = rootNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootNode.getChildAt(kidIndex);
            ((Outline) table).expandPath(new TreePath(kid.getPath()));
        }
    }

    /**
     * Collapses all the levels of the {@link Outline} tree.
     */
    public void collapseAll() {
        checkState(table instanceof Outline, "table must be an Outline");
        DefaultMutableTreeNode rootNode = treeModel.getRootNode();
        int numKids = rootNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootNode.getChildAt(kidIndex);
            ((Outline) table).collapsePath(new TreePath(kid.getPath()));
        }
    }

    /**
     * Sets the preferred width of a column.
     */
    public void setPreferredColumnWidth(int columnIndex, int width) {
        TableColumnModel columnModel = table.getColumnModel();
        checkArgument(columnIndex < columnModel.getColumnCount(), "index value of " + columnIndex
            + " not valid for table with " + columnModel.getColumnCount() + " columns");
        columnModel.getColumn(columnIndex).setPreferredWidth(width);

        // Update the preferred viewport size with this new information.
        setPreferredViewportSizeFromModel();
    }

    private void setPreferredViewportSizeFromModel() {
        // The default column width is sized to fit the data. This code ensures that the heading,
        // with 5 pixels of padding on either side, fits as well.
        JLabel component = new JLabel();
        TableColumnModel columnModel = table.getColumnModel();
        int headerPadding = 10;
        for (int column = 0; column < columnModel.getColumnCount(); column++) {
            TableColumn tableColumn = columnModel.getColumn(column);
            if (tableColumn.getHeaderValue() instanceof String) {
                tableColumn.setMinWidth(
                    ZiggySwingUtils.textWidth(component, (String) tableColumn.getHeaderValue())
                        + headerPadding);
            }
        }

        // Set the preferred viewport size to the preferred size, which is based upon the preferred
        // height and width of the table content. Otherwise, an enclosing scroll pane will put the
        // table in a small square.
        int additionalPixelsPerRowToHideScrollbar = 4;
        Dimension preferredSize = getTable().getPreferredSize();
        getTable().setPreferredScrollableViewportSize(new Dimension(preferredSize.width,
            preferredSize.height + additionalPixelsPerRowToHideScrollbar * table.getRowCount()));
    }

    /**
     * Adds a {@link MouseListener} to the table.
     */
    public void addMouseListener(MouseListener listener) {
        table.addMouseListener(listener);
    }

    /**
     * Returns the table row at a given {@link Point}.
     */
    public int rowAtPoint(Point point) {
        return table.rowAtPoint(point);
    }

    /**
     * Returns the table column at a given {@link Point}.
     */
    public int columnAtPoint(Point point) {
        return table.columnAtPoint(point);
    }

    /**
     * Returns the contents of the {@link ZiggyTable}'s model for a given view row. This operation
     * is valid for all {link ZiggyTable} instances with an {@link Outline}, and all {@link ETable}
     * based instances for which the table model is a subclass of {@link AbstractDatabaseModel}.
     * <p>
     * Note that the argument is assumed to be the view index, i.e., the index into the table based
     * on its current sorting. This is converted to the index into the table model which is then
     * used to obtain the model content. The key thing to remember is that when the view is sorted,
     * the model is not.
     */
    public T getContentAtViewRow(int viewIndex) {
        int modelIndex = table.convertRowIndexToModel(viewIndex);
        if (modelIndex == -1) {
            return null;
        }
        return table instanceof Outline ? getOutlineContentAtModelRow(viewIndex)
            : getTableContentAtModelRow(modelIndex);
    }

    public void fireTableDataChanged() {
        if (tableModel instanceof AbstractTableModel) {
            ((AbstractTableModel) tableModel).fireTableDataChanged();
        }
    }

    /**
     * Returns row content from an instance of {@link Outline}.
     */
    private T getOutlineContentAtModelRow(int modelIndex) {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) outlineModel.getValueAt(modelIndex,
            0);
        Object userObject = node.getUserObject();
        if (modelContentsClass.isInstance(userObject)) {
            return modelContentsClass.cast(userObject);
        }
        return null;
    }

    /**
     * Returns row content from an instance of {@link ETable} if the underlying model is a subclass
     * of {@link AbstractDatabaseModel}.
     */
    private T getTableContentAtModelRow(int viewIndex) {
        checkState(tableModel instanceof AbstractZiggyTableModel,
            "Table model must extend AbstractZiggyTableModel");
        @SuppressWarnings("unchecked")
        AbstractZiggyTableModel<T> abstractModel = (AbstractZiggyTableModel<T>) tableModel;
        return modelContentsClass.cast(abstractModel.getContentAtRow(viewIndex));
    }

    /**
     * Returns row content from multiple rows of the table.
     */
    public List<T> getContentAtSelectedRows() {
        List<T> selectedContent = new LinkedList<>();
        int[] selectedRows = table.getSelectedRows();
        for (int selectedRow : selectedRows) {
            if (selectedRow >= 0) {
                T content = getContentAtViewRow(table.convertRowIndexToModel(selectedRow));
                if (content != null) {
                    selectedContent.add(content);
                }
            }
        }
        return selectedContent;
    }

    public int getSelectedRow() {
        return table.getSelectedRow();
    }

    public int convertRowIndexToModel(int row) {
        return table.convertRowIndexToModel(row);
    }

    /**
     * Implements the {@link JTable#prepareRenderer(TableCellRenderer, int, int)} for use with row
     * shading and cell wrapping.
     *
     * @param jTableRenderer {@link TableCellRenderer} component produced by the superclass
     * ({@link JTable}) version of {@link JTable#prepareRenderer(TableCellRenderer, int, int)}.
     */
    private Component prepareRenderer(int row, int column, Component jTableRenderer) {

        // let ETable handle the shading of the selected row(s)
        if (!table.isCellSelected(row, column) && shadeRows) {
            if (row % SHADED_ROW_INTERVAL == 0) {
                jTableRenderer.setBackground(LIGHTER_GREY);
            } else {
                jTableRenderer.setBackground(Color.white);
            }
        }

        // If the wrapping changes, the row heights may need to be adjusted. However, when
        // setRowHeight() is called, it apparently also calls prepareRenderer(). To prevent an
        // infinite loop, don't call setRowHeight() again if we're already in the midst of
        // setting the row height.
        if (wrapText) {
            int optimalRowHeight = rowHeight(row);
            if (optimalRowHeight != table.getRowHeight(row) && !resizing) {
                resizing = true;
                table.setRowHeight(row, optimalRowHeight);
                resizing = false;
            }
        }
        return jTableRenderer;
    }

    /**
     * Calculates the height of a row by finding the maximum height of cells within the row.
     */
    private int rowHeight(int row) {
        int height = 0;
        for (int column = 0; column < table.getModel().getColumnCount(); column++) {
            int cellHeight = isCellContentWrappable(row, column) ? cellHeight(row, column) : 0;
            height = Math.max(height, cellHeight);
        }
        return height != 0 ? height : table.getRowHeight(row);
    }

    /**
     * Calculates the height of a table cell.
     * <p>
     * There appeared to be some sort of mismatch between the way that the {@link JTextArea} or
     * {@link JEditorPane} wraps text and the way that {@link LineBreakMeasurer} does, with the
     * result that as the width of a column decreases, the text wants to wrap before the row height
     * wants to increase. This results in an interval in which the line is wrapped but the latter
     * part of the line is unreadable because the row height remains unchanged. To address this, the
     * {@link LineBreakMeasurer} was given a wrapping width that was only 98\% as large as the
     * column width.
     * <p>
     * There is also some problem with the way that the fonts are retrieved in this code, in that
     * the fonts always have a landing of 0, resulting in the combination of ascent + descent +
     * leading being too small. Empirically, it seems that each line of text wants a leading that's
     * about equal to the descent, plus the cell wants an additional descent worth of height.
     */
    private int cellHeight(int row, int column) {
        FontRenderContext fontRendererContext = table.getFontMetrics(table.getFont())
            .getFontRenderContext();
        Object tableValue = table.getValueAt(row, column);
        String text = tableValue != null && !StringUtils.isEmpty(tableValue.toString())
            ? tableValue.toString()
            : "Default"; // provide some reasonable string to operate upon
        text = Jsoup.parse(text).text(); // strip HTML as it doesn't contribute to length

        // Since JEditorPane doesn't support CSS3's {word-wrap: break-word}, long words (like
        // filenames) do not break. Therefore, the truncated portion of the word should not
        // contribute to the cell height. To solve this, create a list of word boundary indices and
        // only increase the height of the cell as long as the new position is greater than the last
        // word boundary.
        List<Integer> boundaries = wordBoundaries(text);
        int boundaryIndex = 0;

        AttributedCharacterIterator characterIterator = new AttributedString(text).getIterator();
        LineBreakMeasurer lineMeasurer = new LineBreakMeasurer(characterIterator,
            BreakIterator.getWordInstance(), fontRendererContext);
        lineMeasurer.setPosition(characterIterator.getBeginIndex());

        float formatWidth = table.getColumnModel().getColumn(column).getWidth();
        float height = 0;
        float descent = 0;
        int position = 0;
        while (position < text.length()) {
            TextLayout layout = lineMeasurer.nextLayout(0.98F * formatWidth);
            if (layout == null) {
                // nextLayout() should not return null, but it does sometimes.
                log.warn(
                    "LineBreakMeasurer.nextLayout returned null going from position {} to {} for {} (length={})",
                    position, lineMeasurer.getPosition(), text, text.length());
                break;
            }
            position = lineMeasurer.getPosition();
            log.debug("formatWidth={}, boundaries={}, position={}, length={}, text={}", formatWidth,
                boundaries, position, text.length(), text);

            height += layout.getAscent() + 2 * layout.getDescent();
            descent = layout.getDescent();

            if (position > boundaries.get(boundaryIndex)) {
                // Read one or more words.
                while (boundaryIndex < boundaries.size() - 1
                    && position > boundaries.get(boundaryIndex)) {
                    lineMeasurer.setPosition(boundaries.get(boundaryIndex));
                    boundaryIndex++;
                }
            } else if (position < boundaries.get(boundaryIndex)) {
                // Skip long word.
                lineMeasurer.setPosition(boundaries.get(boundaryIndex));
            }
        }
        return (int) (height + descent);
    }

    private List<Integer> wordBoundaries(String text) {
        List<Integer> wordBoundaries = new ArrayList<>();
        Matcher matcher = WHITESPACE_PUNCTUATION.matcher(text);
        while (matcher.find()) {
            wordBoundaries.add(matcher.start());
        }
        return wordBoundaries;
    }

    /**
     * Determines whether the content of a table cell is "wrappable." What this means is that the
     * table is configured for text wrapping and that the content is not a boolean, is not null and
     * is not a subclass of {@link TreeNode},
     */
    private boolean isCellContentWrappable(int row, int column) {
        Object tableContent = table.getValueAt(row, column);
        return wrapText && table.getColumnClass(column) != Boolean.class && tableContent != null
            && !(tableContent instanceof TreeNode);
    }

    /**
     * Implements the {@link JTable#getCellRenderer(int, int)} for use with row shading and cell
     * wrapping.
     *
     * @param jTableRenderer {@link TableCellRenderer} component produced by the superclass
     * ({@link JTable}) version of {@link JTable#getCellRenderer(int, int)}.
     */
    private TableCellRenderer getCellRenderer(int row, int column,
        TableCellRenderer jTableRenderer) {
        return isCellContentWrappable(row, column) ? wrappingCellRenderer
            : table.getValueAt(row, column) instanceof Date ? dateCellRenderer : jTableRenderer;
    }

    /**
     * Subclass of {@link ETable} that supports the additional features in {@link ZiggyTable}.
     * Implemented as a subclass so that the methods and fields of the {@link ZiggyTable} instance
     * can be used.
     *
     * @author PT
     */
    class ZiggyETable extends ETable {

        private static final long serialVersionUID = 20230511L;

        @Override
        public Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
            return ZiggyTable.this.prepareRenderer(row, column,
                super.prepareRenderer(renderer, row, column));
        }

        @Override
        public TableCellRenderer getCellRenderer(int row, int column) {
            return ZiggyTable.this.getCellRenderer(row, column, super.getCellRenderer(row, column));
        }
    }

    /**
     * Subclass of {@link Outline} that supports the additional features in {@link ZiggyTable}.
     * Implemented as a subclass so that the methods and fields of the {@link ZiggyTable} instance
     * can be used.
     *
     * @author PT
     */
    class ZiggyOutline extends Outline {

        private static final long serialVersionUID = 20230511L;

        @Override
        public Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
            return ZiggyTable.this.prepareRenderer(row, column,
                super.prepareRenderer(renderer, row, column));
        }

        @Override
        public TableCellRenderer getCellRenderer(int row, int column) {
            return ZiggyTable.this.getCellRenderer(row, column,
                superclassOrDefaultRenderer(row, column));
        }

        // This is necessary because the Outline renderer has to render both the normal
        // table cells (as defined by the user) and also the cells in the column for the
        // tree nodes and the rows that contain a tree node. Meanwhile, the superclass
        // getCellRenderer() doesn't know what to do in those cases.
        private TableCellRenderer superclassOrDefaultRenderer(int row, int column) {
            return getValueAt(row, column) != null ? super.getCellRenderer(row, column)
                : getDefaultRenderer(String.class);
        }
    }

    /**
     * Implementation of {@link TableCellRenderer} that displays dates in a YYYY-MM-DD hh:mm:ss
     * format.
     *
     * @see WrappingCellRenderer
     * @author Bill Wohler
     */
    public class DateCellRenderer extends DefaultTableCellRenderer {

        private static final long serialVersionUID = 20230927L;
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
        public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {

            checkArgument(value instanceof Date,
                "DateCellRenderer can only render Date, not " + value.getClass().getSimpleName());

            if (isSelected) {
                super.setForeground(table.getSelectionForeground());
                super.setBackground(table.getSelectionBackground());
            } else {
                super.setForeground(
                    unselectedForeground != null ? unselectedForeground : table.getForeground());
                super.setBackground(
                    unselectedBackground != null ? unselectedBackground : table.getBackground());
            }

            setText(Iso8601Formatter.javaDateTimeSansMillisLocalFormatter().format(value));

            return this;
        }
    }
}
