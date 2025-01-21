package gov.nasa.ziggy.ui.util.table;

import java.awt.Rectangle;
import java.util.List;

import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates or inserts table rows if the data has changed. To use, return this object from
 * {@code SwingWorker.doInBackground()} and call the {@link #updateTable(AbstractTableModel)} method
 * in {@code SwingWorker.done()}. See also {@link #scrollToVisible(JTable)}.
 */
public class TableUpdater {

    private static final Logger log = LoggerFactory.getLogger(TableUpdater.class);

    private int previousRowCount;
    private int currentRowCount;

    private List<Integer> insertedRows;
    private List<Integer> updatedRows;
    private List<Integer> deletedRows;

    /**
     * Creates an {@code InstanceEventInfoComparer} object.
     *
     * @param previousElements a list of objects from the previous update that must have a
     * reasonable {@link #equals(Object)} method; if null, all currentElements are considered new
     * @param currentElements a list of objects for this update that must have a reasonable
     * {@link #equals(Object)} method; if null, all existing rows will be deleted
     */
    public TableUpdater(List<?> previousElements, List<?> currentElements) {
        log.trace("previousElements={}", previousElements);
        log.trace("currentElements={}", currentElements);

        previousRowCount = previousElements == null ? 0 : previousElements.size();
        currentRowCount = currentElements == null ? 0 : currentElements.size();

        // Check for a change in row count.
        if (currentRowCount == previousRowCount) {
            insertedRows = List.of();
            deletedRows = List.of();
        } else if (currentRowCount > previousRowCount) {
            insertedRows = List.of(previousRowCount, currentRowCount - 1);
            deletedRows = List.of();
        } else {
            insertedRows = List.of();
            deletedRows = List.of(currentRowCount, previousRowCount - 1);
        }

        // Check for updated rows.
        int minUpdatedRow = -1;
        int maxUpdatedRow = -1;
        for (int i = 0; i < Math.min(previousRowCount, currentRowCount); i++) {
            boolean elementsEqual = previousElements.get(i).equals(currentElements.get(i));
            if (minUpdatedRow == -1 && !elementsEqual) {
                minUpdatedRow = i;
            }
            if (!elementsEqual) {
                maxUpdatedRow = i;
            }
        }
        updatedRows = minUpdatedRow == -1 ? List.of() : List.of(minUpdatedRow, maxUpdatedRow);
    }

    /**
     * Updates the table with this objects elements. Must be called on the event dispatch thread.
     */
    public void updateTable(AbstractTableModel tableModel) {
        if (getDeletedRows().size() > 0) {
            log.debug("Deleting rows {} in {}", getDeletedRows(),
                tableModel.getClass().getSimpleName());
            for (int i = getDeletedRows().get(0); i <= getDeletedRows().get(1); i++) {
                tableModel.fireTableRowsDeleted(i, i);
            }
        }
        if (getInsertedRows().size() > 0) {
            log.debug("Inserting rows {} in {}", getInsertedRows(),
                tableModel.getClass().getSimpleName());
            for (int i = getInsertedRows().get(0); i <= getInsertedRows().get(1); i++) {
                tableModel.fireTableRowsInserted(i, i);
            }
        }
        if (getUpdatedRows().size() > 0) {
            log.debug("Updating rows {} in {}", getUpdatedRows(),
                tableModel.getClass().getSimpleName());
            for (int i = getUpdatedRows().get(0); i <= getUpdatedRows().get(1); i++) {
                tableModel.fireTableRowsUpdated(i, i);
            }
        }
    }

    /**
     * Scroll to make new rows visible, but only if the last row was already visible. Must be called
     * on the event dispatch thread.
     *
     * @param table the table to scroll; if null, no scrolling is attempted
     */
    public void scrollToVisible(JTable table) {
        if (table == null) {
            return;
        }

        // Note that the indices for tables are zero-based, so add or subtract one when going
        // between table rows and row counts.

        // Consider the previous row visible if more than half the row is visible (see height/2).
        Rectangle visibleRectangle = table.getVisibleRect();
        Rectangle previousRowRectangle = table.getCellRect(previousRowCount - 1, 0, true);
        previousRowRectangle.setSize(previousRowRectangle.width, previousRowRectangle.height / 2);
        boolean visible = visibleRectangle.contains(previousRowRectangle);

        if (currentRowCount > previousRowCount && visible) {
            table.scrollRectToVisible(table.getCellRect(currentRowCount - 1, 0, true));
        }
    }

    private List<Integer> getDeletedRows() {
        return deletedRows;
    }

    private List<Integer> getInsertedRows() {
        return insertedRows;
    }

    private List<Integer> getUpdatedRows() {
        return updatedRows;
    }
}
