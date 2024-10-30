package gov.nasa.ziggy.ui.util.table;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates or inserts table rows if the data has changed. To use, return this object from
 * {@code SwingWorker.doInBackground()} and call the {@link #updateTable(AbstractTableModel)} method
 * in {@code SwingWorker.done()}.
 */
public class TableUpdater {

    private static final Logger log = LoggerFactory.getLogger(TableUpdater.class);

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

        int previousCount = previousElements == null ? 0 : previousElements.size();
        int currentCount = currentElements == null ? 0 : currentElements.size();

        // Check for a change in row count.
        if (currentCount == previousCount) {
            insertedRows = List.of();
            deletedRows = List.of();
        } else if (currentCount > previousCount) {
            insertedRows = List.of(previousCount, currentCount - 1);
            deletedRows = List.of();
        } else {
            insertedRows = List.of();
            deletedRows = List.of(currentCount, previousCount - 1);
        }

        // Check for updated rows.
        int minUpdatedRow = -1;
        int maxUpdatedRow = -1;
        for (int i = 0; i < Math.min(previousCount, currentCount); i++) {
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
