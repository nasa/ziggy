package gov.nasa.ziggy.ui.datastore;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * Panel for viewing and editing datastore configurations.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditDatastorePanel extends AbstractViewEditPanel<DatastoreRegexp> {

    private static final Logger log = LoggerFactory.getLogger(ViewEditDatastorePanel.class);
    private static final long serialVersionUID = 20240614L;

    public ViewEditDatastorePanel() {
        super(new RegexpTableModel());
        buildComponent();

        // An explicit refresh to show the data shouldn't be necessary, but it is.
        refresh();
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void edit(int row) {
        DatastoreRegexp regexp = ziggyTable.getContentAtViewRow(row);

        EditDatastoreRegexpDialog dialog = new EditDatastoreRegexpDialog(
            SwingUtilities.getWindowAncestor(this), regexp);
        dialog.setVisible(true);
        if (!dialog.isCancelled()) {
            ziggyTable.loadFromDatabase();
        }
    }

    public static class RegexpTableModel extends AbstractZiggyTableModel<DatastoreRegexp>
        implements DatabaseModel {

        private static final long serialVersionUID = 20240614L;

        private static final String[] COLUMN_NAMES = { "Name", "Value", "Include", "Exclude" };

        private List<DatastoreRegexp> datastoreRegexps = new ArrayList<>();

        private final DatastoreOperations datastoreOperations = new DatastoreOperations();

        @Override
        public void loadFromDatabase() {
            new SwingWorker<List<DatastoreRegexp>, Void>() {
                @Override
                protected List<DatastoreRegexp> doInBackground() throws Exception {
                    return datastoreOperations().datastoreRegexps();
                }

                @Override
                protected void done() {
                    try {
                        datastoreRegexps = get();
                        fireTableDataChanged();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Could not retrieve datastore regexps", e);
                    }
                }
            }.execute();
        }

        @Override
        public int getRowCount() {
            return datastoreRegexps.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int column) {
            checkColumnArgument(column);
            return COLUMN_NAMES[column];
        }

        private void checkColumnArgument(int columnIndex) {
            checkArgument(columnIndex < COLUMN_NAMES.length, "Column value of " + columnIndex
                + " outside of expected range from 0 to " + COLUMN_NAMES.length);
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            checkColumnArgument(columnIndex);
            DatastoreRegexp regexp = getContentAtRow(rowIndex);
            return switch (columnIndex) {
                case 0 -> regexp.getName();
                case 1 -> regexp.getValue();
                case 2 -> regexp.getInclude();
                case 3 -> regexp.getExclude();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public DatastoreRegexp getContentAtRow(int row) {
            return datastoreRegexps.get(row);
        }

        @Override
        public Class<DatastoreRegexp> tableModelContentClass() {
            return DatastoreRegexp.class;
        }

        private DatastoreOperations datastoreOperations() {
            return datastoreOperations;
        }
    }
}
