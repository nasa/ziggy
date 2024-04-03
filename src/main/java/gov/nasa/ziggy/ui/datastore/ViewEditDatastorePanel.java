package gov.nasa.ziggy.ui.datastore;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.SwingUtilities;

import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.DatastoreRegexpCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * Panel for viewing and editing datastore configurations.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditDatastorePanel extends AbstractViewEditPanel<DatastoreRegexp> {

    private static final long serialVersionUID = 20240208L;

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
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void create() {
        throw new UnsupportedOperationException("Create not supported");
    }

    @Override
    protected void edit(int row) {
        DatastoreRegexp regexp = ziggyTable.getContentAtViewRow(row);

        if (regexp != null) {
            EditDatastoreRegexpDialog dialog = new EditDatastoreRegexpDialog(
                SwingUtilities.getWindowAncestor(this), regexp);
            dialog.setVisible(true);
            if (!dialog.isCancelled()) {
                ziggyTable.loadFromDatabase();
            }
        }
    }

    @Override
    protected void delete(int row) {
        throw new UnsupportedOperationException("Delete not supported");
    }

    @Override
    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return new HashSet<>();
    }

    public static class RegexpTableModel extends AbstractDatabaseModel<DatastoreRegexp> {

        private static final long serialVersionUID = 20240124L;

        private static final String[] COLUMN_NAMES = { "Name", "Value", "Include", "Exclude" };

        private List<DatastoreRegexp> datastoreRegexps = new ArrayList<>();

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
        public void loadFromDatabase() {
            datastoreRegexps = new DatastoreRegexpCrudProxy().retrieveAll();
            fireTableDataChanged();
        }

        @Override
        public DatastoreRegexp getContentAtRow(int row) {
            return datastoreRegexps.get(row);
        }

        @Override
        public Class<DatastoreRegexp> tableModelContentClass() {
            return DatastoreRegexp.class;
        }
    }
}
