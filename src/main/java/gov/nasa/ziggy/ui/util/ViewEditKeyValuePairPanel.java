package gov.nasa.ziggy.ui.util;

import java.util.LinkedList;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.KeyValuePairCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ViewEditKeyValuePairPanel extends AbstractViewEditPanel<KeyValuePair> {

    public ViewEditKeyValuePairPanel() {
        super(new KeyValuePairTableModel());
        buildComponent();
    }

    @Override
    protected void create() {
        showEditDialog(new KeyValuePair());
    }

    @Override
    protected void edit(int row) {
        showEditDialog(ziggyTable.getContentAtViewRow(row));
    }

    private void showEditDialog(KeyValuePair keyValuePair) {

        EditKeyValuePairDialog dialog = new EditKeyValuePairDialog(
            SwingUtilities.getWindowAncestor(this), keyValuePair);
        dialog.setVisible(true);

        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void delete(int row) {

        KeyValuePair keyValuePair = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete key '" + keyValuePair.getKey() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                // TODO: Need transaction/conversation context
                // TODO: only call create for new
                KeyValuePairCrud keyValuePairCrud = new KeyValuePairCrud();
                keyValuePairCrud.delete(keyValuePair);

                ziggyTable.loadFromDatabase();
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private static class KeyValuePairTableModel extends AbstractDatabaseModel<KeyValuePair> {
        private final KeyValuePairCrudProxy keyValuePairCrud;
        private List<KeyValuePair> keyValuePairs = new LinkedList<>();

        private static final String[] COLUMN_NAMES = { "Key", "Value" };

        public KeyValuePairTableModel() {
            keyValuePairCrud = new KeyValuePairCrudProxy();
        }

        @Override
        public void loadFromDatabase() {
            try {
                keyValuePairs = keyValuePairCrud.retrieveAll();
            } catch (ConsoleSecurityException ignore) {
            }

            fireTableDataChanged();
        }

        // TODO Either find a use for getKeyValuePairAtRow or delete
        @SuppressWarnings("unused")
        public KeyValuePair getKeyValuePairAtRow(int rowIndex) {
            validityCheck();
            return keyValuePairs.get(rowIndex);
        }

        @Override
        public int getRowCount() {
            validityCheck();
            return keyValuePairs.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            validityCheck();

            KeyValuePair keyValuePair = keyValuePairs.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> keyValuePair.getKey();
                case 1 -> keyValuePair.getValue();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return String.class;
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public KeyValuePair getContentAtRow(int row) {
            validityCheck();
            return keyValuePairs.get(row);
        }

        @Override
        public Class<KeyValuePair> tableModelContentClass() {
            return KeyValuePair.class;
        }
    }
}
