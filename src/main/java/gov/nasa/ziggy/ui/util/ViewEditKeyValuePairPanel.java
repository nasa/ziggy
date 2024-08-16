package gov.nasa.ziggy.ui.util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairOperations;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * Panel for displaying and editing arbitrary key/value pairs.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ViewEditKeyValuePairPanel extends AbstractViewEditPanel<KeyValuePair> {
    private static Logger log = LoggerFactory.getLogger(ViewEditKeyValuePairPanel.class);

    private final KeyValuePairOperations keyValuePairOperations = new KeyValuePairOperations();

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
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void delete(int row) {

        KeyValuePair keyValuePair = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete key '" + keyValuePair.getKey() + "'?");

        if (choice != JOptionPane.YES_OPTION) {
            return;
        }
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                keyValuePairOperations().delete(keyValuePair);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    ziggyTable.loadFromDatabase();
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(ViewEditKeyValuePairPanel.this, e);
                }
            }
        }.execute();
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    private KeyValuePairOperations keyValuePairOperations() {
        return keyValuePairOperations;
    }

    private static class KeyValuePairTableModel extends AbstractZiggyTableModel<KeyValuePair>
        implements DatabaseModel {

        private static final String[] COLUMN_NAMES = { "Key", "Value" };

        private List<KeyValuePair> keyValuePairs = new LinkedList<>();

        private final KeyValuePairOperations keyValuePairOperations = new KeyValuePairOperations();

        public KeyValuePairTableModel() {
            ZiggyMessenger.subscribe(InvalidateConsoleModelsMessage.class, this::invalidateModel);
        }

        private void invalidateModel(InvalidateConsoleModelsMessage message) {
            loadFromDatabase();
        }

        @Override
        public void loadFromDatabase() {
            new SwingWorker<List<KeyValuePair>, Void>() {
                @Override
                protected List<KeyValuePair> doInBackground() throws Exception {
                    return keyValuePairOperations().keyValuePairs();
                }

                @Override
                protected void done() {
                    try {
                        keyValuePairs = get();
                        fireTableDataChanged();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Can't retrieve key/value pairs", e);
                    }
                }
            }.execute();
        }

        // TODO Either find a use for getKeyValuePairAtRow or delete
        @SuppressWarnings("unused")
        public KeyValuePair getKeyValuePairAtRow(int rowIndex) {
            return keyValuePairs.get(rowIndex);
        }

        @Override
        public int getRowCount() {
            return keyValuePairs.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
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
            return keyValuePairs.get(row);
        }

        @Override
        public Class<KeyValuePair> tableModelContentClass() {
            return KeyValuePair.class;
        }

        private KeyValuePairOperations keyValuePairOperations() {
            return keyValuePairOperations;
        }
    }
}
