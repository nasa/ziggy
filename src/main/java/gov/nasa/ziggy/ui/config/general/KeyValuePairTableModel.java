package gov.nasa.ziggy.ui.config.general;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.KeyValuePairCrudProxy;

@SuppressWarnings("serial")
public class KeyValuePairTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(KeyValuePairTableModel.class);

    private final KeyValuePairCrudProxy keyValuePairCrud;
    private List<KeyValuePair> keyValuePairs = new LinkedList<>();

    public KeyValuePairTableModel() {
        keyValuePairCrud = new KeyValuePairCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        if (keyValuePairs != null) {
            keyValuePairCrud.evictAll(keyValuePairs);
        }

        try {
            keyValuePairs = keyValuePairCrud.retrieveAll();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

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
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        KeyValuePair keyValuePair = keyValuePairs.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return keyValuePair.getKey();
            case 1:
                return keyValuePair.getValue();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return String.class;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Key";
            case 1:
                return "Value";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
