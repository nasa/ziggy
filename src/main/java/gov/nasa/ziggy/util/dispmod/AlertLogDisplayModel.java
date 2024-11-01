package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.alert.AlertLog;

/**
 * {@link DisplayModel} for the alert log. This class is used to format the alert log for display on
 * the console.
 *
 * @author Todd Klaus
 */
public class AlertLogDisplayModel extends DisplayModel {
    private static final String[] COLUMN_NAMES = { "Source", "Task", "Severity", "Message" };

    private List<AlertLog> alerts = new ArrayList<>();

    public AlertLogDisplayModel() {
    }

    public AlertLogDisplayModel(List<AlertLog> alerts) {
        this.alerts = alerts;
    }

    public void update(List<AlertLog> alerts) {
        this.alerts = alerts;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        Alert alert = alerts.get(rowIndex).getAlertData();

        return switch (columnIndex) {
            case 0 -> alert.getSourceComponent();
            case 1 -> alert.getSourceTask();
            case 2 -> alert.getSeverity();
            case 3 -> alert.getMessage();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    @Override
    public int getRowCount() {
        return alerts.size();
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }
}
