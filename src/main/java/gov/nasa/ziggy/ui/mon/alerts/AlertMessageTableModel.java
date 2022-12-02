package gov.nasa.ziggy.ui.mon.alerts;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.alert.AlertMessage;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.ui.mon.master.MasterStatusPanel;

@SuppressWarnings("serial")
public class AlertMessageTableModel extends AbstractTableModel {
    private static final int MAX_ALERTS = 1000;
    private static final String AMBER_MESSAGE = "WARN level alerts present";
    private static final String RED_MESSAGE = "ERROR/INFRASTRUCTURE level alerts present";

    private final List<AlertMessage> alertMessages = new LinkedList<>();
    private final SimpleDateFormat formatter;

    public AlertMessageTableModel() {
        formatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
    }

    public void clear() {
        alertMessages.clear();
        fireTableDataChanged();
    }

    public void addAlertMessage(AlertMessage msg) {
        if (alertMessages.size() >= MAX_ALERTS) {
            // remove oldest (at the bottom)
            alertMessages.remove(alertMessages.size() - 1);
        }

        alertMessages.add(0, msg); // add to the beginning

        fireTableRowsInserted(alertMessages.size() - 1, alertMessages.size() - 1);

        String severity = msg.getAlertData().getSeverity();
        Indicator alertIndicator = MasterStatusPanel.alertsIndicator();
        if (severity.equals("WARNING")) {
            alertIndicator.setState(Indicator.State.AMBER, AMBER_MESSAGE);
        } else if (severity.equals("ERROR") || severity.equals("INFRASTRUCTURE")) {
            alertIndicator.setState(Indicator.State.RED, RED_MESSAGE);
        }
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        Alert alert = alertMessages.get(rowIndex).getAlertData();

        switch (columnIndex) {
            case 0:
                return formatter.format(alert.getTimestamp());
            case 1:
                return alert.getSourceComponent();
            case 2:
                return alert.getProcessHost();
            case 3:
                return alert.getSourceTaskId();
            // return Long.toString(alert.getSourceTaskId());
            case 4:
                return alert.getSeverity();
            case 5:
                return alert.getMessage();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public int getColumnCount() {
        return 6;
    }

    @Override
    public int getRowCount() {
        return alertMessages.size();
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Time";
            case 1:
                return "Source";
            case 2:
                return "Host";
            case 3:
                return "Task";
            case 4:
                return "Severity";
            case 5:
                return "Message";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
