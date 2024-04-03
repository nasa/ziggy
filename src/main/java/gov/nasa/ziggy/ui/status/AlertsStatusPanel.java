package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.event.ActionEvent;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.messages.AlertMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Displays a table of recent alerts.
 *
 * @author PT
 * @author Bill Wohler
 */
public class AlertsStatusPanel extends JPanel {
    private static final long serialVersionUID = 20230822L;

    private AlertsTableModel alertsTableModel;

    public AlertsStatusPanel() {
        buildComponent();
    }

    private void buildComponent() {
        JPanel toolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            ZiggySwingUtils.createButton("Clear", this::clear),
            ZiggySwingUtils.createButton("Ack", this::acknowledge));
        alertsTableModel = new AlertsTableModel();
        ZiggyTable<AlertMessage> ziggyTable = new ZiggyTable<>(alertsTableModel);
        ziggyTable.getTable().setShowVerticalLines(false);
        ziggyTable.getTable().setShowHorizontalLines(false);
        JScrollPane alertTableScrollPane = new JScrollPane(ziggyTable.getTable());

        GroupLayout layout = new GroupLayout(this);
        setLayout(layout);

        layout.setHorizontalGroup(
            layout.createParallelGroup().addComponent(toolBar).addComponent(alertTableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(toolBar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(alertTableScrollPane));
    }

    private void clear(ActionEvent evt) {
        alertsTableModel.clear();
        StatusPanel.ContentItem.ALERTS.menuItem().setState(Indicator.State.NORMAL);
    }

    private void acknowledge(ActionEvent evt) {
        StatusPanel.ContentItem.ALERTS.menuItem().setState(Indicator.State.NORMAL);
    }

    private static class AlertsTableModel extends AbstractTableModel
        implements ModelContentClass<AlertMessage> {

        private static final long serialVersionUID = 20230822L;

        private static final int MAX_ALERTS = 1000;
        private static final String WARNING_MESSAGE = "WARN level alerts present";
        private static final String ERROR_MESSAGE = "ERROR/INFRASTRUCTURE level alerts present";
        private static final String[] COLUMN_NAMES = { "Time", "Source", "Host", "Task", "Severity",
            "Message" };

        private final List<AlertMessage> alertMessages = new LinkedList<>();
        private final SimpleDateFormat formatter;

        public AlertsTableModel() {
            formatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss");

            ZiggyMessenger.subscribe(AlertMessage.class, message -> {
                addAlertMessage(message);
            });
        }

        public void clear() {
            alertMessages.clear();
            fireTableDataChanged();
        }

        public void addAlertMessage(AlertMessage msg) {
            if (alertMessages.size() >= MAX_ALERTS) {
                // Remove oldest message from bottom.
                alertMessages.remove(alertMessages.size() - 1);
            }

            // Insert new message at top.
            alertMessages.add(0, msg);

            fireTableRowsInserted(alertMessages.size() - 1, alertMessages.size() - 1);

            String severity = msg.getAlertData().getSeverity();
            Indicator alertIndicator = StatusPanel.ContentItem.ALERTS.menuItem();
            if (severity.equals("WARNING")) {
                alertIndicator.setState(Indicator.State.WARNING, WARNING_MESSAGE);
            } else if (severity.equals("ERROR") || severity.equals("INFRASTRUCTURE")) {
                alertIndicator.setState(Indicator.State.ERROR, ERROR_MESSAGE);
            }
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            Alert alert = alertMessages.get(rowIndex).getAlertData();

            return switch (columnIndex) {
                case 0 -> formatter.format(alert.getTimestamp());
                case 1 -> alert.getSourceComponent();
                case 2 -> alert.getProcessHost();
                case 3 -> alert.getSourceTaskId();
                case 4 -> alert.getSeverity();
                case 5 -> alert.getMessage();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public int getRowCount() {
            return alertMessages.size();
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<AlertMessage> tableModelContentClass() {
            return AlertMessage.class;
        }
    }
}
