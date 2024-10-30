package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.event.ActionEvent;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.messages.AlertMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Displays a table of recent alerts.
 *
 * @author PT
 * @author Bill Wohler
 */
public class AlertsStatusPanel extends JPanel {
    private static final long serialVersionUID = 20240924L;

    private static final Logger log = LoggerFactory.getLogger(AlertsStatusPanel.class);

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

        private static final long serialVersionUID = 20240924L;

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
            new SwingWorker<Void, Void>() {

                @Override
                protected Void doInBackground() throws Exception {
                    if (alertMessages.size() >= MAX_ALERTS) {
                        // Remove oldest message from bottom.
                        alertMessages.remove(alertMessages.size() - 1);
                    }

                    // Insert new message at top.
                    alertMessages.add(0, msg);
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get(); // check for exception
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Could not load pipeline module definitions", e);
                    }

                    // Remember that we inserted the new message at the top.
                    fireTableRowsInserted(0, 0);

                    Severity severity = msg.getAlertData().getSeverity();
                    Indicator alertIndicator = StatusPanel.ContentItem.ALERTS.menuItem();

                    // If the state is already set to ERROR, it's already as bad as it'll get.
                    // In particular, don't change the state from ERROR to WARNING.
                    if (alertIndicator.getState() == Indicator.State.ERROR) {
                        return;
                    }

                    if (severity == Severity.WARNING) {
                        alertIndicator.setState(Indicator.State.WARNING, WARNING_MESSAGE);
                    } else if (severity == Severity.ERROR || severity == Severity.INFRASTRUCTURE) {
                        alertIndicator.setState(Indicator.State.ERROR, ERROR_MESSAGE);
                    }
                }
            }.execute();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            Alert alert = alertMessages.get(rowIndex).getAlertData();

            return switch (columnIndex) {
                case 0 -> formatter.format(alert.getTimestamp());
                case 1 -> alert.getSourceComponent();
                case 2 -> HostNameUtils.callerHostNameOrLocalhost(alert.getProcessHost());
                case 3 -> alert.getSourceTask();
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
