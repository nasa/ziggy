package gov.nasa.ziggy.ui.status;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingUtilities;

import org.netbeans.swing.outline.Outline;

import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.process.StatusMessage;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.StringUtils;

/**
 * A status panel for worker processes. Status information is displayed using {@link Outline}.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class WorkerStatusPanel extends JPanel {

    private static final long serialVersionUID = 20230822L;

    private WorkerStatusTableModel model = new WorkerStatusTableModel();
    private ZiggyTable<WorkerStatusMessage> table = new ZiggyTable<>(model);
    private JLabel countTextField;
    private JLabel heapTextField;

    public WorkerStatusPanel() {
        buildComponent();

        ZiggyMessenger.subscribe(HeartbeatMessage.class, message -> {
            update((StatusMessage) null);
        });

        ZiggyMessenger.subscribe(WorkerStatusMessage.class, this::update);

        ZiggyMessenger.subscribe(WorkerResources.class, this::updateWorkerResources);
    }

    private void buildComponent() {

        JLabel count = new JLabel("Max worker count:");
        countTextField = new JLabel();

        JLabel heap = new JLabel("Max worker heap size:");
        heapTextField = new JLabel();

        JScrollPane workers = new JScrollPane(table.getTable());

        GroupLayout layout = new GroupLayout(this);
        layout.setAutoCreateGaps(true);
        setLayout(layout);

        layout.setHorizontalGroup(layout.createParallelGroup()
            .addGroup(layout.createSequentialGroup()
                .addComponent(count)
                .addComponent(countTextField)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addComponent(heap)
                .addComponent(heapTextField))
            .addComponent(workers));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addGroup(layout.createParallelGroup()
                .addComponent(count)
                .addComponent(countTextField)
                .addComponent(heap)
                .addComponent(heapTextField))
            .addComponent(workers));
    }

    public void update(StatusMessage statusMessage) {
        SwingUtilities.invokeLater(() -> {
            if (statusMessage != null) {
                model.updateModel((WorkerStatusMessage) statusMessage);
            } else {
                model.removeOutdatedMessages();
            }
            Indicator.State workerState = model.getRowCount() == 0 ? Indicator.State.IDLE
                : Indicator.State.NORMAL;
            StatusPanel.ContentItem.WORKERS.menuItem().setState(workerState);
        });
    }

    public void updateWorkerResources(WorkerResources resources) {
        countTextField.setText(Integer.toString(resources.getMaxWorkerCount()));
        heapTextField.setText(resources.humanReadableHeapSize().toString());
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new WorkerStatusPanel());
    }

    /**
     * Table model for display of worker status.
     * <p>
     * The model contains two {@link Map}s of the {@link WorkerStatusMessage} instances, where the
     * map keys are the task IDs: the current map, which is used in the current display; and the new
     * map, which is populated as instances of {@link WorkerStatusMessage} are received. Once all
     * the workers are believed to have reported in, the current map is replaced by the new map and
     * the table is updated.
     * <p>
     * Note that all methods must be synchronized. This ensures that no thread can attempt to use
     * the class get methods during the time when the new map replaces the current one, and
     * vice-versa.
     *
     * @author PT
     */
    static class WorkerStatusTableModel extends AbstractZiggyTableModel<WorkerStatusMessage> {

        private static final long serialVersionUID = 20230511L;

        private static final String[] COLUMN_NAMES = { "Worker", "State", "Age", "Instance", "Task",
            "Module", "UOW" };

        /**
         * Status messages in the model. The {@link Boolean} component of the {@link Map} indicates
         * whether the message is up-to-date, which is defined as a message that has arrived since
         * the previous heartbeat message from the supervisor. Messages that are not up-to-date are
         * removed when the next heartbeat message is detected. This is necessary to ensure that
         * workers that fail without sending a status message marked as a final message do not
         * remain in the model indefinitely.
         */
        private Map<WorkerStatusMessage, Boolean> statusMessages = new TreeMap<>();
        private Set<WorkerStatusMessage> messageSet = statusMessages.keySet();

        @Override
        public Class<WorkerStatusMessage> tableModelContentClass() {
            return WorkerStatusMessage.class;
        }

        @Override
        public synchronized int getRowCount() {
            return statusMessages.size();
        }

        @Override
        public synchronized int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public synchronized String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        /**
         * Updates the model with a new {@link WorkerStatusMessage}.
         */
        public synchronized void updateModel(WorkerStatusMessage message) {

            // Remove any existing messages for that task.
            statusMessages.remove(message);

            // Put the new message into the Map, unless it's a final message.
            if (!message.isLastMessageFromWorker()) {
                statusMessages.put(message, true);
            } else {
            }
            redrawTable();
        }

        /**
         * Removes out-of-date messages from the {@link Map} and marks all remaining messages as
         * out-of-date.
         */
        public synchronized void removeOutdatedMessages() {

            // Remove any messages that are not marked as up to date; these are from workers that
            // failed in some way that prevented them from sending a last message.
            Set<WorkerStatusMessage> obsoleteMessages = new HashSet<>();
            for (Map.Entry<WorkerStatusMessage, Boolean> entry : statusMessages.entrySet()) {
                if (!entry.getValue()) {
                    obsoleteMessages.add(entry.getKey());
                }
            }

            // Java Map doesn't have a removeAll() method? Shocking!
            if (!obsoleteMessages.isEmpty()) {
                for (WorkerStatusMessage message : obsoleteMessages) {
                    statusMessages.remove(message);
                }
                redrawTable();
            }

            // Mark all the remaining messages as out of date; these messages will either be
            // replaced by up to date messages, removed when a last message from a worker comes in,
            // or will be removed at the next call to this method.
            for (Map.Entry<WorkerStatusMessage, Boolean> entry : statusMessages.entrySet()) {
                entry.setValue(false);
            }
        }

        @Override
        public synchronized Object getValueAt(int rowIndex, int columnIndex) {
            WorkerStatusMessage message = getContentAtRow(rowIndex);
            return switch (columnIndex) {
                case 0 -> message.getSourceProcess().getKey();
                case 1 -> message.getState();
                case 2 -> StringUtils.elapsedTime(message.getProcessingStartTime(),
                    System.currentTimeMillis());
                case 3 -> message.getInstanceId();
                case 4 -> message.getTaskId();
                case 5 -> message.getModule();
                case 6 -> message.getModuleUow();
                default -> "";
            };
        }

        @Override
        public synchronized WorkerStatusMessage getContentAtRow(int row) {
            return new ArrayList<>(messageSet).get(row);
        }

        /**
         * Executes the fireTableDataChanged() method. Abstracted to a package-private method so
         * that unit tests can override it.
         */
        void redrawTable() {
            fireTableDataChanged();
        }

        /** For testing only. */
        Map<WorkerStatusMessage, Boolean> statusMessages() {
            return statusMessages;
        }

        /** For testing only. */
        Set<WorkerStatusMessage> messageSet() {
            return messageSet;
        }
    }
}
