package gov.nasa.ziggy.ui.events;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.messages.EventHandlerRequest;
import gov.nasa.ziggy.services.messages.EventHandlerToggleStateRequest;
import gov.nasa.ziggy.services.messages.ZiggyEventHandlerInfoMessage;
import gov.nasa.ziggy.services.messaging.MessageAction;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.Requestor;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Panel to display the collection of {@link ZiggyEventHandler} instances and their states.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ZiggyEventHandlerPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(ZiggyEventHandlerPanel.class);
    private static final long serialVersionUID = 20230824L;

    private EventHandlerTableModel tableModel;

    public ZiggyEventHandlerPanel() {
        buildComponent();
        update();
    }

    private void buildComponent() {
        JPanel buttonPanel = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(REFRESH, this::refresh));
        tableModel = new EventHandlerTableModel();
        JScrollPane tableScrollPane = new JScrollPane(
            createEventHandlerTable(tableModel).getTable());

        GroupLayout layout = new GroupLayout(this);
        setLayout(layout);

        layout.setHorizontalGroup(
            layout.createParallelGroup().addComponent(buttonPanel).addComponent(tableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(buttonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(tableScrollPane));
    }

    private ZiggyTable<ZiggyEventHandlerInfoForDisplay> createEventHandlerTable(
        EventHandlerTableModel tableModel) {

        ZiggyTable<ZiggyEventHandlerInfoForDisplay> ziggyTable = new ZiggyTable<>(tableModel);

        for (int column = 0; column < EventHandlerTableModel.COLUMN_WIDTHS.length; column++) {
            ziggyTable.setPreferredColumnWidth(column,
                EventHandlerTableModel.COLUMN_WIDTHS[column]);
        }
        ziggyTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {

                // does the user want to enable or disable an event handler?
                int row = ziggyTable.rowAtPoint(e.getPoint());
                int column = ziggyTable.columnAtPoint(e.getPoint());
                log.debug("row={}, column={}", row, column);
                if (row != -1 && column == 3) {
                    EventHandlerToggleStateRequest
                        .requestEventHandlerToggle(tableModel.getName(row));
                    update();
                }
            }
        });
        return ziggyTable;
    }

    private void refresh(ActionEvent evt) {
        try {
            log.debug("evt={}", evt);
            update();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    public void update() {
        log.debug("Calling EventHandlerPanelUpdater");
        new EventHandlerPanelUpdater(SwingUtilities.getWindowAncestor(this), tableModel).execute();
    }

    private static class EventHandlerTableModel extends AbstractTableModel
        implements ModelContentClass<ZiggyEventHandlerInfoForDisplay>, Requestor {

        private static final long serialVersionUID = 20230824L;

        private static final String[] COLUMN_NAMES = { "Name", "Directory", "Pipeline", "Enabled" };
        private static final int[] COLUMN_WIDTHS = { 100, 500, 100, 100 };

        private final UUID uuid = UUID.randomUUID();
        private List<ZiggyEventHandlerInfoForDisplay> eventHandlers = new ArrayList<>();

        public String getName(int rowIndex) {
            return eventHandlers.get(rowIndex).getName();
        }

        @Override
        public int getRowCount() {
            return eventHandlers.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_NAMES[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ZiggyEventHandlerInfoForDisplay eventHandler = eventHandlers.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> eventHandler.getName();
                case 1 -> eventHandler.getDirectory();
                case 2 -> eventHandler.getPipelineName();
                case 3 -> eventHandler.isEnabled();
                default -> throw new IllegalArgumentException("Illegal column: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return switch (columnIndex) {
                case 3 -> Boolean.class;
                default -> String.class;
            };
        }

        @Override
        public Class<ZiggyEventHandlerInfoForDisplay> tableModelContentClass() {
            return ZiggyEventHandlerInfoForDisplay.class;
        }

        @Override
        public UUID requestorIdentifier() {
            return uuid;
        }

        /**
         * Updates the models event handlers.
         */
        private void setEventHandlers(Collection<ZiggyEventHandlerInfoForDisplay> handlerInfo) {
            log.debug("Updating model");
            eventHandlers.clear();
            eventHandlers.addAll(handlerInfo);
            fireTableDataChanged();
        }
    }

    /**
     * Event handler panel updater. Call the {@code execute()} method to update the panel. This
     * method can only be called once per object.
     *
     * @author PT
     * @author Bill Wohler
     */
    private static class EventHandlerPanelUpdater
        extends SwingWorker<Set<ZiggyEventHandlerInfoForDisplay>, Void> {

        private static final long PANEL_CONTENT_TIMEOUT_MILLIS = 2000L;

        private final Window owner;
        private EventHandlerTableModel tableModel;
        private ZiggyEventHandlerInfoMessage message;

        public EventHandlerPanelUpdater(Window owner, EventHandlerTableModel tableModel) {
            this.owner = owner;
            this.tableModel = tableModel;
        }

        /**
         * Retrieves the event handler information. This is done by publishing an
         * {@link EventHandlerRequest} and subscribing to {@link ZiggyEventHandlerInfoMessage} for
         * the response.
         */
        @Override
        protected Set<ZiggyEventHandlerInfoForDisplay> doInBackground() throws Exception {
            CountDownLatch countdownLatch = new CountDownLatch(1);

            MessageAction<ZiggyEventHandlerInfoMessage> action = message -> {
                if (tableModel.isDestination(message)) {
                    log.debug("thread={}, message={}", Thread.currentThread().getName(), message);
                    this.message = message;
                    countdownLatch.countDown();
                }
            };
            ZiggyMessenger.subscribe(ZiggyEventHandlerInfoMessage.class, action);
            ZiggyMessenger.publish(new EventHandlerRequest(tableModel));
            countdownLatch.await(PANEL_CONTENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

            log.debug("thread={}, message={}", Thread.currentThread().getName(), message);
            ZiggyMessenger.unsubscribe(ZiggyEventHandlerInfoMessage.class, action);

            return message != null ? message.getEventHandlerInfo() : null;
        }

        @Override
        protected void done() {
            try {
                Set<ZiggyEventHandlerInfoForDisplay> displayContent = get();
                log.debug("Received {} items", displayContent == null ? 0 : displayContent.size());
                if (displayContent == null) {
                    MessageUtil.showError(owner, "Event handler update timed out.");
                    return;
                }
                tableModel.setEventHandlers(displayContent);
            } catch (InterruptedException | ExecutionException e) {
                MessageUtil.showError(owner, e);
            }
        }
    }
}
