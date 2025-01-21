package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createPopupMenu;

import java.awt.Component;
import java.awt.Desktop;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.apache.commons.lang3.StringUtils;
import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.etable.ETableColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.PerformanceReport;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceFilter;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.events.ZiggyEvent;
import gov.nasa.ziggy.services.events.ZiggyEventOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.TaskHalter;
import gov.nasa.ziggy.ui.util.TaskRestarter;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.TableUpdater;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Displays just the table of instances.
 *
 * @author PT
 * @author Bill Wohler
 */
public class InstancesTable extends JPanel {
    private static final long serialVersionUID = 20240614L;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(InstancesTable.class);

    private Component parent;
    private ZiggyTable<PipelineInstance> instancesTable;
    private InstancesTableModel instancesTableModel;

    // Index in the table model of the selected instance, not to be confused with the instance
    // ID of that instance, which is instanceId.
    private int selectedInstanceIndex = -1;
    private long instanceId = -1;

    public InstancesTable(PipelineInstanceFilter filter) {
        this(null, filter);
    }

    public InstancesTable(JPanel parent, PipelineInstanceFilter instancesFilter) {
        this.parent = parent;
        createInstancesTable(instancesFilter);

        ZiggyMessenger.subscribe(PipelineInstanceStartedMessage.class, this::invalidateModel);
    }

    private void invalidateModel(PipelineInstanceStartedMessage message) {
        instancesTableModel.loadFromDatabase();
    }

    private void createInstancesTable(PipelineInstanceFilter instancesFilter) {
        instancesTable = new ZiggyTable<>(instancesTableModel(instancesFilter));
        instancesTableModel.setTable(getTable());
        instancesTable.setWrapText(false);
        for (int column = 0; column < InstancesTableModel.COLUMN_WIDTHS.length; column++) {
            instancesTable.setPreferredColumnWidth(column,
                InstancesTableModel.COLUMN_WIDTHS[column]);
        }

        ListSelectionModel selectionModel = instancesTable.getTable().getSelectionModel();
        selectionModel.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        selectionModel.addListSelectionListener(evt -> {
            if (evt.getValueIsAdjusting()) {
                return;
            }
            try {
                selectNewPipelineInstance(selectionModel.getMinSelectionIndex());
            } catch (Exception e) {
                MessageUtils.showError(SwingUtilities.getWindowAncestor(parent), e);
            }
        });

        // Initially hide the Event name column to save space.
        ETableColumnModel columnModel = (ETableColumnModel) instancesTable.getTable()
            .getColumnModel();
        columnModel.setColumnHidden(
            columnModel.getColumn(columnModel.getColumnIndex(InstancesTableModel.EVENT_NAME)),
            true);

        configurePopupMenu(instancesTable);
    }

    public InstancesTableModel instancesTableModel(PipelineInstanceFilter instancesFilter) {
        if (instancesTableModel == null) {
            instancesTableModel = new InstancesTableModel(instancesFilter);
        }
        return instancesTableModel;
    }

    /**
     * Captures the selected row and selected pipeline instance ID when the user selects a row in
     * the instances table or possibly clears the selection when the table changes.
     */
    private void selectNewPipelineInstance(int selectedRow) {
        selectedInstanceIndex = instancesTable.convertRowIndexToModel(selectedRow);
        PipelineInstance pipelineInstance = selectedPipelineInstance();
        long oldInstanceId = instanceId;
        instanceId = pipelineInstance != null ? pipelineInstance.getId() : -1;
        if (instanceId != oldInstanceId) {
            ZiggyMessenger.publish(new SelectedInstanceChangedMessage(instanceId), false);
        }
    }

    public PipelineInstance selectedPipelineInstance() {
        if (selectedInstanceIndex < 0) {
            return null;
        }
        return instancesTableModel.getContentAtRow(selectedInstanceIndex).getPipelineInstance();
    }

    /**
     * Configures the pipeline instances popup menu.
     */
    private void configurePopupMenu(ZiggyTable<PipelineInstance> instancesTable) {
        JPopupMenu instancesPopupMenu = instancesPopupMenu();
        if (instancesPopupMenu == null) {
            return;
        }

        instancesTable.getTable().addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    ZiggySwingUtils.adjustSelection(instancesTable.getTable(), e);
                    if (instanceId != -1) {
                        instancesPopupMenu.show(instancesTable.getTable(), e.getX(), e.getY());
                    }
                }
            }
        });
    }

    private JPopupMenu instancesPopupMenu() {
        if (parent == null) {
            return null;
        }
        return createPopupMenu(createMenuItem("Details" + DIALOG, this::displayDetails),
            createMenuItem("Performance report" + DIALOG, this::displayPerformanceReport),
            createMenuItem("Alerts" + DIALOG, this::displayAlerts),
            createMenuItem("Performance statistics" + DIALOG, this::displayStatistics),
            createMenuItem("Estimate cost" + DIALOG, this::estimateCost),
            ZiggySwingUtils.MENU_SEPARATOR, createMenuItem("Restart" + DIALOG, this::restartTasks),
            ZiggySwingUtils.MENU_SEPARATOR,
            createMenuItem("Halt all incomplete tasks", this::haltTasks));
    }

    private void displayDetails(ActionEvent evt) {
        try {
            InstanceDetailsDialog instanceDetailsDialog = new InstanceDetailsDialog(
                SwingUtilities.getWindowAncestor(parent), selectedPipelineInstance());
            instanceDetailsDialog.setVisible(true);
        } catch (Throwable e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(parent), e);
        }
    }

    private void displayPerformanceReport(ActionEvent evt) {
        PerformanceReport report = new PerformanceReport(selectedPipelineInstance().getId(),
            DirectoryProperties.taskDataDir().toFile(), null);

        new SwingWorker<Path, Void>() {
            @Override
            protected Path doInBackground() throws Exception {
                return report.generateReport();
            }

            @Override
            protected void done() {
                try {
                    Desktop.getDesktop().open(get().toFile());
                } catch (IOException | InterruptedException | ExecutionException e) {
                    MessageUtils.showError(InstancesTable.this, e);
                }
            }
        }.execute();
    }

    private void displayAlerts(ActionEvent evt) {
        try {
            new AlertLogDialog(SwingUtilities.getWindowAncestor(parent),
                selectedPipelineInstance().getId()).setVisible(true);
        } catch (Exception e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(parent),
                "Failed to display alerts", e.getMessage(), e);
        }
    }

    private void displayStatistics(ActionEvent evt) {
        try {
            new InstanceStatsDialog(SwingUtilities.getWindowAncestor(parent),
                selectedPipelineInstance()).setVisible(true);
        } catch (Exception e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(parent),
                "Failed to retrieve performance statistics", e.getMessage(), e);
        }
    }

    private void estimateCost(ActionEvent evt) {
        try {
            new InstanceCostEstimateDialog(SwingUtilities.getWindowAncestor(parent),
                selectedPipelineInstance()).setVisible(true);
        } catch (Throwable e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(parent), e);
        }
    }

    private void restartTasks(ActionEvent evt) {
        new TaskRestarter().restartTasks(SwingUtilities.getWindowAncestor(parent),
            selectedPipelineInstance());
    }

    private void haltTasks(ActionEvent evt) {
        new TaskHalter().haltTasks(SwingUtilities.getWindowAncestor(parent),
            selectedPipelineInstance());
    }

    public ETable getTable() {
        return instancesTable.getTable();
    }

    public void clearSelection() {
        getTable().getSelectionModel().clearSelection();
    }

    public void loadFromDatabase() {
        try {
            instancesTableModel.loadFromDatabase();
        } catch (PipelineException e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(parent), e);
        }
    }

    public State getStateOfInstanceWithMaxId() {
        return instancesTableModel.getStateOfInstanceWithMaxId();
    }

    @SuppressWarnings("serial")
    static class InstancesTableModel extends AbstractZiggyTableModel<InstanceEventInfo>
        implements DatabaseModel {

        private static final Logger log = LoggerFactory.getLogger(InstancesTableModel.class);

        private static final String EVENT_NAME = "Event name";
        private static final String[] COLUMN_NAMES = { "ID", "Pipeline", EVENT_NAME, "Date",
            "Status", "Time" };
        private static final int[] COLUMN_WIDTHS = {
            ZiggySwingUtils.textWidth(new JLabel(), "1234"),
            ZiggySwingUtils.textWidth(new JLabel(), "123456789012345678"),
            ZiggySwingUtils.textWidth(new JLabel(), EVENT_NAME),
            ZiggySwingUtils.textWidth(new JLabel(), "0000-00-00 00:00:00"),
            ZiggySwingUtils.textWidth(new JLabel(), "ERRORS_STALLED"),
            ZiggySwingUtils.textWidth(new JLabel(), "00:00:00") };

        private final PipelineInstanceFilter filter;
        private List<InstanceEventInfo> instanceEventInfoList = new LinkedList<>();
        private JTable table;

        private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
        private final ZiggyEventOperations ziggyEventOperations = new ZiggyEventOperations();

        public InstancesTableModel(PipelineInstanceFilter filter) {
            this.filter = filter;
        }

        public void setTable(JTable table) {
            this.table = table;
        }

        @Override
        public void loadFromDatabase() {
            new SwingWorker<TableUpdater, Void>() {

                @Override
                protected TableUpdater doInBackground() throws Exception {
                    // Read the pipeline instances and associated events and merge them in a
                    // map sorted by instance ID (the key).
                    log.trace("filter={}", filter);
                    Map<Long, InstanceEventInfo> instanceEventInfoById = new TreeMap<>();
                    List<PipelineInstance> instances = pipelineInstanceOperations()
                        .pipelineInstance(filter);
                    for (PipelineInstance instance : instances) {
                        instanceEventInfoById.putIfAbsent(instance.getId(),
                            new InstanceEventInfo(instance, null));
                    }
                    for (ZiggyEvent ziggyEvent : ziggyEventOperations().events(instances)) {
                        instanceEventInfoById.computeIfPresent(ziggyEvent.getPipelineInstanceId(),
                            (k, v) -> new InstanceEventInfo(v.getPipelineInstance(), ziggyEvent));
                    }

                    // Use the sorted map to create a sorted list of container objects.
                    List<InstanceEventInfo> oldInstanceEventInfoList = instanceEventInfoList;
                    instanceEventInfoList = new ArrayList<>(instanceEventInfoById.values());

                    return new TableUpdater(oldInstanceEventInfoList, instanceEventInfoList);
                }

                @Override
                protected void done() {
                    try {
                        TableUpdater tableUpdater = get();
                        tableUpdater.updateTable(InstancesTableModel.this);
                        tableUpdater.scrollToVisible(table);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Can't update instances table", e);
                    }
                }
            }.execute();
        }

        /**
         * Returns the model ID corresponding to a selected pipeline instance ID. If the selected
         * pipeline instance ID is less than zero, or if it is not present in the model, -1 is
         * returned.
         */
        public int getModelIndexOfInstance(long instanceId) {
            int modelId = -1;
            if (instanceId < 0) {
                return modelId;
            }
            for (int i = 0; i < instanceEventInfoList.size(); i++) {
                if (instanceEventInfoList.get(i).getPipelineInstance().getId() == instanceId) {
                    modelId = i;
                    break;
                }
            }
            return modelId;
        }

        /**
         * Returns the state of the instance with max ID. Assumes that the list of instances is
         * sorted ascending, which is the expected behavior of the PipelineInstanceOperations
         * retriever.
         *
         * @return state of instance with max ID number
         */
        public State getStateOfInstanceWithMaxId() {
            if (instanceEventInfoList.isEmpty()) {
                return State.COMPLETED;
            }
            return instanceEventInfoList.get(instanceEventInfoList.size() - 1)
                .getPipelineInstance()
                .getState();
        }

        @Override
        public int getRowCount() {
            return instanceEventInfoList.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            InstanceEventInfo instanceEventInfo = instanceEventInfoList.get(rowIndex);
            PipelineInstance pipelineInstance = instanceEventInfo.getPipelineInstance();
            ZiggyEvent ziggyEvent = instanceEventInfo.getZiggyEvent();

            return switch (columnIndex) {
                case 0 -> pipelineInstance.getId();
                case 1 -> pipelineInstance.getPipelineDefinition().getName()
                    + (StringUtils.isBlank(pipelineInstance.getName()) ? ""
                        : ": " + pipelineInstance.getName());
                case 2 -> ziggyEvent != null ? ziggyEvent.getEventHandlerName() : "-";
                case 3 -> ziggyEvent != null ? ziggyEvent.getEventTime()
                    : pipelineInstance.getCreated();
                case 4 -> pipelineInstance.getState().toString();
                case 5 -> pipelineInstance.getExecutionClock().toString();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public InstanceEventInfo getContentAtRow(int row) {
            return instanceEventInfoList.get(row);
        }

        @Override
        public Class<InstanceEventInfo> tableModelContentClass() {
            return InstanceEventInfo.class;
        }

        private PipelineInstanceOperations pipelineInstanceOperations() {
            return pipelineInstanceOperations;
        }

        private ZiggyEventOperations ziggyEventOperations() {
            return ziggyEventOperations;
        }
    }

    private static class InstanceEventInfo {
        private PipelineInstance pipelineInstance;
        private ZiggyEvent ziggyEvent;
        private String time;

        public InstanceEventInfo(PipelineInstance pipelineInstance, ZiggyEvent ziggyEvent) {
            this.pipelineInstance = pipelineInstance;
            this.ziggyEvent = ziggyEvent;
            time = pipelineInstance.getExecutionClock().toString();
        }

        public PipelineInstance getPipelineInstance() {
            return pipelineInstance;
        }

        public ZiggyEvent getZiggyEvent() {
            return ziggyEvent;
        }

        // When updating hashCode() and equals(), use totalHashCode() and totalEquals() with
        // pipelineInstance.
        @Override
        public int hashCode() {
            return Objects.hash(pipelineInstance.totalHashCode(), time, ziggyEvent);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            InstanceEventInfo other = (InstanceEventInfo) obj;
            return pipelineInstance.totalEquals(other.pipelineInstance)
                && Objects.equals(time, other.time) && Objects.equals(ziggyEvent, other.ziggyEvent);
        }

        @Override
        public String toString() {
            return "pipelineInstance.id=" + pipelineInstance.getId() + ", time=" + time
                + ", pipelineInstance.state=" + pipelineInstance.getState() + ", ziggyEvent="
                + ziggyEvent;
        }
    }
}
