package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createPopupMenu;

import java.awt.Desktop;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.etable.ETableColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.PerformanceReport;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceFilter;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.events.ZiggyEvent;
import gov.nasa.ziggy.services.messages.DisplayTasksForInstanceMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxyExecutor;
import gov.nasa.ziggy.ui.util.proxy.PipelineInstanceCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ZiggyEventCrudProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Displays just the table of instances.
 *
 * @author PT
 * @author Bill Wohler
 */
public class InstancesTable extends JPanel {
    private static final long serialVersionUID = 20230822L;

    private InstancesTasksPanel instancesTasksPanel;
    private ZiggyTable<PipelineInstance> instancesTable;
    private InstancesTableModel instancesTableModel;

    // Index in the table model of the selected instance, not to be confused with the instance
    // ID of that instance, which is instanceId.
    private int selectedInstanceIndex = -1;
    private long instanceId = -1;

    public InstancesTable(PipelineInstanceFilter filter) {
        this(null, filter);
    }

    public InstancesTable(InstancesTasksPanel instancesTasksPanel,
        PipelineInstanceFilter instancesFilter) {
        this.instancesTasksPanel = instancesTasksPanel;
        createInstancesTable(instancesFilter);
    }

    private void createInstancesTable(PipelineInstanceFilter instancesFilter) {
        instancesTable = new ZiggyTable<>(instancesTableModel(instancesFilter));
        instancesTable.registerModel();
        instancesTable.setWrapText(false);
        instancesTable.getTable()
            .getSelectionModel()
            .setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        instancesTable.getTable().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        instancesTable.getTable().getSelectionModel().addListSelectionListener(event -> {
            try {
                // Ignore extra messages.
                if (event.getValueIsAdjusting()) {
                    return;
                }

                ListSelectionModel selectionModel = instancesTable.getTable().getSelectionModel();
                if (selectionModel.isSelectionEmpty()) {
                    reselectPipelineInstance();
                } else {
                    selectNewPipelineInstance(selectionModel);
                }
            } catch (Exception e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
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
     * Reselects the correct row in the pipeline instance table after the table has been reset
     * (typically by auto-refresh).
     */
    private void reselectPipelineInstance() {

        // If the selected instance from before the update is still in the
        // table, then select it again
        int instanceModelIndex = instancesTableModel.getModelIndexOfInstance(instanceId);

        if (instanceModelIndex == -1) {
            instanceId = -1;
        } else {
            int rowIndex = instancesTable.getTable().convertRowIndexToView(instanceModelIndex);
            instancesTable.getTable().getSelectionModel().setSelectionInterval(rowIndex, rowIndex);
        }
        ZiggyMessenger.publish(new DisplayTasksForInstanceMessage(true, instanceId));
    }

    /**
     * Captures the selected row and selected pipeline instance ID. Executes when the user selects a
     * row in the instances table.
     */
    private void selectNewPipelineInstance(ListSelectionModel lsm) {

        int selectedRow = lsm.getMinSelectionIndex();
        selectedInstanceIndex = instancesTable.convertRowIndexToModel(selectedRow);
        instanceId = instancesTableModel.getContentAtRow(selectedInstanceIndex)
            .getPipelineInstance()
            .getId();
        ZiggyMessenger.publish(new DisplayTasksForInstanceMessage(false, instanceId));
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
                if (instanceId == -1) {
                    return;
                }
                if (e.isPopupTrigger()) {
                    instancesPopupMenu.show(instancesTable.getTable(), e.getX(), e.getY());
                }
            }
        });
    }

    private JPopupMenu instancesPopupMenu() {
        if (instancesTasksPanel == null) {
            return null;
        }
        return createPopupMenu(createMenuItem("Details" + DIALOG, this::displayDetails),
            createMenuItem("Performance report" + DIALOG, this::displayPerformanceReport),
            createMenuItem("Alerts" + DIALOG, this::displayAlerts),
            createMenuItem("Performance statistics" + DIALOG, this::displayStatistics),
            createMenuItem("Estimate cost" + DIALOG, this::estimateCost),
            ZiggySwingUtils.MENU_SEPARATOR,
            createMenuItem("Restart all failed tasks" + DIALOG, this::restartTasks),
            ZiggySwingUtils.MENU_SEPARATOR,
            createMenuItem("Halt all incomplete tasks", this::haltTasks));
    }

    private void displayDetails(ActionEvent evt) {
        try {
            InstanceDetailsDialog instanceDetailsDialog = new InstanceDetailsDialog(
                SwingUtilities.getWindowAncestor(this),
                instancesTableModel.getContentAtRow(selectedInstanceIndex).getPipelineInstance());
            instanceDetailsDialog.setVisible(true);
        } catch (Throwable e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    private void displayPerformanceReport(ActionEvent evt) {
        PerformanceReport report = new PerformanceReport(
            instancesTableModel.getContentAtRow(selectedInstanceIndex)
                .getPipelineInstance()
                .getId(),
            DirectoryProperties.taskDataDir().toFile(), null);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            try {
                Desktop.getDesktop().open(report.generateReport().toFile());
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
            return null;
        });
    }

    private void displayAlerts(ActionEvent evt) {
        try {
            new AlertLogDialog(SwingUtilities.getWindowAncestor(this),
                instancesTableModel.getContentAtRow(selectedInstanceIndex)
                    .getPipelineInstance()
                    .getId()).setVisible(true);
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), "Failed to Restart Tasks",
                e.getMessage(), e);
        }
    }

    private void displayStatistics(ActionEvent evt) {
        try {
            new InstanceStatsDialog(SwingUtilities.getWindowAncestor(this),
                instancesTableModel.getContentAtRow(selectedInstanceIndex).getPipelineInstance())
                    .setVisible(true);
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this),
                "Failed to Retrieve Performance Stats", e.getMessage(), e);
        }
    }

    private void estimateCost(ActionEvent evt) {
        try {
            new InstanceCostEstimateDialog(SwingUtilities.getWindowAncestor(this),
                instancesTableModel.getContentAtRow(selectedInstanceIndex).getPipelineInstance())
                    .setVisible(true);
        } catch (Throwable e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    private void restartTasks(ActionEvent evt) {
        instancesTasksPanel.restartTasks(null);
    }

    private void haltTasks(ActionEvent evt) {
        instancesTasksPanel.haltTasks(null);
    }

    public ETable getTable() {
        return instancesTable.getTable();
    }

    public void loadFromDatabase() {
        try {
            instancesTableModel.loadFromDatabase();
        } catch (PipelineException e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    public State getStateOfInstanceWithMaxid() {
        return instancesTableModel.getStateOfInstanceWithMaxid();
    }

    public PipelineInstance selectedPipelineInstance() {
        if (selectedInstanceIndex < 0) {
            return null;
        }
        return instancesTableModel.getContentAtRow(selectedInstanceIndex).getPipelineInstance();
    }

    @SuppressWarnings("serial")
    static class InstancesTableModel extends AbstractDatabaseModel<InstanceEventInfo> {

        private static final Logger log = LoggerFactory.getLogger(InstancesTableModel.class);

        private static final String EVENT_NAME = "Event name";
        private static final String[] COLUMN_NAMES = { "ID", "Pipeline name", EVENT_NAME, "Date",
            "State", "P-time" };

        private final PipelineInstanceCrudProxy pipelineInstanceCrud = new PipelineInstanceCrudProxy();
        private final ZiggyEventCrudProxy ziggyEventCrud = new ZiggyEventCrudProxy();
        private final PipelineInstanceFilter filter;
        private List<InstanceEventInfo> instanceEventInfoList = new LinkedList<>();

        public InstancesTableModel(PipelineInstanceFilter filter) {
            this.filter = filter;
        }

        @Override
        public void loadFromDatabase() {
            SwingWorker<Void, Void> swingWorker = new SwingWorker<>() {

                @Override
                protected Void doInBackground() throws Exception {
                    try {
                        // Read the pipeline instances and associated events and merge them in a
                        // map sorted by instance ID (the key).
                        log.trace("filter={}", filter);
                        Map<Long, InstanceEventInfo> instanceEventInfoById = new TreeMap<>();
                        List<PipelineInstance> instances = pipelineInstanceCrud.retrieve(filter);
                        for (PipelineInstance instance : instances) {
                            instanceEventInfoById.putIfAbsent(instance.getId(),
                                new InstanceEventInfo(instance, null));
                        }
                        for (ZiggyEvent ziggyEvent : ziggyEventCrud.retrieve(instances)) {
                            instanceEventInfoById.computeIfPresent(
                                ziggyEvent.getPipelineInstanceId(),
                                (k, v) -> new InstanceEventInfo(v.getPipelineInstance(),
                                    ziggyEvent));
                        }

                        // Use the sorted map to create a sorted list of container objects.
                        instanceEventInfoList = new ArrayList<>(instanceEventInfoById.values());

                        // TODO Delete once comfortable that the content is the same
                        for (InstanceEventInfo instanceEventInfo : instanceEventInfoList) {
                            log.trace("pipeline name: instance={}, event={}",
                                instanceEventInfo.getPipelineInstance()
                                    .getPipelineDefinition()
                                    .getName(),
                                instanceEventInfo.getZiggyEvent() != null
                                    ? instanceEventInfo.getZiggyEvent().getPipelineName()
                                    : "null");
                            log.trace("date: instance={}, event={}",
                                instanceEventInfo.getPipelineInstance().getStartProcessingTime(),
                                instanceEventInfo.getZiggyEvent() != null
                                    ? instanceEventInfo.getZiggyEvent().getEventTime()
                                    : "null");
                        }
                    } catch (ConsoleSecurityException ignore) {
                    }
                    return null;
                }

                @Override
                protected void done() {
                    fireTableDataChanged();
                }
            };
            swingWorker.execute();
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
         * sorted ascending, which is the expected behavior of the PipelineInstanceCrudProxy
         * retriever.
         *
         * @return State of instance with max ID number.
         */
        public State getStateOfInstanceWithMaxid() {
            if (instanceEventInfoList.isEmpty()) {
                return State.COMPLETED;
            }
            return instanceEventInfoList.get(instanceEventInfoList.size() - 1)
                .getPipelineInstance()
                .getState();
        }

        @Override
        public int getRowCount() {
            validityCheck();
            return instanceEventInfoList.size();
        }

        @Override
        public int getColumnCount() {
            validityCheck();
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            validityCheck();
            InstanceEventInfo instanceEventInfo = instanceEventInfoList.get(rowIndex);
            PipelineInstance pipelineInstance = instanceEventInfo.getPipelineInstance();
            ZiggyEvent ziggyEvent = instanceEventInfo.getZiggyEvent();

            return switch (columnIndex) {
                case 0 -> pipelineInstance.getId();
                case 1 -> pipelineInstance.getPipelineDefinition().getName() + ": "
                    + pipelineInstance.getName();
                case 2 -> ziggyEvent != null ? ziggyEvent.getEventHandlerName() : "-";
                case 3 -> ziggyEvent != null ? ziggyEvent.getEventTime()
                    : pipelineInstance.getStartProcessingTime();
                case 4 -> pipelineInstance.getState().toString();
                case 5 -> pipelineInstance.elapsedTime();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public String getColumnName(int column) {
            validityCheck();
            return COLUMN_NAMES[column];
        }

        @Override
        public InstanceEventInfo getContentAtRow(int row) {
            validityCheck();
            return instanceEventInfoList.get(row);
        }

        @Override
        public Class<InstanceEventInfo> tableModelContentClass() {
            return InstanceEventInfo.class;
        }
    }

    private static class InstanceEventInfo {
        private PipelineInstance pipelineInstance;
        private ZiggyEvent ziggyEvent;

        public InstanceEventInfo(PipelineInstance pipelineInstance, ZiggyEvent ziggyEvent) {
            this.pipelineInstance = pipelineInstance;
            this.ziggyEvent = ziggyEvent;
        }

        public PipelineInstance getPipelineInstance() {
            return pipelineInstance;
        }

        public ZiggyEvent getZiggyEvent() {
            return ziggyEvent;
        }
    }
}
