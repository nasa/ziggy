package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.Desktop;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.PerformanceReport;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceFilter;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.DeleteTasksRequest;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.proxy.PipelineExecutorProxy;
import gov.nasa.ziggy.ui.proxy.PipelineTaskCrudProxy;
import gov.nasa.ziggy.ui.proxy.ProcessingSummaryOpsProxy;

/**
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public class OpsInstancesPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(OpsInstancesPanel.class);

    private JMenuItem restartMenuItem;
    private JMenuItem detailsMenuItem;
    private JMenuItem statisticsMenuItem;
    private JMenuItem costEstimateMenuItem;
    private JMenuItem performanceMenuItem;
    private JPopupMenu tasksPopupMenu;
    private JPopupMenu instancesPopupMenu;
    private JMenuItem alertsMenuItem;
    private TaskStatusSummaryPanel taskStatusSummaryPanel;
    private JPanel tasksTablePanel;
    private JMenuItem restartAllFailedTasksMenuItem;
    private JMenuItem deleteAllPbsJobsMenuItem;
    private JMenuItem deleteFromPbsMenuItem;

    private InstancesControlPanel instancesControlPanel;
    private JScrollPane instancesTableScrollPane;
    private JScrollPane tasksTableScrollPane;
    private JPanel tasksPanel;
    private JPanel instancesPanel;
    private ZTable tasksTable;
    private ZTable instancesTable;
    private TasksControlPanel tasksControlPanel;

    private TasksTableModel tasksTableModel;
    private InstancesTableModel instancesTableModel;
    private final PipelineInstanceFilter filter;

    private OpsInstancePanelAutoRefresh autoRefresh;

    private int selectedInstanceModelRow = -1;

    private JMenuItem instanceDetailsMenuItem;

    private JMenuItem retrieveLogInfoMenuItem;

    // Indices in the tasks table of the selected tasks. Not to be confused with the
    // task IDs of the selected tasks (see below).
    protected List<Integer> selectedTasksIndices = new ArrayList<>();

    private static boolean instancesRemaining = true;

    // States of the instances and tasks tables, specifically the instance ID of the selected
    // instance (if any), and the task IDs of the selected tasks (if any).
    private long instanceId = -1;
    private List<Long> taskIds = new ArrayList<>();

    // =======================================================================================
    // Instances panel and items on that side of the GUI

    private JPanel getInstancesPanel() {
        log.debug("getInstancesPanel() - start");

        if (instancesPanel == null) {
            instancesPanel = new JPanel();
            BorderLayout instancesPanelLayout = new BorderLayout();
            instancesPanel.setLayout(instancesPanelLayout);
            instancesPanel.setBorder(BorderFactory.createTitledBorder("Pipeline Instances"));
            instancesPanel.add(getInstancesControlPanel(), BorderLayout.NORTH);
            instancesPanel.add(getInstancesTableScrollPane(), BorderLayout.CENTER);
        }

        log.debug("getInstancesPanel() - end");
        return instancesPanel;
    }

    public void refreshInstanceNowPressed() {

        log.debug("refreshInstanceNowPressed() - start");

        try {
            // For reasons I have yet to fathom, refreshing the instances always clears any
            // selection of rows from the tasks. To avoid that fate, capture the current selection.
            int taskRow = tasksTable.getSelectionModel().getMinSelectionIndex();

            instancesTableModel.loadFromDatabase();

            // Restore the current selection.
            tasksTable.getSelectionModel().setSelectionInterval(taskRow, taskRow);
        } catch (PipelineException e) {
            ZiggyGuiConsole.showError(e);
        }

        log.debug("refreshInstanceNowPressed() - end");
    }

    private InstancesControlPanel getInstancesControlPanel() {

        log.debug("getInstancesControlPanel() - start");

        if (instancesControlPanel == null) {
            instancesControlPanel = new InstancesControlPanel(filter);
            instancesControlPanel.setListener(this);
        }

        log.debug("getInstancesControlPanel() - end");
        return instancesControlPanel;
    }

    private ZTable getInstancesTable() {
        log.debug("getInstancesTable() - start");

        if (instancesTable == null) {
            instancesTableModel = new InstancesTableModel(filter);
            instancesTableModel.register();

            instancesTable = new ZTable();
            instancesTable.setRowShadingEnabled(true);
            instancesTable.getSelectionModel()
                .setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            instancesTable.setModel(instancesTableModel);
            instancesTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            setComponentPopupMenu(instancesTable, getInstancesPopupMenu());

            ListSelectionModel rowSM = instancesTable.getSelectionModel();
            rowSM.addListSelectionListener(event -> {

                try {
                    // Ignore extra messages.
                    if (event.getValueIsAdjusting()) {
                        return;
                    }

                    ListSelectionModel lsm = instancesTable.getSelectionModel();
                    if (lsm.isSelectionEmpty()) {

                        // If the selected instance from before the update is still in the
                        // table, then select it again
                        int instanceModelIndex = instancesTableModel
                            .getModelIndexOfInstance(instanceId);

                        // If there isn't a selected pipeline instance, or if the selected
                        // instance is no longer in the table, then the tasks table also has to
                        // be updated
                        if (instanceModelIndex == -1) {
                            log.debug("Setting tasks table model pipeline instance to null");
                            tasksTableModel.setPipelineInstance(null);
                            tasksTableModel.loadFromDatabase(false);
                            instanceId = -1;
                            taskIds.clear();
                        } else {
                            log.debug("Setting instance ID to " + instanceId);
                            int rowIndex = instancesTable.convertRowIndexToView(instanceModelIndex);
                            instancesTable.getSelectionModel()
                                .setSelectionInterval(rowIndex, rowIndex);
                        }
                    } else {

                        // This is the code block that executes if the user selects a new instance
                        // in the table
                        int selectedRow = lsm.getMinSelectionIndex();
                        int modelIndex = instancesTable.convertRowIndexToModel(selectedRow);
                        log.debug("selected row in instances table: " + selectedRow);
                        PipelineInstance selectedInstance = instancesTableModel
                            .getInstanceAt(modelIndex);
                        instanceId = selectedInstance.getId();
                        boolean genuinelyNewInstance = tasksTableModel
                            .updatePipelineInstance(selectedInstance);
                        tasksTableModel.loadFromDatabase(false);

                        // If the change that caused this listener to act is that a genuinely
                        // new instance was selected, clear the tasks table selections as they
                        // are no longer valid.
                        if (genuinelyNewInstance) {
                            log.debug(
                                "Genuninely new instance so clearing tasks table selection model");
                            tasksTable.getSelectionModel().clearSelection();
                            taskIds.clear();
                        }
                    }
                    taskStatusSummaryPanel.update(tasksTableModel);

                } catch (Exception e) {
                    ZiggyGuiConsole.showError(e);
                }

            });
        }

        log.debug("getInstancesTable() - end");
        return instancesTable;
    }

    private JScrollPane getInstancesTableScrollPane() {
        log.debug("getInstancesTableScrollPane() - start");

        if (instancesTableScrollPane == null) {
            instancesTableScrollPane = new JScrollPane();
            instancesTableScrollPane.setViewportView(getInstancesTable());
        }

        log.debug("getInstancesTableScrollPane() - end");
        return instancesTableScrollPane;
    }

    // =======================================================================================
    // Tasks panel and items on that side of the GUI

    private JPanel getTasksPanel() {
        log.debug("getInstanceNodesPanel() - start");

        if (tasksPanel == null) {
            tasksPanel = new JPanel();
            BorderLayout instanceNodesPanelLayout = new BorderLayout();
            tasksPanel.setLayout(instanceNodesPanelLayout);
            tasksPanel.setBorder(BorderFactory.createTitledBorder("Pipeline Tasks"));
            tasksPanel.add(getTasksControlPanel(), BorderLayout.NORTH);
            tasksPanel.add(getTasksTablePanel(), BorderLayout.CENTER);
        }

        log.debug("getInstanceNodesPanel() - end");
        return tasksPanel;
    }

    private TasksControlPanel getTasksControlPanel() {
        log.debug("getInstanceNodesControlPanel() - start");

        if (tasksControlPanel == null) {
            tasksControlPanel = new TasksControlPanel();
            tasksControlPanel.setListener(this);
        }

        log.debug("getInstanceNodesControlPanel() - end");
        return tasksControlPanel;
    }

    void refreshTaskNowPressed() {
        try {
            ListSelectionModel lsm = instancesTable.getSelectionModel();
            if (!lsm.isSelectionEmpty()) {
                int selectedRow = lsm.getMinSelectionIndex();
                int selectedModelRow = instancesTable.convertRowIndexToModel(selectedRow);
                tasksTableModel
                    .setPipelineInstance(instancesTableModel.getInstanceAt(selectedModelRow));
                tasksTableModel.loadFromDatabase();
                taskStatusSummaryPanel.update(tasksTableModel);
            }
            log.debug("End of try block task refresh");
        } catch (PipelineException e) {
            ZiggyGuiConsole.showError(e);
        }
    }

    private PipelineTask getSelectedTask() {
        if (selectedTasksIndices.size() != 1) {
            log.debug("Only one task may be selected!");
            return null;
        }

        int selectedIndex = selectedTasksIndices.get(0);
        int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
        return tasksTableModel.getPipelineTaskForRow(selectedModelRow);
    }

    private ZTable getTasksTable() {
        log.debug("getInstanceNodesTable() - start");

        if (tasksTable == null) {
            tasksTableModel = new TasksTableModel();
            tasksTableModel.register();

            tasksTable = new ZTable();
            tasksTable.setRowShadingEnabled(true);
            ListSelectionModel selectionModel = tasksTable.getSelectionModel();
            selectionModel.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
            selectionModel.addListSelectionListener(e -> {
                ListSelectionModel lsm = (ListSelectionModel) e.getSource();

                if (!lsm.getValueIsAdjusting()) {
                    selectedTasksIndices = new ArrayList<>();

                    // If the tasks table has no selected rows, but the taskIds list is not
                    // empty, it means that the tasks table got reset and now we need to
                    // re-select the rows that had been selected prior to the reset. Note
                    // that if the task IDs that had been selected before aren't even
                    // in the current contents of the table, then the task IDs that had been
                    // selected before are no longer valid and need to be cleared.
                    if (lsm.isSelectionEmpty()) {
                        if (!taskIds.isEmpty()) {
                            List<Integer> taskModelIds = tasksTableModel
                                .getModelIndicesOfTasks(taskIds);
                            log.debug("Task model IDs: " + taskModelIds.toString());
                            if (taskModelIds.isEmpty()) {
                                taskIds.clear();
                            }
                            for (int taskModelId : taskModelIds) {
                                int rowId = tasksTable.convertRowIndexToView(taskModelId);
                                log.debug(
                                    "Row ID for task model ID " + taskModelId + " == " + rowId);
                                lsm.addSelectionInterval(rowId, rowId);
                            }
                        }
                    } else {

                        // Alternately, if the tasks table has selected rows, then
                        // we need to capture the corresponding task IDs (and the indices
                        // of those IDs in the table model) so that the next time the table's
                        // selections are cleared we can re-select the appropriate tasks.
                        int selectedTasksMinIndex = lsm.getMinSelectionIndex();
                        int selectedTasksMaxIndex = lsm.getMaxSelectionIndex();
                        log.debug("Selected tasks min, max index: " + selectedTasksMinIndex + ", "
                            + selectedTasksMaxIndex);
                        int count = 0;
                        taskIds.clear();
                        for (int i = selectedTasksMinIndex; i <= selectedTasksMaxIndex; i++) {
                            if (lsm.isSelectedIndex(i)) {
                                log.debug("selected index: " + i);
                                selectedTasksIndices.add(i);
                                int rowModel = tasksTable.convertRowIndexToModel(i);
                                long taskId = tasksTableModel.getPipelineTaskForRow(rowModel)
                                    .getId();
                                log.debug("Row model for index == " + rowModel
                                    + ", corresponds to task ID " + taskId);
                                taskIds.add(taskId);
                                count++;
                            }
                        }
                        log.debug("Num selected: " + count);
                    }
                }
            });

            tasksTable.setModel(tasksTableModel);

            // setComponentPopupMenu(tasksTable, getTasksPopupMenu());
            getTasksPopupMenu();
            tasksTable.addMouseListener(new java.awt.event.MouseAdapter() {
                @Override
                public void mousePressed(java.awt.event.MouseEvent e) {
                    int numTasksSelected = selectedTasksIndices.size();
                    if (numTasksSelected == 1) {
                        detailsMenuItem.setEnabled(true);
                        restartMenuItem.setEnabled(true);
                        retrieveLogInfoMenuItem.setEnabled(true);

                    } else {
                        detailsMenuItem.setEnabled(false);
                        restartMenuItem.setEnabled(true);
                        retrieveLogInfoMenuItem.setEnabled(false);
                    }

                    if (numTasksSelected > 0 && e.isPopupTrigger()) {
                        tasksPopupMenu.show(tasksTable, e.getX(), e.getY());
                    }
                }

                @Override
                public void mouseReleased(java.awt.event.MouseEvent e) {
                }
            });
        }

        log.debug("getInstanceNodesTable() - end");
        return tasksTable;
    }

    private JScrollPane getTasksTableScrollPane() {
        log.debug("getInstanceNodesTableScrollPane() - start");

        if (tasksTableScrollPane == null) {
            tasksTableScrollPane = new JScrollPane();
            tasksTableScrollPane.setViewportView(getTasksTable());
        }

        log.debug("getInstanceNodesTableScrollPane() - end");
        return tasksTableScrollPane;
    }

    private TaskStatusSummaryPanel getTaskStatusSummaryPanel() {
        if (taskStatusSummaryPanel == null) {
            taskStatusSummaryPanel = new TaskStatusSummaryPanel();
        }
        return taskStatusSummaryPanel;
    }

    private JPanel getTasksTablePanel() {
        if (tasksTablePanel == null) {
            tasksTablePanel = new JPanel();
            BorderLayout jPanel1Layout = new BorderLayout();
            tasksTablePanel.setLayout(jPanel1Layout);
            tasksTablePanel.add(getTasksTableScrollPane(), BorderLayout.CENTER);
            tasksTablePanel.add(getTaskStatusSummaryPanel(), BorderLayout.NORTH);
        }
        return tasksTablePanel;
    }

    // =======================================================================================
    // Instances Popup menu

    private JPopupMenu getInstancesPopupMenu() {
        if (instancesPopupMenu == null) {
            instancesPopupMenu = new JPopupMenu();
            instancesPopupMenu.add(getInstanceDetailsMenuItem());
            instancesPopupMenu.add(getPerformanceMenuItem());
            instancesPopupMenu.add(getAlertsMenuItem());
            instancesPopupMenu.add(getStatisticsMenuItem());
            instancesPopupMenu.add(getCostEstimateMenuItem());
            instancesPopupMenu.addSeparator();
            instancesPopupMenu.add(getRestartAllFailedTasksMenuItem());
            instancesPopupMenu.addSeparator();
            instancesPopupMenu.add(getDeleteAllTasksMenuItem());

        }
        return instancesPopupMenu;
    }

    private JMenuItem getPerformanceMenuItem() {
        if (performanceMenuItem == null) {
            performanceMenuItem = new JMenuItem();
            performanceMenuItem.setText("Performance report");
            performanceMenuItem.addActionListener(evt -> {
                PipelineInstance selectedInstance = instancesTableModel
                    .getInstanceAt(selectedInstanceModelRow);
                PerformanceReport report = new PerformanceReport(selectedInstance.getId(),
                    DirectoryProperties.taskDataDir().toFile(), null);
                ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
                    Path r = null;
                    try {
                        r = report.generateReport();
                        Desktop.getDesktop().open(r.toFile());
                    } catch (Exception e) {
                        MessageUtil.showError(this, e);
                    }
                    return null;
                });
            });
        }
        return performanceMenuItem;
    }

    private JMenuItem getCostEstimateMenuItem() {
        if (costEstimateMenuItem == null) {
            costEstimateMenuItem = new JMenuItem();
            costEstimateMenuItem.setText("Cost estimate");
            costEstimateMenuItem.addActionListener(evt -> {
                PipelineInstance selectedInstance = instancesTableModel
                    .getInstanceAt(selectedInstanceModelRow);
                ZiggyGuiConsole.newInstanceCostEstimateDialog(selectedInstance);
            });
        }
        return costEstimateMenuItem;
    }

    private JMenuItem getInstanceDetailsMenuItem() {
        if (instanceDetailsMenuItem == null) {
            instanceDetailsMenuItem = new JMenuItem();
            instanceDetailsMenuItem.setText("Details...");
            instanceDetailsMenuItem
                .addActionListener(evt -> instanceDetailsMenuItemActionPerformed());
        }
        return instanceDetailsMenuItem;
    }

    private void instanceDetailsMenuItemActionPerformed() {
        PipelineInstance selectedInstance = instancesTableModel
            .getInstanceAt(selectedInstanceModelRow);

        log.debug("selected instance id = " + selectedInstance.getId());

        if (selectedInstance != null) {
            InstanceDetailsDialog instanceDetailsDialog = ZiggyGuiConsole
                .newInstanceDetailsDialog(selectedInstance);
            instanceDetailsDialog.setVisible(true);
        }
    }

    private JMenuItem getAlertsMenuItem() {
        if (alertsMenuItem == null) {
            alertsMenuItem = new JMenuItem();
            alertsMenuItem.setText("Alerts...");
            alertsMenuItem.addActionListener(this::alertsMenuItemActionPerformed);
        }
        return alertsMenuItem;
    }

    private void alertsMenuItemActionPerformed(ActionEvent evt) {
        log.debug("alertsMenuItem.actionPerformed, event=" + evt);

        try {
            PipelineInstance selectedInstance = instancesTableModel
                .getInstanceAt(selectedInstanceModelRow);
            long id = selectedInstance.getId();

            log.debug("selected instance id = " + id);

            ZiggyGuiConsole.showAlertLogDialog(id);
        } catch (Exception e) {
            ZiggyGuiConsole.showMessageDialog("caught e = " + e, "Failed to Restart Tasks",
                JOptionPane.ERROR_MESSAGE);
        }
    }

    private JMenuItem getStatisticsMenuItem() {
        if (statisticsMenuItem == null) {
            statisticsMenuItem = new JMenuItem();
            statisticsMenuItem.setText("Performance Statistics...");
            statisticsMenuItem.addActionListener(this::statisticsMenuItemActionPerformed);
        }
        return statisticsMenuItem;
    }

    private void statisticsMenuItemActionPerformed(ActionEvent evt) {
        log.debug("statisticsMenuItem.actionPerformed, event=" + evt);

        try {
            PipelineInstance selectedInstance = instancesTableModel
                .getInstanceAt(selectedInstanceModelRow);

            log.debug("selected instance id = " + selectedInstance.getId());

            ZiggyGuiConsole.showInstanceStatsDialog(selectedInstance);
        } catch (Exception e) {
            ZiggyGuiConsole.showMessageDialog("caught e = " + e,
                "Failed to retrieve performance stats", JOptionPane.ERROR_MESSAGE);
        }
    }

    private JMenuItem getRestartAllFailedTasksMenuItem() {
        if (restartAllFailedTasksMenuItem == null) {
            restartAllFailedTasksMenuItem = new JMenuItem();
            restartAllFailedTasksMenuItem.setText("Restart all failed tasks...");
            restartAllFailedTasksMenuItem
                .addActionListener(this::restartAllFailedTasksMenuItemActionPerformed);
        }
        return restartAllFailedTasksMenuItem;
    }

    private void restartAllFailedTasksMenuItemActionPerformed(ActionEvent evt) {
        log.debug("restartAllFailedTasksMenuItem.actionPerformed, event=" + evt);

        try {
            PipelineInstance selectedInstance = instancesTableModel
                .getInstanceAt(selectedInstanceModelRow);

            log.debug("selected instance id = " + selectedInstance.getId());

            PipelineTaskCrudProxy taskCrud = new PipelineTaskCrudProxy();
            List<PipelineTask> failedTasks = taskCrud.retrieveAll(selectedInstance,
                PipelineTask.State.ERROR);

            ProcessingSummaryOpsProxy attrOps = new ProcessingSummaryOpsProxy();
            Map<Long, ProcessingSummary> taskAttrs = attrOps
                .retrieveByInstanceId(selectedInstance.getId());
            List<Long> tasksWithFailedSubTasks = getTaskIdsWithFailedSubtasks(taskAttrs);
            if (tasksWithFailedSubTasks != null && !tasksWithFailedSubTasks.isEmpty()) {
                failedTasks.addAll(taskCrud.retrieveAll(tasksWithFailedSubTasks));
            }

            if (failedTasks.size() > 0) {
                RunMode mode = ZiggyGuiConsole.restartTasks(failedTasks, taskAttrs);

                if (mode != null) {
                    PipelineExecutorProxy pipelineExecutor = new PipelineExecutorProxy();
                    // TODO: let the use decide whether doTransitionOnly
                    // should be true

                    log.debug("Restarting " + failedTasks.size() + " failed tasks");

                    pipelineExecutor.restartTasks(failedTasks, mode);
                }
            } else {
                ZiggyGuiConsole.showMessageDialog(
                    "No failed tasks found for the selected instance!", "Restart Tasks",
                    JOptionPane.ERROR_MESSAGE);
            }
        } catch (Exception e) {
            log.error("Failed to restart tasks, caught: " + e, e);
            ZiggyGuiConsole.showMessageDialog("caught e = " + e, "Failed to Restart Tasks",
                JOptionPane.ERROR_MESSAGE);
        }
    }

    private JMenuItem getDeleteAllTasksMenuItem() {
        if (deleteAllPbsJobsMenuItem == null) {
            deleteAllPbsJobsMenuItem = new JMenuItem();
            deleteAllPbsJobsMenuItem.setText("Delete all tasks");
            deleteAllPbsJobsMenuItem.addActionListener(this::deleteAllTasksMenuItemActionPerformed);
        }
        return deleteAllPbsJobsMenuItem;
    }

    private void deleteAllTasksMenuItemActionPerformed(ActionEvent evt) {
        log.debug("deleteAllPbsJobsMenuItem.actionPerformed event = " + evt);

        try {
            PipelineInstance selectedInstance = instancesTableModel
                .getInstanceAt(selectedInstanceModelRow);

            log.debug("selected instance id = " + selectedInstance.getId());

            PipelineTaskCrudProxy taskCrud = new PipelineTaskCrudProxy();

            // collect all the processing pipeline tasks; the QdelCommandManager will
            // do the work of identifying which ones are remote execution and which are not
            List<PipelineTask> processingTasks = taskCrud.retrieveAll(selectedInstance,
                PipelineTask.State.PROCESSING);
            processingTasks
                .addAll(taskCrud.retrieveAll(selectedInstance, PipelineTask.State.SUBMITTED));

            if (processingTasks.size() > 0) {

                // are you sure you want to do this?
                int confirm = ZiggyGuiConsole.showConfirmDialog("Confirm delete tasks?",
                    "Confirm Delete", JOptionPane.OK_CANCEL_OPTION);
                if (confirm == JOptionPane.OK_OPTION) {

                    // Delete any local-execution tasks.
                    Boolean allTasksDeleted = (Boolean) UiCommunicator
                        .send(new DeleteTasksRequest(processingTasks));

                    // If any tasks remain, try to delete them from PBS.
                    if (!allTasksDeleted) {
                        QueueCommandManager queueCommandManager = QueueCommandManager.newInstance();
                        queueCommandManager.deleteJobsForPipelineTasks(processingTasks);
                    }

                    // Now send a message to the worker to get it to delete the tasks that
                    // execute locally.
                } else {

                    // well, never mind then.
                    ZiggyGuiConsole.showMessageDialog("Delete tasks canceled", "Delete tasks",
                        JOptionPane.OK_OPTION);
                }
            } else {
                ZiggyGuiConsole.showMessageDialog(
                    "No running tasks found for the selected instance!", "Delete tasks",
                    JOptionPane.ERROR_MESSAGE);
            }
        } catch (Exception e) {

            // If something above errored, we want to go out of our way to notify
            // the operator
            log.error("Failed to delete jobs, caught: " + e, e);
            ZiggyGuiConsole.showMessageDialog("caught e = " + e, "Failed to delete jobs",
                JOptionPane.ERROR_MESSAGE);

        }
    }

    // =======================================================================================
    // Tasks Popup menu

    private JPopupMenu getTasksPopupMenu() {
        if (tasksPopupMenu == null) {
            tasksPopupMenu = new JPopupMenu();
            tasksPopupMenu.add(getDetailsMenuItem());
            tasksPopupMenu.add(getRestartMenuItem());
            tasksPopupMenu.add(getRetrieveLogInfoMenuItem());
            tasksPopupMenu.addSeparator();
            tasksPopupMenu.add(getDeleteTasksMenuItem());
        }
        return tasksPopupMenu;
    }

    private JMenuItem getDetailsMenuItem() {
        if (detailsMenuItem == null) {
            detailsMenuItem = new JMenuItem();
            detailsMenuItem.setText("Task Details...");
            detailsMenuItem.addActionListener(evt -> detailsMenuItemActionPerformed());
        }
        return detailsMenuItem;
    }

    private void detailsMenuItemActionPerformed() {
        PipelineTask selectedTask = getSelectedTask();

        if (selectedTask != null) {
            try {
                ZiggyGuiConsole.showTaskInfoDialog(selectedTask);
            } catch (PipelineException e) {
                ZiggyGuiConsole.showError(e);
            }
        }
    }

    private JMenuItem getRestartMenuItem() {
        if (restartMenuItem == null) {
            restartMenuItem = new JMenuItem();
            restartMenuItem.setText("Restart");
            restartMenuItem.addActionListener(this::restartMenuItemActionPerformed);
        }
        return restartMenuItem;
    }

    private void restartMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "restartMenuItemActionPerformed(ActionEvent) - restartMenuItem.actionPerformed, event="
                + evt);

        PipelineInstance selectedInstance = instancesTableModel
            .getInstanceAt(selectedInstanceModelRow);

        try {
            if (selectedTasksIndices.isEmpty()) {
                log.debug("No tasks selected");
                return;
            }

            ProcessingSummaryOpsProxy attrOps = new ProcessingSummaryOpsProxy();
            Map<Long, ProcessingSummary> taskAttrs = attrOps
                .retrieveByInstanceId(selectedInstance.getId());
            List<PipelineTask> failedTasks = new ArrayList<>();

            for (int selectedIndex : selectedTasksIndices) {
                int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
                PipelineTask task = tasksTableModel.getPipelineTaskForRow(selectedModelRow);
                ProcessingSummary attributes = taskAttrs.get(task.getId());
                if (task.getState() == PipelineTask.State.ERROR
                    || attributes.getProcessingState() == ProcessingState.COMPLETE
                        && attributes.getFailedSubtaskCount() > 0) {
                    failedTasks.add(task);
                }
            }

            if (failedTasks.isEmpty()) {
                log.debug("Selected tasks contain no ERROR tasks");
                ZiggyGuiConsole.showMessageDialog(
                    "None of the selected tasks are in the ERROR state.", "Restart Tasks",
                    JOptionPane.ERROR_MESSAGE);
                return;
            }

            RunMode restartMode = ZiggyGuiConsole.restartTasks(failedTasks, taskAttrs);

            if (restartMode != null) {
                PipelineExecutorProxy pipelineExecutor = new PipelineExecutorProxy();
                // TODO: let the use decide whether doTransitionOnly
                // should be true

                log.debug("Restarting " + failedTasks.size() + " failed tasks");

                pipelineExecutor.restartTasks(failedTasks, restartMode);
            }
        } catch (Exception e) {
            ZiggyGuiConsole.showMessageDialog("caught e = " + e, "Failed to restart Tasks",
                JOptionPane.ERROR_MESSAGE);
        }
    }

    private JMenuItem getRetrieveLogInfoMenuItem() {
        if (retrieveLogInfoMenuItem == null) {
            retrieveLogInfoMenuItem = new JMenuItem();
            retrieveLogInfoMenuItem.setText("List task logs");
            retrieveLogInfoMenuItem
                .addActionListener(evt -> retrieveLogInfoMenuItemActionPerformed());
        }
        return retrieveLogInfoMenuItem;
    }

    private void retrieveLogInfoMenuItemActionPerformed() {
        PipelineTask selectedTask = getSelectedTask();

        if (selectedTask != null) {
            try {
                ZiggyGuiConsole.newTaskLogInformationDialog(selectedTask);
            } catch (PipelineException e) {
                ZiggyGuiConsole.showError(e);
            }
        }

    }

    private JMenuItem getDeleteTasksMenuItem() {
        if (deleteFromPbsMenuItem == null) {
            deleteFromPbsMenuItem = new JMenuItem();
            deleteFromPbsMenuItem.setText("Delete tasks");
            deleteFromPbsMenuItem.addActionListener(this::deleteTasksMenuItemActionPerformed);
        }
        return deleteFromPbsMenuItem;
    }

    private void deleteTasksMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "deleteFromPbsMenuItemActionPerformed(ActionEvent) - deleteFromPbsMenuItem.actionPerformed, event="
                + evt);

        try {
            if (selectedTasksIndices.isEmpty()) {
                log.debug("No tasks selected");
                return;
            }
            List<PipelineTask> processingTasks = new ArrayList<>();

            // make sure that the selected tasks are PROCESSING or SUBMITTED
            for (int selectedIndex : selectedTasksIndices) {
                int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
                PipelineTask task = tasksTableModel.getPipelineTaskForRow(selectedModelRow);
                if (task.getState() == PipelineTask.State.PROCESSING
                    || task.getState() == PipelineTask.State.SUBMITTED) {
                    processingTasks.add(task);
                }
            }

            if (processingTasks.isEmpty()) {
                log.debug("Selected tasks contain no PROCESSING tasks");
                ZiggyGuiConsole.showMessageDialog(
                    "None of the selected tasks are in the PROCESSING state.", "Delete tasks",
                    JOptionPane.ERROR_MESSAGE);
                return;
            }

            // are you sure you want to do this?
            int confirm = ZiggyGuiConsole.showConfirmDialog("Confirm delete tasks?",
                "Confirm Delete", JOptionPane.OK_CANCEL_OPTION);
            if (confirm == JOptionPane.OK_OPTION) {

                // Delete any tasks running locally.
                Boolean returnBoolean = (Boolean) UiCommunicator
                    .send(new DeleteTasksRequest(processingTasks));

                // If some tasks were not found in local execution, try deleting from PBS.
                if (!returnBoolean) {
                    QueueCommandManager.newInstance().deleteJobsForPipelineTasks(processingTasks);
                }

            } else {

                // well, never mind then.
                ZiggyGuiConsole.showMessageDialog("Delete tasks canceled", "Delete tasks",
                    JOptionPane.OK_OPTION);
            }

        } catch (Exception e) {

            // If something above errored, we want to go out of our way to notify
            // the operator
            log.error("Failed to delete jobs, caught: " + e, e);
            ZiggyGuiConsole.showMessageDialog("caught e = " + e, "Failed to delete jobs",
                JOptionPane.ERROR_MESSAGE);
        }

    }

    // =======================================================================================
    // Utilities

    private static List<Long> getTaskIdsWithFailedSubtasks(Map<Long, ProcessingSummary> taskAttrs) {
        List<Long> taskIds = new ArrayList<>();
        Set<Long> allTaskIds = taskAttrs.keySet();
        for (Long taskId : allTaskIds) {
            ProcessingSummary taskAttributes = taskAttrs.get(taskId);
            if (taskAttributes.getProcessingState() == ProcessingState.COMPLETE
                && taskAttributes.getFailedSubtaskCount() > 0) {
                taskIds.add(taskId);
            }

        }
        return taskIds;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
                ZTable table = (ZTable) parent;
                int selectedTableRow = table.rowAtPoint(e.getPoint());
                // windows bug? works ok on Linux/gtk. Here's a workaround:
                if (selectedTableRow == -1) {
                    selectedTableRow = table.getSelectedRow();
                }
                selectedInstanceModelRow = table.convertRowIndexToModel(selectedTableRow);
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });
    }

    public static void setInstancesRemaining() {
        instancesRemaining = true;
    }

    public static void clearInstancesRemaining() {
        instancesRemaining = false;
    }

    public static boolean getInstancesRemaining() {
        return instancesRemaining;
    }

    // =======================================================================================
    // Top level

    public OpsInstancesPanel() {
        filter = new PipelineInstanceFilter();
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            GridBagLayout thisLayout = new GridBagLayout();
            thisLayout.columnWeights = new double[] { 0.1, 0.1 };
            thisLayout.columnWidths = new int[] { 7, 7 };
            thisLayout.rowWeights = new double[] { 0.1 };
            thisLayout.rowHeights = new int[] { 7 };
            setLayout(thisLayout);
            setPreferredSize(new java.awt.Dimension(1000, 700));
            this.add(getInstancesPanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            this.add(getTasksPanel(), new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            autoRefresh = new OpsInstancePanelAutoRefresh(instancesTableModel, tasksTableModel,
                taskStatusSummaryPanel);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("initGUI() - end");
    }

    public void startAutoRefresh() {
        autoRefresh.start();
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        frame.getContentPane().add(new OpsInstancesPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);

        log.debug("main(String[]) - end");
    }

}
