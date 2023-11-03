package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.MENU_SEPARATOR;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createPopupMenu;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.messages.DisplayTasksForInstanceMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.proxy.PipelineInstanceCrudProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * The right hand side subpanel to the operations > instances panel. This panel displays the table
 * of tasks for a given instance, plus the subtask "scoreboard."
 *
 * @author PT
 * @author Bill Wohler
 */
public class TasksPanel extends JPanel {

    private static final long serialVersionUID = 20230817L;

    private InstancesTasksPanel instancesTasksPanel;
    private TaskStatusSummaryPanel taskStatusSummaryPanel;
    private ZiggyTable<PipelineTask> tasksTable;
    private TasksTableModel tasksTableModel;

    private Map<Integer, Long> selectedTasks = new HashMap<>();

    public TasksPanel(InstancesTasksPanel instancesTasksPanel) {
        this.instancesTasksPanel = instancesTasksPanel;
        buildComponent();

        // Subscribe to the messages that indicate that something has potentially changed
        // with the instance panel's instance selection.
        ZiggyMessenger.subscribe(DisplayTasksForInstanceMessage.class, message -> {
            long instanceId = message.getPipelineInstanceId();

            if (message.isReselect() && instanceId == -1) {

                // If the message comes from a reselect action, we only need to do anything
                // if the reselect resulted in no selected instance.
                tasksTableModel.setPipelineInstance(null);
                tasksTableModel.loadFromDatabase(false);
                clearSelectedTasks();
            } else if (!message.isReselect()) {

                // If the message comes from a user clicking on a row in the table, we
                // need to see whether this is a genuinely new instance, or if the user
                // just clicked on the instance that was already selected.
                PipelineInstance selectedInstance = new PipelineInstanceCrudProxy()
                    .retrieve(instanceId);
                boolean genuinelyNewInstance = tasksTableModel
                    .updatePipelineInstance(selectedInstance);
                tasksTableModel.loadFromDatabase(false);

                // If the change is that a genuinely new instance was selected, clear the tasks
                // table selections as they are no longer valid.
                if (genuinelyNewInstance) {
                    tasksTable.getTable().getSelectionModel().clearSelection();
                    clearSelectedTasks();
                }
            }

            // No matter what happened, update the summary display.
            updateSummaryPanel();
        });
    }

    private void buildComponent() {
        JLabel instance = boldLabel("Pipeline tasks", LabelType.HEADING1);

        taskStatusSummaryPanel = new TaskStatusSummaryPanel();

        tasksTableModel = new TasksTableModel();
        tasksTable = createTasksTable(tasksTableModel);
        JScrollPane tasksTableScrollPane = new JScrollPane(tasksTable.getTable());
        createTasksTablePopupMenu();

        GroupLayout layout = new GroupLayout(this);
        layout.setAutoCreateContainerGaps(true);
        setLayout(layout);

        layout.setHorizontalGroup(layout.createParallelGroup()
            .addComponent(instance)
            .addComponent(taskStatusSummaryPanel)
            .addComponent(tasksTableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(instance)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(taskStatusSummaryPanel, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
            .addComponent(tasksTableScrollPane));
    }

    private ZiggyTable<PipelineTask> createTasksTable(TasksTableModel tasksTableModel) {
        ZiggyTable<PipelineTask> tasksTable = new ZiggyTable<>(tasksTableModel);
        tasksTable.setWrapText(false);
        tasksTable.registerModel();

        ListSelectionModel selectionModel = tasksTable.getTable().getSelectionModel();
        selectionModel.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        selectionModel.addListSelectionListener(e -> {
            ListSelectionModel lsm = (ListSelectionModel) e.getSource();

            if (!lsm.getValueIsAdjusting()) {

                if (lsm.isSelectionEmpty()) {
                    reselectTaskRows(lsm);
                } else {
                    captureSelectedTaskRows(lsm);
                }
            }
        });

        return tasksTable;
    }

    /**
     * Repopulate the selected task rows after the table has been reset. If the table has been reset
     * because the user selected a different pipeline instance, then the tasks that were selected
     * are no longer present and the cache of selected rows / tasks needs to be cleared.
     */
    private void reselectTaskRows(ListSelectionModel lsm) {

        // If there were no selected tasks in the first place, return.
        if (selectedTasks.isEmpty()) {
            return;
        }

        // Find the model indices of the selected task IDs. Here we use a List of
        // task IDs so that the order of the task IDs and the model IDs match one another.
        List<Long> selectedTaskIds = new ArrayList<>(selectedTasks.values());
        List<Integer> taskModelIds = tasksTableModel.getModelIndicesOfTasks(selectedTaskIds);
        selectedTasks.clear();

        // If all the tasks that had been selected before are now gone from the table, return.
        if (taskModelIds.isEmpty()) {
            return;
        }

        // Convert the model indices to view indices and repopulate the Map.
        for (int listIndex = 0; listIndex < taskModelIds.size(); listIndex++) {
            int taskModelId = taskModelIds.get(listIndex);
            long taskId = selectedTaskIds.get(listIndex);
            int rowId = tasksTable.getTable().convertRowIndexToView(taskModelId);
            lsm.addSelectionInterval(rowId, rowId);
            selectedTasks.put(rowId, taskId);
        }
    }

    /**
     * Capture the selected task table rows and the corresponding task IDs.
     */
    private void captureSelectedTaskRows(ListSelectionModel lsm) {

        // Get the range of selected rows.
        int selectedTasksMinIndex = lsm.getMinSelectionIndex();
        int selectedTasksMaxIndex = lsm.getMaxSelectionIndex();

        // Clear the cache of selected rows / tasks.
        selectedTasks.clear();

        // Check each row in the range to see if it's selected; if so, capture the
        // task ID and the row index in the cache.
        for (int i = selectedTasksMinIndex; i <= selectedTasksMaxIndex; i++) {
            if (lsm.isSelectedIndex(i)) {
                int rowModel = tasksTable.convertRowIndexToModel(i);
                long taskId = tasksTableModel.getContentAtRow(rowModel).getId();
                selectedTasks.put(i, taskId);
            }
        }
    }

    /**
     * Sets up the context menu for the tasks table.
     */
    private void createTasksTablePopupMenu() {
        JMenuItem detailsMenuItem = createMenuItem("Task details" + DIALOG, this::showDetails);
        JMenuItem retrieveLogInfoMenuItem = createMenuItem("List task logs" + DIALOG,
            this::retrieveLogInfo);
        JMenuItem restartFailedTasksMenuItem = createMenuItem("Restart failed tasks" + DIALOG,
            this::restartFailedTasks);
        JMenuItem killTasksMenuItem = createMenuItem("Kill selected tasks", this::killTasks);

        JPopupMenu tasksPopupMenu = createPopupMenu(detailsMenuItem, retrieveLogInfoMenuItem,
            MENU_SEPARATOR, restartFailedTasksMenuItem, MENU_SEPARATOR, killTasksMenuItem);

        tasksTable.getTable().addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                int numTasksSelected = selectedTasks.size();
                if (numTasksSelected == 1) {
                    detailsMenuItem.setEnabled(true);
                    retrieveLogInfoMenuItem.setEnabled(true);
                } else {
                    detailsMenuItem.setEnabled(false);
                    retrieveLogInfoMenuItem.setEnabled(false);
                }
                restartFailedTasksMenuItem.setEnabled(true);

                if (numTasksSelected > 0 && e.isPopupTrigger()) {
                    tasksPopupMenu.show(tasksTable.getTable(), e.getX(), e.getY());
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
            }
        });
    }

    private void showDetails(ActionEvent evt) {
        PipelineTask selectedTask = selectedTask();
        if (selectedTask != null) {
            try {
                TaskInfoDialog.showTaskInfoDialog(SwingUtilities.getWindowAncestor(this),
                    selectedTask);
            } catch (PipelineException e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void retrieveLogInfo(ActionEvent evt) {
        PipelineTask selectedTask = selectedTask();

        if (selectedTask != null) {
            try {
                new TaskLogInformationDialog(SwingUtilities.getWindowAncestor(this), selectedTask)
                    .setVisible(true);
            } catch (PipelineException e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void restartFailedTasks(ActionEvent evt) {
        if (selectedTasks.isEmpty()) {
            return;
        }
        instancesTasksPanel.restartTasks(selectedTasks.values());
    }

    private void killTasks(ActionEvent evt) {
        if (selectedTasks.isEmpty()) {
            return;
        }
        instancesTasksPanel.killTasks(selectedTasks.values());
    }

    private PipelineTask selectedTask() {
        if (selectedTasks.size() != 1) {
            return null;
        }

        int selectedIndex = selectedTasks.keySet().iterator().next();
        int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
        return tasksTableModel.getContentAtRow(selectedModelRow);
    }

    /**
     * Allows the instances panel to clear the selected tasks when necessary.
     */
    void clearSelectedTasks() {
        selectedTasks.clear();
    }

    public TasksTableModel tasksTableModel() {
        return tasksTableModel;
    }

    public TaskStatusSummaryPanel taskStatusSummaryPanel() {
        return taskStatusSummaryPanel;
    }

    public void updateSummaryPanel() {
        taskStatusSummaryPanel.update(tasksTableModel);
    }
}
