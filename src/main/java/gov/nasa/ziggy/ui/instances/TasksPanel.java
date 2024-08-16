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
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.TaskHalter;
import gov.nasa.ziggy.ui.util.TaskRestarter;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * The right hand side subpanel to the operations > instances panel. This panel displays the table
 * of tasks for a given instance, plus the subtask "scoreboard."
 *
 * @author PT
 * @author Bill Wohler
 */
public class TasksPanel extends JPanel {

    private static final long serialVersionUID = 20240614L;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TasksPanel.class);

    private TaskStatusSummaryPanel taskStatusSummaryPanel;
    private ZiggyTable<PipelineTask> tasksTable;
    private TasksTableModel tasksTableModel;
    private long currentInstanceId = -1;

    private Map<Integer, Long> selectedTaskIdByRow = new HashMap<>();

    private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    public TasksPanel() {
        buildComponent();

        // Subscribe to the messages that indicate that something has potentially changed
        // with the instance panel's instance selection.
        ZiggyMessenger.subscribe(DisplayTasksForInstanceMessage.class,
            this::displayTasksForInstance);

        ZiggyMessenger.subscribe(InvalidateConsoleModelsMessage.class, this::invalidateModel);
    }

    private void displayTasksForInstance(DisplayTasksForInstanceMessage message) {
        new SwingWorker<Void, Void>() {

            @Override
            protected Void doInBackground() {
                long instanceId = message.getPipelineInstanceId();

                if (message.isReselect() && instanceId == -1) {
                    currentInstanceId = instanceId;

                    // If the message comes from a reselect action, we only need to do anything
                    // if the reselect resulted in no selected instance.
                    tasksTableModel.setPipelineInstance(null);
                    tasksTableModel.loadFromDatabase(false);
                    clearSelectedTasks();
                } else if (!message.isReselect()) {

                    // If the message comes from a user clicking on a row in the table, we
                    // need to see whether this is a genuinely new instance, or if the user
                    // just clicked on the instance that was already selected.
                    PipelineInstance selectedInstance = null;
                    if (instanceId != currentInstanceId) {
                        selectedInstance = pipelineInstanceOperations()
                            .pipelineInstance(instanceId);
                        currentInstanceId = instanceId;
                    }
                    boolean genuinelyNewInstance = selectedInstance != null
                        ? tasksTableModel.updatePipelineInstance(selectedInstance)
                        : false;
                    tasksTableModel.loadFromDatabase(false);

                    // If the change is that a genuinely new instance was selected, clear the tasks
                    // table selections as they are no longer valid.
                    if (genuinelyNewInstance) {
                        SwingUtilities.invokeLater(
                            () -> tasksTable.getTable().getSelectionModel().clearSelection());
                        clearSelectedTasks();
                    }
                }
                return null;
            }

            @Override
            protected void done() {
                // No matter what happened, update the summary display.
                updateSummaryPanel();
            }
        }.execute();
    }

    private void invalidateModel(InvalidateConsoleModelsMessage message) {
        tasksTableModel.loadFromDatabase();
    }

    private void buildComponent() {
        JLabel instance = boldLabel("Pipeline tasks", LabelType.HEADING1);

        taskStatusSummaryPanel = new TaskStatusSummaryPanel();

        tasksTableModel = new TasksTableModel();
        tasksTable = createTasksTable(tasksTableModel);
        JScrollPane tasksTableScrollPane = new JScrollPane(tasksTable.getTable());
        createTasksTablePopupMenu();

        GroupLayout layout = new GroupLayout(this);
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
        for (int column = 0; column < TasksTableModel.COLUMN_WIDTHS.length; column++) {
            tasksTable.setPreferredColumnWidth(column, TasksTableModel.COLUMN_WIDTHS[column]);
        }

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
        if (selectedTaskIdByRow.isEmpty()) {
            return;
        }

        // Find the model indices of the selected task IDs. Here we use a List of
        // task IDs so that the order of the task IDs and the model IDs match one another.
        List<Long> selectedTaskIds = new ArrayList<>(selectedTaskIdByRow.values());
        List<Integer> taskModelIds = tasksTableModel.getModelIndicesOfTasks(selectedTaskIds);
        selectedTaskIdByRow.clear();

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
            selectedTaskIdByRow.put(rowId, taskId);
        }
    }

    /**
     * Capture the selected task table rows and the corresponding task IDs.
     */
    private void captureSelectedTaskRows(ListSelectionModel lsm) {

        // Clear the cache of selected rows / tasks.
        selectedTaskIdByRow.clear();

        // Capture the selected row index and task ID in the cache.
        for (int index : lsm.getSelectedIndices()) {
            int rowModel = tasksTable.convertRowIndexToModel(index);
            long taskId = tasksTableModel.getContentAtRow(rowModel).getId();
            selectedTaskIdByRow.put(index, taskId);
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
            this::restartTasks);
        JMenuItem haltTasksMenuItem = createMenuItem("Halt selected tasks", this::haltTasks);

        JPopupMenu tasksPopupMenu = createPopupMenu(detailsMenuItem, retrieveLogInfoMenuItem,
            MENU_SEPARATOR, restartFailedTasksMenuItem, MENU_SEPARATOR, haltTasksMenuItem);

        tasksTable.getTable().addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    ZiggySwingUtils.adjustSelection(tasksTable.getTable(), e);
                    detailsMenuItem.setEnabled(selectedTaskIdByRow.size() == 1);
                    retrieveLogInfoMenuItem.setEnabled(selectedTaskIdByRow.size() == 1);
                    tasksPopupMenu.show(tasksTable.getTable(), e.getX(), e.getY());
                }
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
                MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
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
                MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void restartTasks(ActionEvent evt) {
        if (selectedTaskIdByRow.isEmpty()) {
            return;
        }

        new TaskRestarter().restartTasks(SwingUtilities.getWindowAncestor(this),
            tasksTableModel.getPipelineInstance(), selectedTaskIdByRow.values());
    }

    private void haltTasks(ActionEvent evt) {
        if (selectedTaskIdByRow.isEmpty()) {
            return;
        }

        new TaskHalter().haltTasks(SwingUtilities.getWindowAncestor(this),
            tasksTableModel.getPipelineInstance(), selectedTaskIdByRow.values());
    }

    private PipelineTask selectedTask() {
        if (selectedTaskIdByRow.size() != 1) {
            return null;
        }

        int selectedIndex = selectedTaskIdByRow.keySet().iterator().next();
        int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
        return tasksTableModel.getContentAtRow(selectedModelRow);
    }

    /**
     * Allows the instances panel to clear the selected tasks when necessary.
     */
    void clearSelectedTasks() {
        selectedTaskIdByRow.clear();
    }

    public TasksTableModel tasksTableModel() {
        return tasksTableModel;
    }

    public TaskStatusSummaryPanel taskStatusSummaryPanel() {
        return taskStatusSummaryPanel;
    }

    private void updateSummaryPanel() {
        taskStatusSummaryPanel.update(tasksTableModel);
    }

    private PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }
}
