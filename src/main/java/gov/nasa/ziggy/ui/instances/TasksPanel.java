package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.MENU_SEPARATOR;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createPopupMenu;

import java.awt.event.ActionEvent;
import java.awt.event.MouseListener;
import java.util.HashMap;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
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

    private ZiggyTable<PipelineTask> tasksTable;
    private TasksTableModel tasksTableModel;
    private long currentInstanceId = -1;

    private Map<Integer, PipelineTask> selectedTasksByRow = new HashMap<>();

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public TasksPanel() {
        buildComponent();

        // Subscribe to the messages that indicate that something has potentially changed
        // with the instance panel's instance selection.
        ZiggyMessenger.subscribe(SelectedInstanceChangedMessage.class,
            this::displayTasksForInstance);

        ZiggyMessenger.subscribe(PipelineInstanceStartedMessage.class, this::invalidateModel);
    }

    private void displayTasksForInstance(SelectedInstanceChangedMessage message) {
        if (message.getPipelineInstanceId() == currentInstanceId) {
            return;
        }
        tasksTable.getTable().getSelectionModel().clearSelection();
        ZiggySwingUtils.flushEventDispatchThread();
        selectedTasksByRow.clear();
        currentInstanceId = message.getPipelineInstanceId();
        tasksTableModel.updatePipelineInstanceId(currentInstanceId);
    }

    private void invalidateModel(PipelineInstanceStartedMessage message) {
        tasksTableModel.loadFromDatabase();
    }

    private void buildComponent() {
        JLabel tasks = boldLabel("Pipeline tasks", LabelType.HEADING1);

        TaskStatusSummaryPanel taskStatusSummaryPanel = new TaskStatusSummaryPanel();

        tasksTableModel = new TasksTableModel();
        tasksTable = createTasksTable(tasksTableModel);
        tasksTableModel.setTable(tasksTable.getTable());
        JScrollPane tasksTableScrollPane = new JScrollPane(tasksTable.getTable());
        createTasksTablePopupMenu();

        GroupLayout layout = new GroupLayout(this);
        setLayout(layout);

        layout.setHorizontalGroup(layout.createParallelGroup()
            .addComponent(tasks)
            .addComponent(taskStatusSummaryPanel)
            .addComponent(tasksTableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(tasks)
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
        selectionModel.addListSelectionListener(evt -> {
            if (evt.getValueIsAdjusting()) {
                return;
            }
            captureSelectedTaskRows(selectionModel);
        });

        return tasksTable;
    }

    /**
     * Capture the selected task table rows and the corresponding tasks.
     */
    private void captureSelectedTaskRows(ListSelectionModel selectionModel) {

        // Clear the cache of selected rows / tasks.
        selectedTasksByRow.clear();

        // Capture the selected row index and task in the cache.
        for (int index : selectionModel.getSelectedIndices()) {
            int rowModel = tasksTable.convertRowIndexToModel(index);
            PipelineTask pipelineTask = pipelineTaskOperations()
                .pipelineTask(tasksTableModel.getContentAtRow(rowModel).getPipelineTaskId());
            selectedTasksByRow.put(index, pipelineTask);
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

        // Remove the existing table mouse listener as it clears the selection with Control-Click on
        // the Mac rather than preserving the selection before it displays a menu. Perform selection
        // ourselves using ZiggySwingUtils.adjustSelection(). See below.
        for (MouseListener l : tasksTable.getTable().getMouseListeners()) {
            if (l.getClass().getName().equals("javax.swing.plaf.basic.BasicTableUI$Handler")) {
                tasksTable.getTable().removeMouseListener(l);
            }
        }
        tasksTable.getTable().addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                ZiggySwingUtils.adjustSelection(tasksTable.getTable(), e);
                if (e.isPopupTrigger()) {
                    detailsMenuItem.setEnabled(selectedTasksByRow.size() == 1);
                    retrieveLogInfoMenuItem.setEnabled(selectedTasksByRow.size() == 1);
                    tasksPopupMenu.show(tasksTable.getTable(), e.getX(), e.getY());
                }
            }
        });
    }

    private void showDetails(ActionEvent evt) {
        PipelineTaskDisplayData selectedTask = selectedTask();
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
        PipelineTaskDisplayData selectedTask = selectedTask();

        if (selectedTask != null) {
            try {
                new TaskLogInformationDialog(SwingUtilities.getWindowAncestor(this),
                    selectedTask.getPipelineTaskId()).setVisible(true);
            } catch (PipelineException e) {
                MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void restartTasks(ActionEvent evt) {
        if (selectedTasksByRow.isEmpty()) {
            return;
        }

        new TaskRestarter().restartTasks(SwingUtilities.getWindowAncestor(this),
            tasksTableModel.getPipelineInstance(), selectedTasksByRow.values());
    }

    private void haltTasks(ActionEvent evt) {
        if (selectedTasksByRow.isEmpty()) {
            return;
        }

        new TaskHalter().haltTasks(SwingUtilities.getWindowAncestor(this),
            tasksTableModel.getPipelineInstance(), selectedTasksByRow.values());
    }

    private PipelineTaskDisplayData selectedTask() {
        if (selectedTasksByRow.size() != 1) {
            return null;
        }

        int selectedIndex = selectedTasksByRow.keySet().iterator().next();
        int selectedModelRow = tasksTable.convertRowIndexToModel(selectedIndex);
        return tasksTableModel.getContentAtRow(selectedModelRow);
    }

    public TasksTableModel tasksTableModel() {
        return tasksTableModel;
    }

    private PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }
}
