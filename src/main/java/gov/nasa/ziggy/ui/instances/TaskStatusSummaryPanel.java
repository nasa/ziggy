package gov.nasa.ziggy.ui.instances;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.JScrollPane;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.TableUpdater;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskStatusSummaryPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(TaskStatusSummaryPanel.class);

    private TaskSummaryTableModel taskSummaryTableModel;

    public TaskStatusSummaryPanel() {
        taskSummaryTableModel = new TaskSummaryTableModel();
        ZiggyTable<Counts> ziggyTable = new ZiggyTable<>(taskSummaryTableModel);
        ziggyTable.setWrapText(false);
        setLayout(new BorderLayout());
        setPreferredSize(new java.awt.Dimension(400, 112));
        add(new JScrollPane(ziggyTable.getTable()), BorderLayout.CENTER);

        ZiggyMessenger.subscribe(TasksUpdatedMessage.class,
            message -> taskSummaryTableModel.update(message.getTasksTableModel()));
    }

    /**
     * Updates the task status summary ("scoreboard") panel.
     */
    public void update(TasksTableModel tasksTableModel) {
        taskSummaryTableModel.update(tasksTableModel);
    }

    private static class TaskSummaryTableModel extends AbstractZiggyTableModel<Counts> {
        private final TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel();
        private List<Counts> counts;

        public void update(TasksTableModel tasksTableModel) {
            new SwingWorker<TableUpdater, Void>() {

                @Override
                protected TableUpdater doInBackground() {
                    List<Counts> oldCounts = counts;
                    TaskCounts taskCounts = tasksTableModel.getTaskStates();
                    taskSummaryDisplayModel.update(taskCounts);
                    counts = taskCountsToCountList(taskCounts);
                    return new TableUpdater(oldCounts, counts);
                }

                @Override
                protected void done() {
                    try {
                        get().updateTable(TaskSummaryTableModel.this);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Can't update task summary table", e);
                    }
                }
            }.execute();
        }

        private List<Counts> taskCountsToCountList(TaskCounts taskCounts) {
            List<Counts> counts = new ArrayList<>();
            for (String moduleName : taskCounts.getModuleNames()) {
                counts.add(taskCounts.getModuleCounts().get(moduleName));
            }
            counts.add(taskCounts.getTotalCounts());
            return counts;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return taskSummaryDisplayModel.getValueAt(rowIndex, columnIndex);
        }

        @Override
        public int getColumnCount() {
            return taskSummaryDisplayModel.getColumnCount();
        }

        @Override
        public String getColumnName(int column) {
            return taskSummaryDisplayModel.getColumnName(column);
        }

        @Override
        public int getRowCount() {
            return taskSummaryDisplayModel.getRowCount();
        }

        @Override
        public Class<Counts> tableModelContentClass() {
            return Counts.class;
        }

        @Override
        public Counts getContentAtRow(int row) {
            return taskSummaryDisplayModel.getContentAtRow(row);
        }
    }
}
