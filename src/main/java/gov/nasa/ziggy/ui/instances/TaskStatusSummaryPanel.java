package gov.nasa.ziggy.ui.instances;

import java.awt.BorderLayout;

import javax.swing.JScrollPane;

import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskStatusSummaryPanel extends javax.swing.JPanel {
    private TaskSummaryTableModel taskSummaryTableModel;

    public TaskStatusSummaryPanel() {
        taskSummaryTableModel = new TaskSummaryTableModel();
        ZiggyTable<Counts> ziggyTable = new ZiggyTable<>(taskSummaryTableModel);
        ziggyTable.setWrapText(false);
        setLayout(new BorderLayout());
        setPreferredSize(new java.awt.Dimension(400, 112));
        add(new JScrollPane(ziggyTable.getTable()), BorderLayout.CENTER);
    }

    /**
     * Updates the task status summary ("scoreboard") panel. This method must be called from the
     * event dispatch thread only.
     */
    public void update(TasksTableModel tasksTableModel) {
        taskSummaryTableModel.update(tasksTableModel);
    }

    private static class TaskSummaryTableModel extends AbstractZiggyTableModel<Counts> {
        private final TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel();

        public TaskSummaryTableModel() {
        }

        public void update(TasksTableModel tasksTableModel) {
            taskSummaryDisplayModel.update(tasksTableModel.getTaskStates());

            fireTableDataChanged();
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
