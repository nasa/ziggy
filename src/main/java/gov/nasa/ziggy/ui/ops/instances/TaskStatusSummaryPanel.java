package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;

import javax.swing.JScrollPane;

import gov.nasa.ziggy.ui.common.ZTable;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskStatusSummaryPanel extends javax.swing.JPanel {
    private JScrollPane summaryTableScrollPane;
    private ZTable taskSummaryTable;

    private TaskSummaryTableModel taskSummaryTableModel;

    public TaskStatusSummaryPanel() {
        initGUI();
    }

    /**
     * Updates the task status summary ("scoreboard") panel. This method must be called from the
     * event dispatch thread only.
     */
    public void update(TasksTableModel tasksTableModel) {
        taskSummaryTableModel.update(tasksTableModel);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new java.awt.Dimension(400, 112));
            this.add(getSummaryTableScrollPane(), BorderLayout.CENTER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getSummaryTableScrollPane() {
        if (summaryTableScrollPane == null) {
            summaryTableScrollPane = new JScrollPane();
            summaryTableScrollPane.setViewportView(getTaskSummaryTable());
        }
        return summaryTableScrollPane;
    }

    private ZTable getTaskSummaryTable() {
        if (taskSummaryTable == null) {
            taskSummaryTableModel = new TaskSummaryTableModel();
            taskSummaryTable = new ZTable();
//            taskSummaryTable.setTextWrappingEnabled(true);
            taskSummaryTable.setRowShadingEnabled(true);
            taskSummaryTable.setModel(taskSummaryTableModel);
        }
        return taskSummaryTable;
    }
}
