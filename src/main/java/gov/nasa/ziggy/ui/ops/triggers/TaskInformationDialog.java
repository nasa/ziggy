package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.Dialog;
import java.util.List;

import javax.swing.JDialog;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

/**
 * Displays task information for a {@link PipelineDefinitionNode}. Implemented as a modal dialog so
 * as to block until the user is done with it.
 *
 * @author PT
 */
public class TaskInformationDialog extends JDialog {

    private static final long serialVersionUID = 20210810L;

    private PipelineDefinitionNode node;

    public TaskInformationDialog(Dialog parent, PipelineDefinitionNode node) {
        super(parent, true);
        this.node = node;
        initGUI();
    }

    public void initGUI() {
        setSize(480, 400);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setTitle(node.getPipelineName() + " : " + node.getModuleName().getName());
        List<SubtaskInformation> subtaskInfo = PipelineTaskInformation.subtaskInformation(node);
        boolean maxSubtaskLimtsEnabled = PipelineTaskInformation.parallelLimits(node);
        JTable table = new JTable(
            new TaskInformationTableModel(subtaskInfo, maxSubtaskLimtsEnabled));
        table.setAutoCreateRowSorter(true);
        JScrollPane jsp = new JScrollPane(table);
        getContentPane().add(jsp, BorderLayout.CENTER);
    }

    private static class TaskInformationTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20210818L;

        private static final String MAX_PARALLEL_SUBTASK_HEADER = "Max Parallel Subtasks";
        private static final String[] ALL_COLUMN_HEADERS = new String[] { "", "UOW", "Subtasks",
            MAX_PARALLEL_SUBTASK_HEADER };
        String[] columnHeaders;
        List<SubtaskInformation> subtaskInfo;

        public TaskInformationTableModel(List<SubtaskInformation> subtaskInfo,
            boolean maxSubtaskLimitsEnabled) {
            this.subtaskInfo = subtaskInfo;
            if (!maxSubtaskLimitsEnabled) {
                columnHeaders = new String[ALL_COLUMN_HEADERS.length - 1];
                int iColumn = 0;
                for (String columnHeader : ALL_COLUMN_HEADERS) {
                    if (!columnHeader.equals(MAX_PARALLEL_SUBTASK_HEADER)) {
                        columnHeaders[iColumn] = columnHeader;
                        iColumn++;
                    }
                }
            } else {
                columnHeaders = ALL_COLUMN_HEADERS;
            }
        }

        @Override
        public int getRowCount() {
            return subtaskInfo.size();
        }

        @Override
        public int getColumnCount() {
            return columnHeaders.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return columnHeaders[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            SubtaskInformation row = subtaskInfo.get(rowIndex);
            Object returnValue = null;
            switch (columnIndex) {
                case 0:
                    returnValue = rowIndex + 1;
                    break;
                case 1:
                    returnValue = row.getUowBriefState();
                    break;
                case 2:
                    returnValue = row.getSubtaskCount();
                    break;
                case 3:
                    returnValue = row.getMaxParallelSubtasks();
                    break;
                default:
                    throw new IllegalArgumentException("Illegal column number: " + columnIndex);
            }
            return returnValue;
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            if (subtaskInfo == null || subtaskInfo.isEmpty()) {
                return Object.class;
            }
            return getValueAt(0, columnIndex).getClass();
        }

    }

}
