package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

/**
 * Displays task information for a {@link PipelineDefinitionNode}. Implemented as a modal dialog so
 * as to block until the user is done with it.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskInformationDialog extends JDialog {

    private static final long serialVersionUID = 20230810L;

    private PipelineDefinitionNode node;

    public TaskInformationDialog(Window owner, PipelineDefinitionNode node) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.node = node;
        buildComponent();
        setLocationRelativeTo(owner);
    }

    public void buildComponent() {
        setTitle("Task information");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        pack();
    }

    public JPanel createDataPanel() {
        JLabel pipeline = boldLabel("Pipeline");
        JLabel pipelineText = new JLabel(node.getPipelineName());

        JLabel module = boldLabel("Module");
        JLabel moduleText = new JLabel(node.getModuleName());

        List<SubtaskInformation> subtaskInfo = PipelineTaskInformation.subtaskInformation(node);
        JTable table = new JTable(new TaskInformationTableModel(subtaskInfo));
        table.setAutoCreateRowSorter(true);
        JScrollPane tablePane = new JScrollPane(table);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addComponent(module)
            .addComponent(moduleText)
            .addComponent(tablePane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(module)
            .addComponent(moduleText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(tablePane));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        dispose();
    }

    private static class TaskInformationTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20230810L;

        private static final String[] COLUMN_NAMES = { "Task number", "UOW", "Subtasks" };

        List<SubtaskInformation> subtaskInfo;

        public TaskInformationTableModel(List<SubtaskInformation> subtaskInfo) {
            this.subtaskInfo = subtaskInfo;
        }

        @Override
        public int getRowCount() {
            return subtaskInfo.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_NAMES[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            SubtaskInformation row = subtaskInfo.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> rowIndex + 1;
                case 1 -> row.getUowBriefState();
                case 2 -> row.getSubtaskCount();
                default -> throw new IllegalArgumentException(
                    "Illegal column number: " + columnIndex);
            };
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
