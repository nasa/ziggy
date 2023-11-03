package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REPORT;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.proxy.PipelineInstanceCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineInstanceNodeCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstanceDetailsDialog extends javax.swing.JDialog {
    private JLabel nameText;
    private ZiggyTable<PipelineInstanceNode> ziggyTable;
    private final PipelineInstance pipelineInstance;

    public InstanceDetailsDialog(Window owner, PipelineInstance pipelineInstance) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipelineInstance = pipelineInstance;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Pipeline instance details");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(ZiggySwingUtils.createButton(REFRESH, this::refresh),
            ZiggySwingUtils.createButton(REPORT, this::generateReport),
            ZiggySwingUtils.createButton(CLOSE, this::close)), BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel instanceGroup = boldLabel("Pipeline instance", LabelType.HEADING1);

        JLabel id = boldLabel("ID: ");
        JLabel idText = new JLabel(Long.toString(pipelineInstance.getId()));

        JLabel name = boldLabel("Name:");
        nameText = new JLabel(pipelineInstance.getName());

        JLabel start = boldLabel("Start:");
        JLabel startText = new JLabel(pipelineInstance.getStartProcessingTime().toString());

        JLabel end = boldLabel("End:");
        Date endProcessingTime = pipelineInstance.getEndProcessingTime();
        JLabel endText = new JLabel(
            endProcessingTime.getTime() == 0 ? "-" : endProcessingTime.toString());

        JLabel total = boldLabel("Total:");
        JLabel totalText = new JLabel(pipelineInstance.elapsedTime());

        JLabel pipelineParametersGroup = boldLabel("Pipeline parameter sets", LabelType.HEADING1);
        ParameterSetViewPanel pipelineParameterSetsPanel = new ParameterSetViewPanel(
            pipelineInstance.getPipelineParameterSets());

        JLabel modulesGroup = boldLabel("Modules", LabelType.HEADING1);
        JPanel modulesButtonPanel = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("View module parameters", this::viewModuleParameters));
        ziggyTable = new ZiggyTable<>(new InstanceModulesTableModel(pipelineInstance));
        JScrollPane tableScrollPane = new JScrollPane(ziggyTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(instanceGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggySwingUtils.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(id)
                    .addComponent(name)
                    .addComponent(start)
                    .addComponent(end)
                    .addComponent(total))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(idText)
                    .addComponent(nameText)
                    .addComponent(startText)
                    .addComponent(endText)
                    .addComponent(totalText)))
            .addComponent(pipelineParametersGroup)
            .addComponent(pipelineParameterSetsPanel)
            .addComponent(modulesGroup)
            .addComponent(modulesButtonPanel)
            .addComponent(tableScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(instanceGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup().addComponent(id).addComponent(idText))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(name).addComponent(nameText))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(start).addComponent(startText))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup().addComponent(end).addComponent(endText))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(total).addComponent(totalText))
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(pipelineParametersGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(pipelineParameterSetsPanel)
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(modulesGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(modulesButtonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(tableScrollPane));

        return dataPanel;
    }

    private void viewModuleParameters(ActionEvent evt) {
        int selectedRow = ziggyTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            PipelineInstanceNode node = ziggyTable.getContentAtViewRow(selectedRow);
            new ParameterSetViewDialog(this, node.getModuleParameterSets()).setVisible(true);
        }
    }

    private void refresh(ActionEvent evt) {

        try {
            String newName = nameText.getText();

            if (!newName.equals(pipelineInstance.getName())) {
                PipelineInstanceCrudProxy instanceCrud = new PipelineInstanceCrudProxy();
                instanceCrud.updateName(pipelineInstance.getId(), newName);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void generateReport(ActionEvent evt) {
        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generatePedigreeReport(pipelineInstance);

        TextualReportDialog.showReport(this, report, "Instance report",
            ReportFilePaths.instanceDetailsReportPath(pipelineInstance.getId()));
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private static class InstanceModulesTableModel
        extends AbstractZiggyTableModel<PipelineInstanceNode> {

        private static final String[] COLUMN_NAMES = { "Name", "Tasks", "Submitted", "Completed",
            "Failed" };

        private List<PipelineInstanceNode> pipelineInstanceNodes = new LinkedList<>();
        private Map<PipelineInstanceNode, TaskCounts> nodeTaskCounts = new HashMap<>();

        public InstanceModulesTableModel(PipelineInstance instance) {
            if (instance != null) {
                PipelineInstanceNodeCrudProxy pipelineInstanceNodeCrud = new PipelineInstanceNodeCrudProxy();
                pipelineInstanceNodes = pipelineInstanceNodeCrud.retrieveAll(instance);
                PipelineOperationsProxy pipelineOperations = new PipelineOperationsProxy();
                for (PipelineInstanceNode node : pipelineInstanceNodes) {
                    nodeTaskCounts.put(node, pipelineOperations.taskCounts(node));
                }
            }
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public int getRowCount() {
            return pipelineInstanceNodes.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            PipelineInstanceNode node = pipelineInstanceNodes.get(rowIndex);
            TaskCounts taskCounts = nodeTaskCounts.get(node);

            return switch (columnIndex) {
                case 0 -> node.getPipelineDefinitionNode().getModuleName();
                case 1 -> taskCounts.getTaskCount();
                case 2 -> taskCounts.getSubmittedTaskCount();
                case 3 -> taskCounts.getCompletedTaskCount();
                case 4 -> taskCounts.getFailedTaskCount();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<PipelineInstanceNode> tableModelContentClass() {
            return PipelineInstanceNode.class;
        }

        @Override
        public PipelineInstanceNode getContentAtRow(int row) {
            return pipelineInstanceNodes.get(row);
        }
    }
}
