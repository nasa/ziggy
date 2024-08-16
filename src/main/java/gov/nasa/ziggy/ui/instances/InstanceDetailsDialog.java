package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
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

    private final PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    public InstanceDetailsDialog(Window owner, PipelineInstance pipelineInstance) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipelineInstance = pipelineInstance;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Pipeline instance details");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane()
            .add(createButtonPanel(ZiggySwingUtils.createButton(REPORT, this::generateReport),
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
            pipelineInstanceOperations().parameterSets(pipelineInstance));
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
                .addGap(ZiggyGuiConstants.INDENT)
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
            .addGap(ZiggyGuiConstants.GROUP_GAP)
            .addComponent(pipelineParametersGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(pipelineParameterSetsPanel)
            .addGap(ZiggyGuiConstants.GROUP_GAP)
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
            MessageUtils.showError(this, "No module selected");
        } else {
            PipelineInstanceNode node = ziggyTable.getContentAtViewRow(selectedRow);
            new SwingWorker<Set<ParameterSet>, Void>() {

                @Override
                protected Set<ParameterSet> doInBackground() throws Exception {
                    return pipelineInstanceNodeOperations().parameterSets(node);
                }

                @Override
                protected void done() {
                    try {
                        new ParameterSetViewDialog(InstanceDetailsDialog.this, get())
                            .setVisible(true);
                    } catch (InterruptedException | ExecutionException e) {
                        MessageUtils.showError(InstanceDetailsDialog.this, e);
                    }
                }
            }.execute();
        }
    }

    private void generateReport(ActionEvent evt) {
        new SwingWorker<String, String>() {
            @Override
            protected String doInBackground() throws Exception {
                return new PipelineReportGenerator().generatePedigreeReport(pipelineInstance);
            }

            @Override
            protected void done() {
                try {
                    TextualReportDialog.showReport(InstanceDetailsDialog.this, get(),
                        "Instance report",
                        ReportFilePaths.instanceDetailsReportPath(pipelineInstance.getId()));
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(getRootPane(), e);
                }
            }
        }.execute();
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    private PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    private static class InstanceModulesTableModel
        extends AbstractZiggyTableModel<PipelineInstanceNode> {

        private static final String[] COLUMN_NAMES = { "Module", "Tasks", "Waiting to run",
            "Completed", "Failed" };

        private List<PipelineInstanceNode> pipelineInstanceNodes = new LinkedList<>();
        private Map<PipelineInstanceNode, TaskCounts> nodeTaskCounts = new HashMap<>();

        private final PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

        public InstanceModulesTableModel(PipelineInstance instance) {
            if (instance == null) {
                return;
            }

            pipelineInstanceNodes = pipelineInstanceNodeOperations()
                .pipelineInstanceNodes(instance);
            for (PipelineInstanceNode node : pipelineInstanceNodes) {
                nodeTaskCounts.put(node, pipelineInstanceNodeOperations().taskCounts(node));
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
                case 0 -> node.getModuleName();
                case 1 -> taskCounts.getTaskCount();
                case 2 -> taskCounts.getTotalCounts().getWaitingToRunTaskCount();
                case 3 -> taskCounts.getTotalCounts().getCompletedTaskCount();
                case 4 -> taskCounts.getTotalCounts().getFailedTaskCount();
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

        private PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
            return pipelineInstanceNodeOperations;
        }
    }
}
