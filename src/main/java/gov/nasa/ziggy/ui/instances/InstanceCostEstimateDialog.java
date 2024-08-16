package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.text.DecimalFormat;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.table.AbstractTableModel;

import org.netbeans.swing.etable.ETable;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Displays cost estimates for a pipeline instance and the tasks within that instance. Costs will be
 * in Standard Billing Units (SBUs) for tasks run on Pleiades, or in dollars for tasks run on AWS.
 *
 * @author PT
 * @author Bill Wohler
 */
public class InstanceCostEstimateDialog extends JDialog {

    private static final long serialVersionUID = 20240614L;

    private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    public InstanceCostEstimateDialog(Window owner, PipelineInstance pipelineInstance) {
        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(pipelineInstance);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(PipelineInstance pipelineInstance) {
        setTitle("Cost estimate");

        getContentPane().add(createDataPanel(pipelineInstance), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);
        setPreferredSize(ZiggyGuiConstants.MIN_DIALOG_SIZE);

        pack();
    }

    private JPanel createDataPanel(PipelineInstance pipelineInstance) {
        JLabel instance = boldLabel("Pipeline instance", LabelType.HEADING);
        instance.setToolTipText("Displays estimated cost of the full pipeline instance.");

        JLabel instanceId = boldLabel("ID:");
        JLabel instanceText = new JLabel(Long.toString(pipelineInstance.getId()));

        JLabel state = boldLabel("State:");
        JLabel stateText = new JLabel(pipelineInstance.getState().toString());

        List<PipelineTask> pipelineTasksInInstance = pipelineInstanceOperations()
            .updateJobs(pipelineInstance);

        JLabel cost = boldLabel("Cost estimate:");
        JLabel costText = new JLabel(instanceCost(pipelineTasksInInstance));

        JLabel tasks = boldLabel("Pipeline tasks", LabelType.HEADING);
        tasks.setToolTipText("Displays estimated cost of tasks in the selected pipeline instance.");

        ZiggyTable<PipelineTask> ziggyTable = new ZiggyTable<>(
            new TaskCostEstimateTableModel(pipelineTasksInInstance));

        ETable taskTable = ziggyTable.getTable();
        taskTable.setCellSelectionEnabled(false);
        taskTable.setRowSelectionAllowed(false);
        JScrollPane tableScrollPane = new JScrollPane(taskTable);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(instance)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggyGuiConstants.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instanceId)
                    .addComponent(state)
                    .addComponent(cost))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instanceText)
                    .addComponent(stateText)
                    .addComponent(costText)))
            .addComponent(tasks)
            .addComponent(tableScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(instance)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(instanceId)
                .addComponent(instanceText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(state).addComponent(stateText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(cost).addComponent(costText))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(tasks)
            .addComponent(tableScrollPane));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    static String instanceCost(List<PipelineTask> pipelineTasksInInstance) {
        double totalCost = 0;
        for (PipelineTask task : pipelineTasksInInstance) {
            totalCost += task.costEstimate();
        }
        return formatCost(totalCost);
    }

    private static String formatCost(double cost) {
        String format;
        if (cost < 1) {
            format = "#.####";
        } else if (cost < 10) {
            format = "#.###";
        } else if (cost < 100) {
            format = "#.##";
        } else {
            format = "#.#";
        }

        return new DecimalFormat(format).format(cost);
    }

    private PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    private static class TaskCostEstimateTableModel extends AbstractTableModel
        implements ModelContentClass<PipelineTask> {

        private static final long serialVersionUID = 20240614L;

        private static final String[] COLUMN_NAMES = { "ID", "Module", "UOW", "Status",
            "Cost estimate" };

        private final List<PipelineTask> pipelineTasks;

        public TaskCostEstimateTableModel(List<PipelineTask> pipelineTasks) {
            this.pipelineTasks = pipelineTasks;
        }

        @Override
        public int getRowCount() {
            return pipelineTasks.size();
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
            PipelineTask task = pipelineTasks.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> task.getId();
                case 1 -> task.getModuleName();
                case 2 -> task.uowTaskInstance().briefState();
                case 3 -> task.getDisplayProcessingStep();
                case 4 -> formatCost(task.costEstimate());
                default -> throw new IllegalArgumentException(
                    "Illegal column number: " + columnIndex);
            };
        }

        @Override
        public Class<PipelineTask> tableModelContentClass() {
            return PipelineTask.class;
        }
    }
}
