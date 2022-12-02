package gov.nasa.ziggy.ui.ops.instances;

import static gov.nasa.ziggy.ui.common.LabelUtils.boldLabel;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingConstants;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.ui.common.ToolTipBorderJPanel;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.proxy.PipelineTaskOperationsProxy;

/**
 * Displays cost estimates for a pipeline instance and the tasks within that instance. Costs will be
 * in Standard Billing Units (SBUs) for tasks run on Pleiades, or in dollars for tasks run on AWS.
 *
 * @author PT
 */
public class InstanceCostEstimateDialog extends JDialog {

//    private static final Logger log = LoggerFactory.getLogger(InstanceCostEstimateDialog.class);

    private static final long serialVersionUID = 20220425L;

    private final PipelineInstance pipelineInstance;
    private JButton closeButton;
    private JPanel buttonPanel;
    private JPanel instancePanel;
    private JPanel tasksPanel;
    private JScrollPane taskScrollPane;
    private List<PipelineTask> pipelineTasksInInstance;

    private JLabel instanceIdLabel = boldLabel();
    private JLabel instanceStateLabel = boldLabel();
    private JLabel instanceCostLabel = boldLabel();

    public InstanceCostEstimateDialog(JFrame parent, PipelineInstance pipelineInstance) {
        super(parent, true);
        this.pipelineInstance = pipelineInstance;
        initGUI();
        setVisible(true);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setTitle("Cost Estimate: Instance " + pipelineInstance.getId());
            pipelineTasksInInstance = new PipelineTaskOperationsProxy()
                .updateAndRetrieveTasks(pipelineInstance);
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getTasksPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            getContentPane().add(getInstancePanel(), BorderLayout.NORTH);
            this.setSize(750, 500);
            setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // At the bottom are some buttons: refresh and close.
    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(20);
            actionPanelLayout.setAlignment(FlowLayout.RIGHT);
            buttonPanel.setLayout(actionPanelLayout);
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("Close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed());
        }
        return closeButton;
    }

    private void closeButtonActionPerformed() {
        setVisible(false);
    }

    // At the top: the roll-up of the instance state and cost estimate
    private JPanel getInstancePanel() {
        if (instancePanel == null) {
            instancePanel = new ToolTipBorderJPanel();
            instancePanel.setBorder(BorderFactory.createTitledBorder("Pipeline Instance"));
            instancePanel.setToolTipText("Displays estimated cost of the full pipeline instance");
            instancePanel.setLayout(new GridLayout(3, 2));
            instancePanel.add(new JLabel("Instance ID:  ", SwingConstants.RIGHT));
            instancePanel.add(instanceIdLabel);
            instanceIdLabel.setText(Long.toString(pipelineInstance.getId()));
            instancePanel.add(new JLabel("State:  ", SwingConstants.RIGHT));
            instancePanel.add(instanceStateLabel);
            instanceStateLabel.setText(pipelineInstance.getState().toString());
            instancePanel.add(new JLabel("Cost estimate:  ", SwingConstants.RIGHT));
            instancePanel.add(instanceCostLabel);
            instanceCostLabel.setText(instanceCost());

        }
        return instancePanel;
    }

    private String instanceCost() {
        double totalCost = 0;
        for (PipelineTask task : pipelineTasksInInstance) {
            totalCost += task.costEstimate();
        }
        return Long.toString(Math.round(totalCost));
    }

    private JPanel getTasksPanel() {
        if (tasksPanel == null) {
            tasksPanel = new ToolTipBorderJPanel();
            tasksPanel.setBorder(BorderFactory.createTitledBorder("Pipeline Tasks"));
            tasksPanel.setLayout(new BorderLayout());
            tasksPanel.setToolTipText(
                "Displays estimated cost of tasks in the selected pipeline instance");
            tasksPanel.add(getTaskScrollPane());
        }
        return tasksPanel;
    }

    // In between: the table of task states and costs
    private JScrollPane getTaskScrollPane() {
        if (taskScrollPane == null) {
            ZTable taskTable = new ZTable(new TaskCostEstimateTableModel(pipelineTasksInInstance));
            taskTable.setRowShadingEnabled(true);
            taskTable.setTextWrappingEnabled(true);
            taskTable.setCellSelectionEnabled(false);
            taskTable.setRowSelectionAllowed(false);
            taskScrollPane = new JScrollPane(taskTable);
        }
        return taskScrollPane;
    }

    private static class TaskCostEstimateTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20220425L;

        private static final String[] COLUMN_HEADINGS = { "ID", "Module", "UOW", "State",
            "Cost Estimate" };

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
            return COLUMN_HEADINGS.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_HEADINGS[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            PipelineTask task = pipelineTasks.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return task.getId();
                case 1:
                    return task.getModuleName();
                case 2:
                    return task.uowTaskInstance().briefState();
                case 3:
                    return task.getState().toString();
                case 4:
                    return Long.toString(Math.round(task.costEstimate()));
                default:
                    throw new IllegalArgumentException("Illegal column number: " + columnIndex);
            }
        }

    }
}
