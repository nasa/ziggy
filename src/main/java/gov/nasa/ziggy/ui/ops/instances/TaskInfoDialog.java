package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.util.dispmod.DisplayModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskInfoDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(TaskInfoDialog.class);

    private JPanel detailsPanel;
    private JLabel failureCountLabel;
    private JTextField createdTextField;
    private JTextField endTextField;
    private JTextField startTextField;
    private JTextField workerTextField;
    private JTextField uowTextField;
    private JTextField moduleTextField;
    private JTextField stateTextField;
    private ZTable processingBreakdownTable;
    private JScrollPane processingBreakdownScrollPane;
    private JPanel processingBreakdownPanel;
    private JTextField revisionTextField;
    private JTextField idTextField;
    private JCheckBox transitionCheckBox;
    private JTextField failCountTextField;
    private JPanel miscPanel;
    private JLabel stateLabel;
    private JLabel revisionLabel;
    private JLabel createdLabel;
    private JLabel endLabel;
    private JLabel startLabel;
    private JLabel workerLabel;
    private JLabel uowLabel;
    private JLabel moduleLabel;
    private JLabel idLabel;
    private JPanel dataPanel;
    private JButton closeButton;
    private JPanel buttonPanel;

    private TaskMetricsTableModel processingBreakdownTableModel;
    private PipelineTask pipelineTask;

    public TaskInfoDialog(JFrame frame, PipelineTask pipelineTask) {
        super(frame);

        this.pipelineTask = pipelineTask;

        initGUI();
    }

    public static void showTaskInfoDialog(JFrame frame, PipelineTask pipelineTask) {
        TaskInfoDialog dialog = new TaskInfoDialog(frame, pipelineTask);

        dialog.setVisible(true);
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.info("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void transitionCheckBoxActionPerformed() {
        transitionCheckBox.setSelected(pipelineTask.isTransitionComplete());
    }

    private void initGUI() {
        try {
            {
                BorderLayout thisLayout = new BorderLayout();
                thisLayout.setHgap(10);
                thisLayout.setVgap(10);
                getContentPane().setLayout(thisLayout);
                setTitle("Pipeline Task Details");
                getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
                getContentPane().add(getDataPanel(), BorderLayout.NORTH);
            }
            this.setSize(1280, 400);

            String moduleName = pipelineTask.getPipelineInstanceNode()
                .getPipelineModuleDefinition()
                .getName()
                .getName();

            idTextField.setText(pipelineTask.getId() + "");
            moduleTextField.setText(moduleName + "");
            uowTextField.setText(pipelineTask.uowTaskInstance().briefState());
            workerTextField.setText(pipelineTask.getWorkerName());
            startTextField.setText(DisplayModel.formatDate(pipelineTask.getStartProcessingTime()));
            endTextField.setText(DisplayModel.formatDate(pipelineTask.getEndProcessingTime()));
            createdTextField.setText(DisplayModel.formatDate(pipelineTask.getCreated()));
            revisionTextField.setText(pipelineTask.getSoftwareRevision());
            failCountTextField.setText(pipelineTask.getFailureCount() + "");
            transitionCheckBox.setSelected(pipelineTask.isTransitionComplete());

            State state = pipelineTask.getState();
            stateTextField.setText(state.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDetailsPanel() {
        if (detailsPanel == null) {
            detailsPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1 };
            detailsPanel.setLayout(dataPanelLayout);
            detailsPanel.add(getIdLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getStateLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getModuleLabel(),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getUowLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getWorkerLabel(),
                new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getStartLabel(),
                new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getEndLabel(),
                new GridBagConstraints(0, 6, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getCreatedLabel(),
                new GridBagConstraints(0, 7, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getRevisionLabel(),
                new GridBagConstraints(0, 8, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getMiscPanel(), new GridBagConstraints(0, 9, 8, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getIdTextField(),
                new GridBagConstraints(1, 0, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getStateTextField(),
                new GridBagConstraints(1, 1, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getModuleTextField(),
                new GridBagConstraints(1, 2, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getUowTextField(),
                new GridBagConstraints(1, 3, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getWorkerTextField(),
                new GridBagConstraints(1, 4, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getStartTextField(),
                new GridBagConstraints(1, 5, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getEndTextField(),
                new GridBagConstraints(1, 6, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getCreatedTextField(),
                new GridBagConstraints(1, 7, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            detailsPanel.add(getRevisionTextField(),
                new GridBagConstraints(1, 8, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return detailsPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(this::closeButtonActionPerformed);
        }
        return closeButton;
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getDetailsPanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getMetricsPanel(), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JLabel getIdLabel() {
        if (idLabel == null) {
            idLabel = new JLabel();
            idLabel.setText("ID: ");
            idLabel.setHorizontalAlignment(SwingConstants.TRAILING);
        }
        return idLabel;
    }

    private JLabel getStateLabel() {
        if (stateLabel == null) {
            stateLabel = new JLabel();
            stateLabel.setText("State: ");
        }
        return stateLabel;
    }

    private JLabel getModuleLabel() {
        if (moduleLabel == null) {
            moduleLabel = new JLabel();
            moduleLabel.setText("Module Name: ");
        }
        return moduleLabel;
    }

    private JLabel getUowLabel() {
        if (uowLabel == null) {
            uowLabel = new JLabel();
            uowLabel.setText("Unit of Work: ");
        }
        return uowLabel;
    }

    private JLabel getWorkerLabel() {
        if (workerLabel == null) {
            workerLabel = new JLabel();
            workerLabel.setText("Worker (host:thread): ");
        }
        return workerLabel;
    }

    private JLabel getStartLabel() {
        if (startLabel == null) {
            startLabel = new JLabel();
            startLabel.setText("Start Time: ");
        }
        return startLabel;
    }

    private JLabel getEndLabel() {
        if (endLabel == null) {
            endLabel = new JLabel();
            endLabel.setText("End Time: ");
        }
        return endLabel;
    }

    private JLabel getCreatedLabel() {
        if (createdLabel == null) {
            createdLabel = new JLabel();
            createdLabel.setText("Create Time: ");
        }
        return createdLabel;
    }

    private JLabel getRevisionLabel() {
        if (revisionLabel == null) {
            revisionLabel = new JLabel();
            revisionLabel.setText("Software Revision: ");
        }
        return revisionLabel;
    }

    private JPanel getMiscPanel() {
        if (miscPanel == null) {
            miscPanel = new JPanel();
            FlowLayout miscPanelLayout = new FlowLayout();
            miscPanelLayout.setHgap(20);
            miscPanel.setLayout(miscPanelLayout);
            miscPanel.add(getFailureCountLabel());
            miscPanel.add(getFailCountTextField());
            miscPanel.add(getTransitionCheckBox());
        }
        return miscPanel;
    }

    private JLabel getFailureCountLabel() {
        if (failureCountLabel == null) {
            failureCountLabel = new JLabel();
            failureCountLabel.setText("Failure Count");
        }
        return failureCountLabel;
    }

    private JTextField getFailCountTextField() {
        if (failCountTextField == null) {
            failCountTextField = new JTextField();
            failCountTextField.setEditable(false);
            failCountTextField.setColumns(2);
        }
        return failCountTextField;
    }

    private JCheckBox getTransitionCheckBox() {
        if (transitionCheckBox == null) {
            transitionCheckBox = new JCheckBox();
            transitionCheckBox.setText("Transition Complete");
            transitionCheckBox.setSelected(true);
            transitionCheckBox.addActionListener(evt -> transitionCheckBoxActionPerformed());
        }
        return transitionCheckBox;
    }

    private JTextField getIdTextField() {
        if (idTextField == null) {
            idTextField = new JTextField();
            idTextField.setColumns(10);
            idTextField.setEditable(false);
        }
        return idTextField;
    }

    private JTextField getStateTextField() {
        if (stateTextField == null) {
            stateTextField = new JTextField();
            stateTextField.setText("COMPLETED");
            stateTextField.setColumns(50);
            stateTextField.setEditable(false);
        }
        return stateTextField;
    }

    private JTextField getModuleTextField() {
        if (moduleTextField == null) {
            moduleTextField = new JTextField();
            moduleTextField.setText("pa");
            moduleTextField.setEditable(false);
            moduleTextField.setColumns(50);
        }
        return moduleTextField;
    }

    private JTextField getUowTextField() {
        if (uowTextField == null) {
            uowTextField = new JTextField();
            uowTextField.setColumns(50);
            uowTextField.setEditable(false);
        }
        return uowTextField;
    }

    private JTextField getWorkerTextField() {
        if (workerTextField == null) {
            workerTextField = new JTextField();
            workerTextField.setColumns(50);
            workerTextField.setEditable(false);
        }
        return workerTextField;
    }

    private JTextField getStartTextField() {
        if (startTextField == null) {
            startTextField = new JTextField();
            startTextField.setEditable(false);
            startTextField.setColumns(50);
        }
        return startTextField;
    }

    private JTextField getEndTextField() {
        if (endTextField == null) {
            endTextField = new JTextField();
            endTextField.setColumns(50);
            endTextField.setEditable(false);
        }
        return endTextField;
    }

    private JTextField getCreatedTextField() {
        if (createdTextField == null) {
            createdTextField = new JTextField();
            createdTextField.setEditable(false);
            createdTextField.setColumns(50);
        }
        return createdTextField;
    }

    private JTextField getRevisionTextField() {
        if (revisionTextField == null) {
            revisionTextField = new JTextField();
            revisionTextField.setColumns(50);
            revisionTextField.setEditable(false);
        }
        return revisionTextField;
    }

    private JPanel getMetricsPanel() {
        if (processingBreakdownPanel == null) {
            processingBreakdownPanel = new JPanel();
            BorderLayout metricsPanelLayout = new BorderLayout();
            processingBreakdownPanel.setLayout(metricsPanelLayout);
            processingBreakdownPanel.add(getMetricsScrollPane(), BorderLayout.CENTER);
        }
        return processingBreakdownPanel;
    }

    private JScrollPane getMetricsScrollPane() {
        if (processingBreakdownScrollPane == null) {
            processingBreakdownScrollPane = new JScrollPane();
            processingBreakdownScrollPane.setViewportView(getMetricsTable());
        }
        return processingBreakdownScrollPane;
    }

    private ZTable getMetricsTable() {
        if (processingBreakdownTable == null) {
            List<PipelineTask> tasks = new ArrayList<>();
            tasks.add(pipelineTask);

            String moduleName = pipelineTask.getPipelineInstanceNode()
                .getPipelineModuleDefinition()
                .getName()
                .getName();
            List<String> moduleNames = new ArrayList<>();
            moduleNames.add(moduleName);

            processingBreakdownTableModel = new TaskMetricsTableModel(tasks, moduleNames, false);
            processingBreakdownTable = new ZTable();
            processingBreakdownTable.setTextWrappingEnabled(true);
            processingBreakdownTable.setModel(processingBreakdownTableModel);
        }
        return processingBreakdownTable;
    }

    /**
     * Auto-generated main method to display this JDialog
     */

    public TaskInfoDialog(JFrame frame) {
        super(frame);
        initGUI();
    }
}
