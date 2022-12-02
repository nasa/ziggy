package gov.nasa.ziggy.ui.ops.triggers;

import static gov.nasa.ziggy.ui.common.LabelUtils.boldLabel;

import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.text.NumberFormatter;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.module.AlgorithmExecutor;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.RemoteQueueDescriptor;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.common.ExecuteOnValidityCheck;
import gov.nasa.ziggy.ui.common.ToolTipBorderJPanel;
import gov.nasa.ziggy.ui.common.ValidityTestingFormattedTextField;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * Dialog box that allows the user to set values in an instance of {@link RemoteParameters} and
 * determine the resulting values in a corresponding {@link PbsParameters} instance.
 *
 * @author PT
 */
public class RemoteExecutionDialog extends JDialog {

    private static final long serialVersionUID = 20210810L;

    // Data model
    private RemoteParameters originalParameters;
    private RemoteParameters currentParameters;
    private PipelineDefinitionNode node;
    private int subtaskCount;
    private int maxParallelSubtaskCount;
    private ParameterSet parameterSet;
    private int originalSubtaskCount;
    private int originalMaxParallelSubtaskCount;

    // Dialog box elements
    private JPanel subtasksPanel;
    private JPanel remoteParamsPanel;
    private JPanel pbsParamsPanel;
    private JPanel remoteParamsValuesPanel;
    private JPanel remoteParamsRequiredValuesPanel;
    private JPanel remoteParamsOptionalValuesPanel;
    private JPanel remoteParamsButtonsPanel;
    private JPanel nodeSharingPanel;

    private ValidityTestingFormattedTextField subtasksField;
    private ValidityTestingFormattedTextField gigsPerSubtaskField;
    private ValidityTestingFormattedTextField maxWallTimeField;
    private ValidityTestingFormattedTextField wallTimeRatioField;
    private JComboBox<RemoteArchitectureOptimizer> optimizerComboBox;
    private JComboBox<String> architectureComboBox;
    private JComboBox<String> queueComboBox;
    private ValidityTestingFormattedTextField maxNodesField;
    private ValidityTestingFormattedTextField subtasksPerCoreField;
    private JButton resetButton;
    private JButton calculateButton;
    private JButton saveButton;
    private JButton tasksButton;
    private JCheckBox nodeSharingCheckBox = new JCheckBox("Node Sharing");
    private JCheckBox wallTimeScalingCheckBox = new JCheckBox("Wall Time Scaling");

    private JLabel pbsArch = boldLabel();
    private JLabel pbsQueue = boldLabel();
    private JLabel pbsWallTime = boldLabel();
    private JLabel pbsNodeCount = boldLabel();
    private JLabel pbsActiveCoresPerNode = boldLabel();
    private JLabel pbsCost = boldLabel();

    private Set<ValidityTestingFormattedTextField> validityTestingFormattedTextFields = new HashSet<>();

    public RemoteExecutionDialog(Dialog parent, String title, ParameterSetName paramSetName,
        PipelineDefinitionNode node, int subtaskCount, int maxParallelSubtaskCount) {
        super(parent, true);
        setTitle("Remote Execution: " + title);
        PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
        parameterSet = pipelineOps.retrieveLatestParameterSet(paramSetName);
        originalParameters = parameterSet.parametersInstance();
        currentParameters = new RemoteParameters(originalParameters);
        this.node = node;
        this.subtaskCount = subtaskCount;
        this.maxParallelSubtaskCount = maxParallelSubtaskCount;
        originalSubtaskCount = subtaskCount;
        originalMaxParallelSubtaskCount = maxParallelSubtaskCount;

        initGUI();
        setVisible(true);
    }

    private void initGUI() {
        setSize(650, 400);
        getContentPane().add(subtasksPanel(), BorderLayout.NORTH);
        getContentPane().add(remoteParamsPanel(), BorderLayout.CENTER);
        getContentPane().add(getPbsParamsPanel(), BorderLayout.SOUTH);
        populateTextFieldsAndComboBoxes();
    }

    private JPanel subtasksPanel() {
        if (subtasksPanel == null) {
            subtasksPanel = new ToolTipBorderJPanel();
            subtasksPanel.setToolTipText("Controls the number of subtasks for the selected module");
            subtasksPanel.setBorder(BorderFactory.createTitledBorder("Subtasks"));
            FlowLayout subtasksLayout = new FlowLayout();
            subtasksPanel.setLayout(subtasksLayout);
            JLabel subtasksLabel = new JLabel();
            subtasksLabel.setText("Total subtasks: ");
            subtasksPanel.add(subtasksLabel);
            subtasksPanel.add(subtasksField());
        }
        return subtasksPanel;
    }

    private void setButtonState() {
        boolean allFieldsValid = true;
        for (ValidityTestingFormattedTextField field : validityTestingFormattedTextFields) {
            allFieldsValid = allFieldsValid && field.isValidState();
        }
        getCalculateButton().setEnabled(allFieldsValid);
        getSaveButton().setEnabled(allFieldsValid);
    }

    private ExecuteOnValidityCheck checkFieldsAndEnableButtons = valid -> setButtonState();

    private ValidityTestingFormattedTextField subtasksField() {
        if (subtasksField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Integer.class);
            formatter.setMinimum(1);
            formatter.setMaximum(Integer.MAX_VALUE);
            subtasksField = new ValidityTestingFormattedTextField(formatter);
            subtasksField.setColumns(6);
            subtasksField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(subtasksField);
            subtasksField.setToolTipText("Set the total number of subtasks (must be >= 1).");
        }
        return subtasksField;
    }

    private JPanel remoteParamsPanel() {
        if (remoteParamsPanel == null) {
            remoteParamsPanel = new ToolTipBorderJPanel();
            remoteParamsPanel.setBorder(BorderFactory
                .createTitledBorder(BorderFactory.createEmptyBorder(), "Remote Parameters"));
            remoteParamsPanel
                .setToolTipText("Controls user-settable parameters for remote execution");
            remoteParamsPanel.add(remoteParamsValuesPanel(), BorderLayout.NORTH);
            remoteParamsPanel.add(getRemoteParamsButtonsPanel(), BorderLayout.SOUTH);
        }
        return remoteParamsPanel;
    }

    private JPanel remoteParamsValuesPanel() {
        if (remoteParamsValuesPanel == null) {
            remoteParamsValuesPanel = new JPanel();
            remoteParamsValuesPanel.setLayout(remoteParamsLayout());
            remoteParamsValuesPanel.add(nodeSharingPanel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsValuesPanel.add(remoteParamsRequiredValuesPanel());
            remoteParamsValuesPanel.add(remoteParamsOptionalValuesPanel());
            populateTextFieldsAndComboBoxes();
        }
        return remoteParamsValuesPanel;
    }

    private static LayoutManager remoteParamsLayout() {
        GridBagLayout layout = new GridBagLayout();
        layout.columnWidths = new int[] { 15, 30, 30 };
        layout.rowHeights = new int[] { 15 };
        return layout;
    }

    private JPanel nodeSharingPanel() {
        if (nodeSharingPanel == null) {
            nodeSharingPanel = new ToolTipBorderJPanel();
            nodeSharingPanel.setBorder(BorderFactory.createTitledBorder("Node Sharing"));
            nodeSharingPanel
                .setToolTipText("Controls parallel processing of subtasks on each node");
            nodeSharingPanel.setLayout(new GridLayout(2, 1));
            nodeSharingPanel.add(nodeSharingCheckBox);
            nodeSharingCheckBox
                .setToolTipText("Enables concurrent processing of multiple subtasks on each node");
            nodeSharingCheckBox.addItemListener(e -> nodeSharingCheckBoxEvent());
            nodeSharingPanel.add(wallTimeScalingCheckBox);
            wallTimeScalingCheckBox.setToolTipText(
                "Scales subtask wall times inversely to the number of cores per node\n"
                    + "Only enabled when node sharing is disabled");
        }
        return nodeSharingPanel;
    }

    private void nodeSharingCheckBoxEvent() {
        wallTimeScalingCheckBox.setEnabled(!nodeSharingCheckBox.isSelected());
        gigsPerSubtaskField().setEnabled(nodeSharingCheckBox.isSelected());
        setGigsPerSubtaskToolTip();
    }

    private JPanel remoteParamsRequiredValuesPanel() {
        if (remoteParamsRequiredValuesPanel == null) {
            remoteParamsRequiredValuesPanel = new ToolTipBorderJPanel();
            remoteParamsRequiredValuesPanel.setBorder(BorderFactory.createTitledBorder("Required"));
            remoteParamsRequiredValuesPanel.setToolTipText("Parameters that must be set.");
            GridBagLayout requiredValuesLayout = new GridBagLayout();
            remoteParamsRequiredValuesPanel.setLayout(requiredValuesLayout);
            requiredValuesLayout.columnWidths = new int[] { 7, 7 };
            requiredValuesLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            requiredValuesLayout.columnWeights = new double[] { 0.1, 0.1 };
            requiredValuesLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            remoteParamsRequiredValuesPanel.add(new JLabel("Gigs per Subtask"),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(new JLabel("Max Subtask Wall Time"),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(new JLabel("Subtask Wall Time Ratio"),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(new JLabel("Optimizer"),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

            remoteParamsRequiredValuesPanel.add(gigsPerSubtaskField(),
                new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(maxWallTimeField(),
                new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(typicalWallTimeField(),
                new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsRequiredValuesPanel.add(optimizerComboBox(),
                new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return remoteParamsRequiredValuesPanel;
    }

    private ValidityTestingFormattedTextField gigsPerSubtaskField() {
        if (gigsPerSubtaskField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Double.class);
            formatter.setMinimum(Double.MIN_VALUE);
            formatter.setMaximum(Double.MAX_VALUE);
            gigsPerSubtaskField = new ValidityTestingFormattedTextField(formatter);
            gigsPerSubtaskField.setColumns(6);
            gigsPerSubtaskField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(gigsPerSubtaskField);
        }
        return gigsPerSubtaskField;
    }

    private void setGigsPerSubtaskToolTip() {
        String enabledTip = "Enter the number of GB needed for each subtask (>0).";
        String disabledTip = "Gigs per subtask disabled when node sharing is disabled";
        String tip = gigsPerSubtaskField().isEnabled() ? enabledTip : disabledTip;
        gigsPerSubtaskField().setToolTipText(tip);
    }

    private ValidityTestingFormattedTextField maxWallTimeField() {
        if (maxWallTimeField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Double.class);
            formatter.setMinimum(Double.MIN_VALUE);
            formatter.setMaximum(Double.MAX_VALUE);
            maxWallTimeField = new ValidityTestingFormattedTextField(formatter);
            maxWallTimeField.setColumns(6);
            maxWallTimeField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(maxWallTimeField);
            maxWallTimeField.addFocusListener(new FocusListener() {

                @Override
                public void focusGained(FocusEvent e) {
                }

                // Here is where we modify the typical field when the max field has been
                // updated
                @Override
                public void focusLost(FocusEvent e) {
                    if (maxWallTimeField.isValidState()) {
                        ValidityTestingFormattedTextField typicalField = typicalWallTimeField();
                        double maxValue = (double) maxWallTimeField.getValue();
                        double typicalValue = (double) typicalField.getValue();
                        NumberFormatter formatter = (NumberFormatter) typicalField.getFormatter();
                        formatter.setMaximum(maxValue);
                        if (typicalValue > maxValue) {
                            typicalField
                                .setBorder(ValidityTestingFormattedTextField.INVALID_BORDER);
                        }
                    }
                }

            });
            maxWallTimeField.setToolTipText(
                "Enter the MAXIMUM wall time needed by any subtask, in hours (>0).");
        }
        return maxWallTimeField;
    }

    private ValidityTestingFormattedTextField typicalWallTimeField() {
        if (wallTimeRatioField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Double.class);
            formatter.setMinimum(Double.MIN_VALUE);
            formatter.setMaximum(currentParameters.getSubtaskMaxWallTimeHours());
            wallTimeRatioField = new ValidityTestingFormattedTextField(formatter);
            wallTimeRatioField.setColumns(6);
            wallTimeRatioField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(wallTimeRatioField);
            wallTimeRatioField.setToolTipText(
                "Enter the TYPICAL wall time needed by subtasks, in hours (>0, <= max wall time)");
        }
        return wallTimeRatioField;
    }

    private JComboBox<RemoteArchitectureOptimizer> optimizerComboBox() {
        if (optimizerComboBox == null) {
            optimizerComboBox = new JComboBox<>(RemoteArchitectureOptimizer.values());
            optimizerComboBox
                .setToolTipText("<html>Select the optimizer used to pick a remote architecture:<br>"
                    + "&ensp;CORES minimizes the number of idle cores.<br>"
                    + "&ensp;QUEUE_DEPTH minimizes the number of hours of queued tasks.<br>"
                    + "&ensp;QUEUE_TIME minimizes the estimated total time including time in the queue.<br>"
                    + "&ensp;COST minimizes the number of SBUs.</html>");
        }
        return optimizerComboBox;
    }

    private JPanel remoteParamsOptionalValuesPanel() {
        if (remoteParamsOptionalValuesPanel == null) {
            remoteParamsOptionalValuesPanel = new ToolTipBorderJPanel();
            remoteParamsOptionalValuesPanel.setBorder(BorderFactory.createTitledBorder("Optional"));
            remoteParamsOptionalValuesPanel
                .setToolTipText("Parameters that can be calculated if not set. "
                    + "Values set by users will be included when calculating PBS parameters.");
            GridBagLayout optionalValuesLayout = new GridBagLayout();
            remoteParamsOptionalValuesPanel.setLayout(optionalValuesLayout);
            optionalValuesLayout.columnWidths = new int[] { 7, 7 };
            optionalValuesLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            optionalValuesLayout.columnWeights = new double[] { 0.1, 0.1 };
            optionalValuesLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };

            remoteParamsOptionalValuesPanel.add(new JLabel("Architecture"),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(new JLabel("Queue"),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(new JLabel("Max Nodes"),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(new JLabel("Subtasks per Core"),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

            remoteParamsOptionalValuesPanel.add(architectureComboBox(),
                new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(queueComboBox(),
                new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(maxNodesField(),
                new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            remoteParamsOptionalValuesPanel.add(subtasksPerCoreField(),
                new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        }
        return remoteParamsOptionalValuesPanel;
    }

    private JComboBox<String> architectureComboBox() {
        if (architectureComboBox == null) {
            architectureComboBox = new JComboBox<>(RemoteNodeDescriptor.allNames());
            architectureComboBox.insertItemAt("", 0);
            architectureComboBox.setToolTipText("Select remote node architecture.");
        }
        return architectureComboBox;
    }

    private JComboBox<String> queueComboBox() {
        if (queueComboBox == null) {
            queueComboBox = new JComboBox<>(RemoteQueueDescriptor.allNames());
            queueComboBox.insertItemAt("", 0);
            queueComboBox.setToolTipText("Select batch queue for use with these jobs.");
        }
        return queueComboBox;
    }

    private ValidityTestingFormattedTextField maxNodesField() {
        if (maxNodesField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Integer.class);
            formatter.setMinimum(1);
            formatter.setMaximum(Integer.MAX_VALUE);
            maxNodesField = new ValidityTestingFormattedTextField(formatter);
            maxNodesField.setColumns(6);
            maxNodesField.setEmptyIsValid(true);
            maxNodesField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(maxNodesField);
            maxNodesField
                .setToolTipText("<html>Enter the maximum number of nodes to request (>=1)<br>"
                    + "or leave empty to let the algorithm determine the number.");
        }
        return maxNodesField;
    }

    private ValidityTestingFormattedTextField subtasksPerCoreField() {
        if (subtasksPerCoreField == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Double.class);
            formatter.setMinimum(1.0);
            formatter.setMaximum(Double.MAX_VALUE);
            subtasksPerCoreField = new ValidityTestingFormattedTextField(formatter);
            subtasksPerCoreField.setColumns(6);
            subtasksPerCoreField.setEmptyIsValid(true);
            subtasksPerCoreField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
            validityTestingFormattedTextFields.add(subtasksPerCoreField);
            subtasksPerCoreField
                .setToolTipText("<html>Enter the number of subtasks per active core (>=1)<br>"
                    + "or leave empty to let the algorithm decide the number");
        }
        return subtasksPerCoreField;
    }

    private void populateTextFieldsAndComboBoxes() {

        // Subtask counts
        subtasksField().setValue(subtaskCount);

        // Required parameters
        if (!StringUtils.isEmpty(currentParameters.getOptimizer())
            && RemoteArchitectureOptimizer.fromName(currentParameters.getOptimizer()) != null) {
            optimizerComboBox.setSelectedItem(
                RemoteArchitectureOptimizer.fromName(currentParameters.getOptimizer()));
        }
        typicalWallTimeField().setValue(currentParameters.getSubtaskTypicalWallTimeHours());
        maxWallTimeField().setValue(currentParameters.getSubtaskMaxWallTimeHours());
        gigsPerSubtaskField().setValue(currentParameters.getGigsPerSubtask());
        nodeSharingCheckBox.setSelected(currentParameters.isNodeSharing());
        wallTimeScalingCheckBox.setSelected(currentParameters.isWallTimeScaling());
        wallTimeScalingCheckBox.setEnabled(!currentParameters.isNodeSharing());
        gigsPerSubtaskField().setEnabled(currentParameters.isNodeSharing());
        setGigsPerSubtaskToolTip();

        // Optional parameters
        if (!StringUtils.isEmpty(currentParameters.getRemoteNodeArchitecture())
            && RemoteNodeDescriptor
                .fromName(currentParameters.getRemoteNodeArchitecture()) != null) {
            architectureComboBox().setSelectedItem(
                RemoteNodeDescriptor.fromName(currentParameters.getRemoteNodeArchitecture())
                    .name());
        } else {
            architectureComboBox().setSelectedIndex(0);
        }

        if (!StringUtils.isEmpty(currentParameters.getQueueName())
            && RemoteQueueDescriptor.fromName(currentParameters.getQueueName()) != null) {
            queueComboBox().setSelectedItem(
                RemoteNodeDescriptor.fromName(currentParameters.getQueueName()).name());
        } else {
            queueComboBox().setSelectedIndex(0);
        }

        if (!StringUtils.isEmpty(currentParameters.getMaxNodes())) {
            maxNodesField().setValue(Integer.parseInt(currentParameters.getMaxNodes()));
        }
        if (!StringUtils.isEmpty(currentParameters.getSubtasksPerCore())) {
            subtasksPerCoreField()
                .setValue(Double.parseDouble(currentParameters.getSubtasksPerCore()));
        }

    }

    private void populateCurrentParameters() {

        // Subtask counts.
        subtaskCount = (int) subtasksField().getValue();

        // Required parameters.
        RemoteArchitectureOptimizer optimizerSelection = (RemoteArchitectureOptimizer) optimizerComboBox()
            .getSelectedItem();
        currentParameters.setOptimizer(optimizerSelection.name());
        currentParameters
            .setSubtaskTypicalWallTimeHours((double) typicalWallTimeField().getValue());
        currentParameters.setSubtaskMaxWallTimeHours((double) maxWallTimeField.getValue());
        currentParameters.setGigsPerSubtask((double) gigsPerSubtaskField().getValue());
        currentParameters.setNodeSharing(nodeSharingCheckBox.isSelected());
        currentParameters.setWallTimeScaling(wallTimeScalingCheckBox.isSelected());

        // Optional parameters.
        String choice = (String) architectureComboBox().getSelectedItem();
        if (!StringUtils.isEmpty(choice)) {
            RemoteNodeDescriptor chosenArch = RemoteNodeDescriptor.valueOf(choice);
            currentParameters.setRemoteNodeArchitecture(chosenArch.getNodeName());
        } else {
            currentParameters.setRemoteNodeArchitecture("");
        }

        choice = (String) queueComboBox().getSelectedItem();
        if (!StringUtils.isEmpty(choice)) {
            RemoteQueueDescriptor chosenQueue = RemoteQueueDescriptor.valueOf(choice);
            currentParameters.setQueueName(chosenQueue.getQueueName());
        } else {
            currentParameters.setQueueName("");
        }

        Object maxNodesValue = maxNodesField().getValue();
        if (maxNodesValue != null) {
            currentParameters.setMaxNodes(Integer.toString((int) maxNodesValue));
        } else {
            currentParameters.setMaxNodes("");
        }

        Object subtasksPerCoreValue = subtasksPerCoreField().getValue();
        if (subtasksPerCoreValue != null) {
            currentParameters.setSubtasksPerCore(Double.toString((double) subtasksPerCoreValue));
        } else {
            currentParameters.setSubtasksPerCore("");
        }
    }

    private JPanel getRemoteParamsButtonsPanel() {
        if (remoteParamsButtonsPanel == null) {
            remoteParamsButtonsPanel = new JPanel();
            remoteParamsButtonsPanel.add(getResetButton());
            remoteParamsButtonsPanel.add(getCalculateButton());
            remoteParamsButtonsPanel.add(getSaveButton());
            remoteParamsButtonsPanel.add(getTasksButton());
        }
        return remoteParamsButtonsPanel;
    }

    private JButton getResetButton() {
        if (resetButton == null) {
            resetButton = new JButton("Reset to DB Values");
            resetButton.addActionListener(e -> {
                PipelineTaskInformation.reset(node);
                currentParameters = new RemoteParameters(originalParameters);
                subtaskCount = originalSubtaskCount;
                maxParallelSubtaskCount = originalMaxParallelSubtaskCount;
                populateTextFieldsAndComboBoxes();
                displayPbsValues(null);
            });
            resetButton.setToolTipText("Reset RemoteParameters values to original (DB) values.");
        }
        return resetButton;
    }

    private JButton getCalculateButton() {
        if (calculateButton == null) {
            calculateButton = new JButton("Calculate PBS Parameters");
            calculateButton
                .setToolTipText("Generate PBS parameters from the RemoteParameters values.");
            calculateButton.addActionListener(e -> {
                calculatePbsParameters();
            });

            // NB: the FocusListener is needed because of the way that the field values are
            // validated. When one of the ValidityTestingFormattedTextBox instances loses the
            // focus, the resulting event causes the validity of that box to become false for
            // a blink, which causes the calculate and save buttons to become disabled for a
            // blink; as a result, the buttons are disabled when the action listener would tell
            // them to take their actions, and the user has to hit the button again to get it to
            // take its action. By performing the action when the button gains focus, we get
            // back to needing only one button push to get the focus and perform the action.
            calculateButton.addFocusListener(new FocusListener() {

                @Override
                public void focusGained(FocusEvent e) {
                    if (calculateButton.isEnabled()) {
                        calculatePbsParameters();
                    }
                }

                @Override
                public void focusLost(FocusEvent e) {
                }
            });
        }
        return calculateButton;
    }

    private void calculatePbsParameters() {
        calculateButton.setEnabled(false);
        try {
            populateCurrentParameters();
            AlgorithmExecutor executor = AlgorithmExecutor.newRemoteInstance(null);
            String currentOptimizer = currentParameters.getOptimizer();
            if (!currentParameters.isNodeSharing()
                && currentOptimizer.equals(RemoteArchitectureOptimizer.CORES.name())) {
                JOptionPane.showMessageDialog(this,
                    "CORES optimization disabled when node sharing disabled.\n"
                        + "COST optimization will be used instead.");
                currentParameters.setOptimizer(RemoteArchitectureOptimizer.COST.name());
            }
            PbsParameters pbsParameters = executor.generatePbsParameters(currentParameters,
                subtaskCount);
            displayPbsValues(pbsParameters);
            currentParameters.setOptimizer(currentOptimizer);
            currentOptimizer = null;
        } catch (Exception f) {
            boolean handled = false;
            if (f instanceof IllegalStateException | f instanceof PipelineException) {
                handled = handlePbsParametersException(f.getStackTrace());
            }
            if (!handled) {
                throw f;
            }
        } finally {
            calculateButton.setEnabled(true);
        }
    }

    private boolean handlePbsParametersException(StackTraceElement[] stackTrace) {
        boolean handled = false;
        String message = null;
        for (StackTraceElement element : stackTrace) {
            if (element.getClassName().equals(PbsParameters.class.getName())) {
                if (element.getMethodName().equals("populateArchitecture")) {
                    message = "Selected architecture has insufficient RAM";
                    break;
                }
                if (element.getMethodName().equals("selectArchitecture")) {
                    message = "All architectures lack sufficient RAM";
                    break;
                }
                if (element.getMethodName().equals("computeWallTimeAndQueue")) {
                    message = "No queue exists with sufficiently high time limit";
                    break;
                }
            }
        }
        if (message != null) {
            handled = true;
            JOptionPane.showMessageDialog(this, message);
        }
        return handled;
    }

    private JButton getSaveButton() {
        if (saveButton == null) {
            saveButton = new JButton("Save to DB");
            saveButton
                .setToolTipText("Save RemoteParameters values to DB, replacing current values.");
            saveButton.addActionListener(e -> saveRemoteParameters());

            // NB: the FocusListener is needed because of the way that the field values are
            // validated. When one of the ValidityTestingFormattedTextBox instances loses the
            // focus, the resulting event causes the validity of that box to become false for
            // a blink, which causes the calculate and save buttons to become disabled for a
            // blink; as a result, the buttons are disabled when the action listener would tell
            // them to take their actions, and the user has to hit the button again to get it to
            // take its action. By performing the action when the button gains focus, we get
            // back to needing only one button push to get the focus and perform the action.
            saveButton.addFocusListener(new FocusListener() {

                @Override
                public void focusGained(FocusEvent e) {
                    if (saveButton.isEnabled()) {
                        saveRemoteParameters();
                    }
                }

                @Override
                public void focusLost(FocusEvent e) {
                }
            });
        }
        return saveButton;
    }

    private void saveRemoteParameters() {
        PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
        pipelineOps.updateParameterSet(parameterSet, currentParameters,
            parameterSet.getDescription(), false);
        originalParameters = new RemoteParameters(currentParameters);
        originalSubtaskCount = subtaskCount;
        originalMaxParallelSubtaskCount = maxParallelSubtaskCount;
    }

    private JButton getTasksButton() {
        if (tasksButton == null) {
            tasksButton = new JButton("Display Task Info");
            tasksButton
                .setToolTipText("Display task and subtask information for this pipeline node.");
            tasksButton.addActionListener(e -> displayTaskInformation());
        }
        return tasksButton;
    }

    private void displayTaskInformation() {
        TaskInformationDialog infoTable = new TaskInformationDialog(this, node);
        infoTable.setVisible(true);
    }

    private JPanel getPbsParamsPanel() {
        if (pbsParamsPanel == null) {
            pbsParamsPanel = new ToolTipBorderJPanel();
            pbsParamsPanel.setBorder(BorderFactory.createTitledBorder("PBS Parameters"));
            pbsParamsPanel.setToolTipText("Displays parameters that will be sent to PBS");
            pbsParamsPanel.setLayout(new GridLayout(6, 2));
            pbsParamsPanel.add(new JLabel("Architecture:    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsArch);
            pbsParamsPanel.add(new JLabel("Queue:    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsQueue);
            pbsParamsPanel.add(new JLabel("Wall Time:    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsWallTime);
            pbsParamsPanel.add(new JLabel("Node Count:    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsNodeCount);
            pbsParamsPanel.add(new JLabel("Active Cores per Node:    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsActiveCoresPerNode);
            pbsParamsPanel.add(new JLabel("Cost (SBUs):    ", SwingConstants.RIGHT));
            pbsParamsPanel.add(pbsCost);

        }
        return pbsParamsPanel;
    }

    private void displayPbsValues(PbsParameters pbsParameters) {
        if (pbsParameters == null) {
            pbsArch.setText("  ");
            pbsQueue.setText("  ");
            pbsWallTime.setText("  ");
            pbsNodeCount.setText("  ");
            pbsActiveCoresPerNode.setText("  ");
            pbsCost.setText("  ");
        } else {
            pbsArch.setText(pbsParameters.getArchitecture().name());
            pbsQueue.setText(pbsParameters.getQueueName());
            pbsWallTime.setText(pbsParameters.getRequestedWallTime());
            pbsNodeCount.setText(Integer.toString(pbsParameters.getRequestedNodeCount()));
            pbsActiveCoresPerNode.setText(Integer.toString(pbsParameters.getActiveCoresPerNode()));
            pbsCost.setText(String.format("%.2f", pbsParameters.getEstimatedCost()));
        }
    }

}
