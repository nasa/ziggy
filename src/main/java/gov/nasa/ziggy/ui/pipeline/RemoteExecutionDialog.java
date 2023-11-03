package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.text.NumberFormatter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.RemoteQueueDescriptor;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.util.ValidityTestingFormattedTextField;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;

/**
 * Dialog box that allows the user to set values in an instance of {@link RemoteParameters} and
 * determine the resulting values in a corresponding {@link PbsParameters} instance.
 *
 * @author PT
 * @author Bill Wohler
 */
public class RemoteExecutionDialog extends JDialog {

    @SuppressWarnings("unused")
    private static Logger log = LoggerFactory.getLogger(RemoteExecutionDialog.class);

    /** Minimum width to ensure {@code pack()} allows for the initially empty fields. */
    private static final int PBS_PARAMETERS_MINIMUM_WIDTH = 100;

    private static final long serialVersionUID = 20230927L;

    // Reserved queue name: this name is a static variable so that it's "sticky,"
    // i.e., once the user sets a reserved queue name it sticks around until the user
    // changes it to a different value, loads a different value by selecting a pipeline
    // node that's using a different reserved queue, or stops and restarts the console.
    private static String reservedQueueName = "";

    // Data model
    private RemoteParameters originalParameters;
    private RemoteParameters currentParameters;
    private PipelineDefinitionNode node;
    private int subtaskCount;
    private int maxParallelSubtaskCount;
    private ParameterSet parameterSet;
    private int originalSubtaskCount;
    private int originalMaxParallelSubtaskCount;
    private List<SubtaskInformation> tasksInformation;

    // Dialog box elements
    private ValidityTestingFormattedTextField subtasksField;
    private ValidityTestingFormattedTextField gigsPerSubtaskField;
    private ValidityTestingFormattedTextField maxWallTimeField;
    private ValidityTestingFormattedTextField wallTimeRatioField;
    private JComboBox<RemoteArchitectureOptimizer> optimizerComboBox;
    private JComboBox<RemoteNodeDescriptor> architectureComboBox;
    private JComboBox<RemoteQueueDescriptor> queueComboBox;
    private ValidityTestingFormattedTextField maxNodesField;
    private ValidityTestingFormattedTextField subtasksPerCoreField;
    private JButton calculateButton;
    private JCheckBox nodeSharingCheckBox;
    private JCheckBox wallTimeScalingCheckBox;

    private JLabel pbsArch;
    private JLabel pbsQueue;
    private JLabel pbsWallTime;
    private JLabel pbsNodeCount;
    private JLabel pbsActiveCoresPerNode;
    private JLabel pbsCost;

    private Set<ValidityTestingFormattedTextField> validityTestingFormattedTextFields = new HashSet<>();
    private Consumer<Boolean> checkFieldsAndEnableButtons = valid -> setButtonState();

    public RemoteExecutionDialog(Window owner, ParameterSet originalParameterSet,
        PipelineDefinitionNode node, List<SubtaskInformation> tasksInformation) {

        super(owner, DEFAULT_MODALITY_TYPE);
        parameterSet = originalParameterSet;
        originalParameters = originalParameterSet.parametersInstance();
        currentParameters = new RemoteParameters(originalParameters);
        this.node = node;
        this.tasksInformation = tasksInformation;
        for (SubtaskInformation taskInformation : tasksInformation) {
            subtaskCount += taskInformation.getSubtaskCount();
            maxParallelSubtaskCount += taskInformation.getMaxParallelSubtasks();
        }
        originalSubtaskCount = subtaskCount;
        originalMaxParallelSubtaskCount = maxParallelSubtaskCount;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit remote execution parameters");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(
            createButton(CLOSE, htmlBuilder("Close this dialog box.").appendBreak()
                .append(
                    "Your remote parameter changes won't be saved until you hit Save on the edit pipeline dialog.")
                .toString(), this::updateRemoteParameters),
            createButton(CANCEL, "Clear all changes made in this dialog box and close it.",
                this::cancel)),
            BorderLayout.SOUTH);

        populateTextFieldsAndComboBoxes();
        pack();
    }

    private JPanel createDataPanel() {
        JPanel remoteParametersToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("Reset",
                "Reset RemoteParameters values to the values at the start of this dialog box.",
                this::resetAction),
            createButton("Display task info",
                "Display task and subtask information for this pipeline node.",
                this::displayTaskInformation));
        remoteParametersToolBar
            .setToolTipText("Controls user-settable parameters for remote execution");

        JLabel moduleGroup = boldLabel("Module", LabelType.HEADING1);

        JLabel pipeline = boldLabel("Pipeline");
        JLabel pipelineText = new JLabel(node.getPipelineName());

        JLabel module = boldLabel("Module");
        JLabel moduleText = new JLabel(node.getModuleName());

        JLabel nodeSharingGroup = boldLabel("Node sharing", LabelType.HEADING1);
        nodeSharingGroup.setToolTipText("Controls parallel processing of subtasks on each node.");
        nodeSharingCheckBox = new JCheckBox("Node sharing");
        nodeSharingCheckBox
            .setToolTipText("Enables concurrent processing of multiple subtasks on each node.");
        nodeSharingCheckBox.addItemListener(this::nodeSharingCheckBoxEvent);
        wallTimeScalingCheckBox = new JCheckBox("Wall time scaling");
        wallTimeScalingCheckBox.setToolTipText(
            htmlBuilder("Scales subtask wall times inversely to the number of cores per node.")
                .appendBreak()
                .append("Only enabled when node sharing is disabled.")
                .toString());

        JLabel requiredRemoteParametersGroup = boldLabel("Required", LabelType.HEADING1);
        requiredRemoteParametersGroup.setToolTipText("Parameters that must be set.");

        JLabel subtasks = boldLabel("Total subtasks");
        subtasksField = createSubtasksField();
        validityTestingFormattedTextFields.add(subtasksField);

        JLabel gigsPerSubtask = boldLabel("Gigs per subtask");
        gigsPerSubtaskField = createGigsPerSubtaskField();
        validityTestingFormattedTextFields.add(gigsPerSubtaskField);

        JLabel maxWallTime = boldLabel("Max subtask wall time");
        maxWallTimeField = createMaxWallTimeField();
        validityTestingFormattedTextFields.add(maxWallTimeField);

        JLabel typicalWallTime = boldLabel("Typical subtask wall time");
        wallTimeRatioField = createTypicalWallTimeField();
        validityTestingFormattedTextFields.add(wallTimeRatioField);

        JLabel optimizer = boldLabel("Optimizer");
        optimizerComboBox = createOptimizerComboBox();

        JLabel optionalRemoteParametersGroup = boldLabel("Optional", LabelType.HEADING1);
        optionalRemoteParametersGroup.setToolTipText(
            htmlBuilder("Parameters that can be calculated if not set.").appendBreak()
                .append("Values set by users will be included when calculating PBS parameters.")
                .toString());

        JLabel architecture = boldLabel("Architecture");
        architectureComboBox = createArchitectureComboBox();

        JLabel queue = boldLabel("Queue");
        queueComboBox = createQueueComboBox();

        JLabel maxNodesPerTask = boldLabel("Max nodes per task");
        maxNodesField = createMaxNodesField();
        validityTestingFormattedTextFields.add(maxNodesField);

        JLabel subtasksPerCore = boldLabel("Subtasks per core");
        subtasksPerCoreField = createSubtasksPerCoreField();
        validityTestingFormattedTextFields.add(subtasksPerCoreField);

        JLabel pbsParametersGroup = boldLabel("PBS parameters", LabelType.HEADING1);
        pbsParametersGroup.setToolTipText("Displays parameters that will be sent to PBS.");

        calculateButton = createButton("Calculate",
            "Generate PBS parameters from the RemoteParameters values.",
            this::calculatePbsParameters);
        JPanel pbsParametersToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            calculateButton);

        JLabel pbsArchLabel = boldLabel("Architecture:");
        pbsArch = new JLabel();

        JLabel pbsQueueLabel = boldLabel("Queue:");
        pbsQueue = new JLabel();

        JLabel pbsWallTimeLabel = boldLabel("Wall time:");
        pbsWallTime = new JLabel();

        JLabel pbsNodeCountLabel = boldLabel("Node count:");
        pbsNodeCount = new JLabel();

        JLabel pbsActiveCoresPerNodeLabel = boldLabel("Active cores per node:");
        pbsActiveCoresPerNode = new JLabel();

        JLabel pbsCostLabel = boldLabel("Cost (SBUs):");
        pbsCost = new JLabel();

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(remoteParametersToolBar)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(moduleGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggySwingUtils.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(pipeline)
                            .addComponent(pipelineText)
                            .addComponent(module)
                            .addComponent(moduleText)))
                    .addComponent(nodeSharingGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggySwingUtils.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(nodeSharingCheckBox)
                            .addComponent(wallTimeScalingCheckBox)))
                    .addComponent(requiredRemoteParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggySwingUtils.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(subtasks)
                            .addComponent(subtasksField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(gigsPerSubtask)
                            .addComponent(gigsPerSubtaskField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(maxWallTime)
                            .addComponent(maxWallTimeField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(typicalWallTime)
                            .addComponent(wallTimeRatioField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(optimizer)
                            .addComponent(optimizerComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
                    .addComponent(optionalRemoteParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggySwingUtils.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(architecture)
                            .addComponent(architectureComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(queue)
                            .addComponent(queueComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(maxNodesPerTask)
                            .addComponent(maxNodesField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(subtasksPerCore)
                            .addComponent(subtasksPerCoreField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))))
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(pbsParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggySwingUtils.INDENT)
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addGroup(dataPanelLayout.createParallelGroup()
                                .addComponent(pbsParametersToolBar)
                                .addComponent(pbsArchLabel)
                                .addComponent(pbsQueueLabel)
                                .addComponent(pbsWallTimeLabel)
                                .addComponent(pbsNodeCountLabel)
                                .addComponent(pbsActiveCoresPerNodeLabel)
                                .addComponent(pbsCostLabel))
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addGroup(dataPanelLayout.createParallelGroup()
                                .addComponent(pbsArch, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)
                                .addComponent(pbsQueue, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)
                                .addComponent(pbsWallTime, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)
                                .addComponent(pbsNodeCount, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)
                                .addComponent(pbsActiveCoresPerNode, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)
                                .addComponent(pbsCost, PBS_PARAMETERS_MINIMUM_WIDTH,
                                    GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)))))));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(remoteParametersToolBar, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(moduleGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(pipeline)
                    .addComponent(pipelineText)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(module)
                    .addComponent(moduleText)
                    .addGap(ZiggySwingUtils.GROUP_GAP)
                    .addComponent(nodeSharingGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(nodeSharingCheckBox)
                    .addComponent(wallTimeScalingCheckBox)
                    .addGap(ZiggySwingUtils.GROUP_GAP)
                    .addComponent(requiredRemoteParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(subtasks)
                    .addComponent(subtasksField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(gigsPerSubtask)
                    .addComponent(gigsPerSubtaskField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(maxWallTime)
                    .addComponent(maxWallTimeField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(typicalWallTime)
                    .addComponent(wallTimeRatioField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(optimizer)
                    .addComponent(optimizerComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addGap(ZiggySwingUtils.GROUP_GAP)
                    .addComponent(optionalRemoteParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(architecture)
                    .addComponent(architectureComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(queue)
                    .addComponent(queueComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(maxNodesPerTask)
                    .addComponent(maxNodesField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(subtasksPerCore)
                    .addComponent(subtasksPerCoreField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(pbsParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(pbsParametersToolBar, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addGroup(dataPanelLayout.createParallelGroup()
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addComponent(pbsArchLabel)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsQueueLabel)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsWallTimeLabel)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsNodeCountLabel)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsActiveCoresPerNodeLabel)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsCostLabel))
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addComponent(pbsArch)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsQueue)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsWallTime)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsNodeCount)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsActiveCoresPerNode)
                            .addPreferredGap(ComponentPlacement.RELATED)
                            .addComponent(pbsCost))))));

        return dataPanel;
    }

    private void setButtonState() {
        boolean allFieldsValid = true;
        for (ValidityTestingFormattedTextField field : validityTestingFormattedTextFields) {
            allFieldsValid = allFieldsValid && field.isValidState();
        }
        calculateButton.setEnabled(allFieldsValid);
    }

    private ValidityTestingFormattedTextField createSubtasksField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Integer.class);
        formatter.setMinimum(1);
        formatter.setMaximum(Integer.MAX_VALUE);
        ValidityTestingFormattedTextField subtasksField = new ValidityTestingFormattedTextField(
            formatter);
        subtasksField.setColumns(6);
        subtasksField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        subtasksField.setToolTipText(
            "Set the total number of subtasks for the selected module (must be >= 1).");

        return subtasksField;
    }

    private void nodeSharingCheckBoxEvent(ItemEvent evt) {
        wallTimeScalingCheckBox.setEnabled(!nodeSharingCheckBox.isSelected());
        gigsPerSubtaskField.setEnabled(nodeSharingCheckBox.isSelected());
        updateGigsPerSubtaskToolTip();
    }

    private ValidityTestingFormattedTextField createGigsPerSubtaskField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Double.class);
        formatter.setMinimum(Double.MIN_VALUE);
        formatter.setMaximum(Double.MAX_VALUE);
        ValidityTestingFormattedTextField gigsPerSubtaskField = new ValidityTestingFormattedTextField(
            formatter);
        gigsPerSubtaskField.setColumns(6);
        gigsPerSubtaskField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        return gigsPerSubtaskField;
    }

    private void updateGigsPerSubtaskToolTip() {
        String enabledTip = "Enter the number of GB needed for each subtask (>0).";
        String disabledTip = "Gigs per subtask is disabled when node sharing is disabled.";
        String tip = gigsPerSubtaskField.isEnabled() ? enabledTip : disabledTip;
        gigsPerSubtaskField.setToolTipText(tip);
    }

    private ValidityTestingFormattedTextField createMaxWallTimeField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Double.class);
        formatter.setMinimum(Double.MIN_VALUE);
        formatter.setMaximum(Double.MAX_VALUE);
        ValidityTestingFormattedTextField maxWallTimeField = new ValidityTestingFormattedTextField(
            formatter);
        maxWallTimeField.setColumns(6);
        maxWallTimeField
            .setToolTipText("Enter the MAXIMUM wall time needed by any subtask, in hours (>0).");
        maxWallTimeField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        maxWallTimeField.addFocusListener(new FocusAdapter() {

            // Here is where we modify the typical field when the max field has been
            // updated.
            @Override
            public void focusLost(FocusEvent e) {
                if (maxWallTimeField.isValidState()) {
                    ValidityTestingFormattedTextField typicalField = wallTimeRatioField;
                    double maxValue = (double) maxWallTimeField.getValue();
                    double typicalValue = (double) typicalField.getValue();
                    NumberFormatter formatter = (NumberFormatter) typicalField.getFormatter();
                    formatter.setMaximum(maxValue);
                    if (typicalValue > maxValue) {
                        typicalField.setBorder(ValidityTestingFormattedTextField.INVALID_BORDER);
                    }
                }
            }
        });
        return maxWallTimeField;
    }

    private ValidityTestingFormattedTextField createTypicalWallTimeField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Double.class);
        formatter.setMinimum(Double.MIN_VALUE);
        formatter.setMaximum(currentParameters.getSubtaskMaxWallTimeHours());
        ValidityTestingFormattedTextField wallTimeRatioField = new ValidityTestingFormattedTextField(
            formatter);
        wallTimeRatioField.setColumns(6);
        wallTimeRatioField.setToolTipText(
            "Enter the TYPICAL wall time needed by subtasks, in hours (>0, <= max wall time)");
        wallTimeRatioField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        return wallTimeRatioField;
    }

    private JComboBox<RemoteArchitectureOptimizer> createOptimizerComboBox() {
        JComboBox<RemoteArchitectureOptimizer> optimizerComboBox = new JComboBox<>(
            RemoteArchitectureOptimizer.values());
        optimizerComboBox.setToolTipText(
            htmlBuilder("Select the optimizer used to pick a remote architecture:").appendBreak()
                .append("&ensp;Cores minimizes the number of idle cores.")
                .appendBreak()
                .append("&ensp;Queue depth minimizes the number of hours of queued tasks.")
                .appendBreak()
                .append(
                    "&ensp;Queue time minimizes the estimated total time including time in the queue.")
                .appendBreak()
                .append("&ensp;Cost minimizes the number of SBUs.")
                .toString());
        return optimizerComboBox;
    }

    private JComboBox<RemoteNodeDescriptor> createArchitectureComboBox() {
        JComboBox<RemoteNodeDescriptor> architectureComboBox = new JComboBox<>(
            RemoteNodeDescriptor.allDescriptors());
        architectureComboBox.setToolTipText("Select remote node architecture.");
        return architectureComboBox;
    }

    private JComboBox<RemoteQueueDescriptor> createQueueComboBox() {
        JComboBox<RemoteQueueDescriptor> queueComboBox = new JComboBox<>(
            RemoteQueueDescriptor.allDescriptors());
        queueComboBox.setToolTipText("Select batch queue for use with these jobs.");
        queueComboBox.addActionListener(this::validateQueueComboBox);
        return queueComboBox;
    }

    /**
     * Prompts the user to enter a reserved queue name when setting the queue combo box to
     * "reserved". Note that the input dialog won't allow the user to return to messing around with
     * the rest of the remote execution dialog until a valid value is entered.
     */
    private void validateQueueComboBox(ActionEvent evt) {
        if (queueComboBox.getSelectedItem() == null
            || queueComboBox.getSelectedItem() != RemoteQueueDescriptor.RESERVED) {
            return;
        }
        String userReservedQueueName = "";
        while (!userReservedQueueName.startsWith("R")) {
            userReservedQueueName = JOptionPane.showInputDialog(
                "Enter name of Reserved Queue (must start with R)", reservedQueueName);
        }
        reservedQueueName = userReservedQueueName;
    }

    private ValidityTestingFormattedTextField createMaxNodesField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Integer.class);
        formatter.setMinimum(1);
        formatter.setMaximum(Integer.MAX_VALUE);
        ValidityTestingFormattedTextField maxNodesField = new ValidityTestingFormattedTextField(
            formatter);
        maxNodesField.setColumns(6);
        maxNodesField.setToolTipText(
            htmlBuilder("Enter the maximum number of nodes to request for each task (>=1)")
                .appendBreak()
                .append("or leave empty to let the algorithm determine the number.")
                .toString());
        maxNodesField.setEmptyIsValid(true);
        maxNodesField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        return maxNodesField;
    }

    private ValidityTestingFormattedTextField createSubtasksPerCoreField() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        NumberFormatter formatter = new NumberFormatter(numberFormat);
        formatter.setValueClass(Double.class);
        formatter.setMinimum(1.0);
        formatter.setMaximum(Double.MAX_VALUE);
        ValidityTestingFormattedTextField subtasksPerCoreField = new ValidityTestingFormattedTextField(
            formatter);
        subtasksPerCoreField.setColumns(6);
        subtasksPerCoreField.setToolTipText(
            htmlBuilder("Enter the number of subtasks per active core (>=1)").appendBreak()
                .append("or leave empty to let the algorithm decide the number")
                .toString());
        subtasksPerCoreField.setEmptyIsValid(true);
        subtasksPerCoreField.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        return subtasksPerCoreField;
    }

    private void populateTextFieldsAndComboBoxes() {

        // Subtask counts.
        subtasksField.setValue(subtaskCount);

        // Required parameters.
        if (!StringUtils.isEmpty(currentParameters.getOptimizer())
            && RemoteArchitectureOptimizer.fromName(currentParameters.getOptimizer()) != null) {
            optimizerComboBox.setSelectedItem(
                RemoteArchitectureOptimizer.fromName(currentParameters.getOptimizer()));
        }
        wallTimeRatioField.setValue(currentParameters.getSubtaskTypicalWallTimeHours());
        maxWallTimeField.setValue(currentParameters.getSubtaskMaxWallTimeHours());
        gigsPerSubtaskField.setValue(currentParameters.getGigsPerSubtask());
        nodeSharingCheckBox.setSelected(currentParameters.isNodeSharing());
        wallTimeScalingCheckBox.setSelected(currentParameters.isWallTimeScaling());
        wallTimeScalingCheckBox.setEnabled(!currentParameters.isNodeSharing());
        gigsPerSubtaskField.setEnabled(currentParameters.isNodeSharing());
        updateGigsPerSubtaskToolTip();

        // Optional parameters.
        if (!StringUtils.isEmpty(currentParameters.getRemoteNodeArchitecture())
            && RemoteNodeDescriptor
                .fromName(currentParameters.getRemoteNodeArchitecture()) != null) {
            architectureComboBox.setSelectedItem(
                RemoteNodeDescriptor.fromName(currentParameters.getRemoteNodeArchitecture()));
        } else {
            architectureComboBox.setSelectedItem(RemoteNodeDescriptor.ANY);
        }

        if (!StringUtils.isEmpty(currentParameters.getQueueName())
            && RemoteQueueDescriptor.fromQueueName(currentParameters.getQueueName()) != null) {
            queueComboBox.setSelectedItem(
                RemoteQueueDescriptor.fromQueueName(currentParameters.getQueueName()));
            if (queueComboBox.getSelectedItem() == RemoteQueueDescriptor.RESERVED) {
                reservedQueueName = currentParameters.getQueueName();
            }
        } else {
            queueComboBox.setSelectedItem(RemoteQueueDescriptor.ANY);
        }

        if (!StringUtils.isEmpty(currentParameters.getMaxNodes())) {
            maxNodesField.setValue(Integer.parseInt(currentParameters.getMaxNodes()));
        }
        if (!StringUtils.isEmpty(currentParameters.getSubtasksPerCore())) {
            subtasksPerCoreField
                .setValue(Double.parseDouble(currentParameters.getSubtasksPerCore()));
        }
    }

    private void resetAction(ActionEvent evt) {
        PipelineTaskInformation.reset(node);
        currentParameters = new RemoteParameters(originalParameters);
        subtaskCount = originalSubtaskCount;
        maxParallelSubtaskCount = originalMaxParallelSubtaskCount;
        populateTextFieldsAndComboBoxes();
        displayPbsValues(null);
    }

    private void displayTaskInformation(ActionEvent evt) {
        TaskInformationDialog infoTable = new TaskInformationDialog(this, node);
        infoTable.setVisible(true);
    }

    private void calculatePbsParameters(ActionEvent evt) {

        try {
            populateCurrentParameters();

            String currentOptimizer = currentParameters.getOptimizer();
            if (!currentParameters.isNodeSharing()
                && currentOptimizer.equals(RemoteArchitectureOptimizer.CORES.toString())) {
                JOptionPane.showMessageDialog(this,
                    "Cores optimization disabled when node sharing disabled.\n"
                        + "Cost optimization will be used instead.");
                currentParameters.setOptimizer(RemoteArchitectureOptimizer.COST.toString());
            }

            // If the user has not changed the subtask counts parameter, use the original
            // subtask counts that were generated for each task.
            AlgorithmExecutor executor = AlgorithmExecutor.newRemoteInstance(null);
            PbsParameters pbsParameters = null;
            if (subtaskCount == originalSubtaskCount) {
                Set<PbsParameters> perTaskPbsParameters = new HashSet<>();
                for (SubtaskInformation taskInformation : tasksInformation) {
                    perTaskPbsParameters.add(executor.generatePbsParameters(currentParameters,
                        taskInformation.getSubtaskCount()));
                }
                pbsParameters = PbsParameters.aggregatePbsParameters(perTaskPbsParameters);
            } else {
                pbsParameters = executor.generatePbsParameters(currentParameters, subtaskCount);
            }
            displayPbsValues(pbsParameters);
            currentParameters.setOptimizer(currentOptimizer);
        } catch (Exception f) {
            boolean handled = false;
            if (f instanceof IllegalStateException || f instanceof PipelineException) {
                handled = handlePbsParametersException(f.getStackTrace());
            }
            if (!handled) {
                throw f;
            }
        }
    }

    private void populateCurrentParameters() {

        // Subtask counts.
        subtaskCount = (int) subtasksField.getValue();

        // Required parameters.
        RemoteArchitectureOptimizer optimizerSelection = (RemoteArchitectureOptimizer) optimizerComboBox
            .getSelectedItem();
        currentParameters.setOptimizer(optimizerSelection.toString());
        currentParameters.setSubtaskTypicalWallTimeHours((double) wallTimeRatioField.getValue());
        currentParameters.setSubtaskMaxWallTimeHours((double) maxWallTimeField.getValue());
        currentParameters.setGigsPerSubtask((double) gigsPerSubtaskField.getValue());
        currentParameters.setNodeSharing(nodeSharingCheckBox.isSelected());
        currentParameters.setWallTimeScaling(wallTimeScalingCheckBox.isSelected());

        // Optional parameters.
        if (architectureComboBox.getSelectedItem() == null
            || architectureComboBox.getSelectedItem() == RemoteNodeDescriptor.ANY) {
            currentParameters.setRemoteNodeArchitecture("");
        } else {
            currentParameters.setRemoteNodeArchitecture(
                ((RemoteNodeDescriptor) architectureComboBox.getSelectedItem()).getNodeName());
        }

        if (queueComboBox.getSelectedItem() == null
            || queueComboBox.getSelectedItem() == RemoteQueueDescriptor.ANY) {
            currentParameters.setQueueName("");
        } else {
            RemoteQueueDescriptor queue = (RemoteQueueDescriptor) queueComboBox.getSelectedItem();
            if (queue == RemoteQueueDescriptor.RESERVED) {
                currentParameters.setQueueName(reservedQueueName);
            } else {
                currentParameters.setQueueName(queue.getQueueName());
            }
        }

        Object maxNodesValue = maxNodesField.getValue();
        if (maxNodesValue != null) {
            currentParameters.setMaxNodes(Integer.toString((int) maxNodesValue));
        } else {
            currentParameters.setMaxNodes("");
        }

        Object subtasksPerCoreValue = subtasksPerCoreField.getValue();
        if (subtasksPerCoreValue != null) {
            currentParameters.setSubtasksPerCore(Double.toString((double) subtasksPerCoreValue));
        } else {
            currentParameters.setSubtasksPerCore("");
        }
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
            pbsArch.setText(pbsParameters.getArchitecture().toString());
            pbsQueue.setText(pbsParameters.getQueueName());
            pbsWallTime.setText(pbsParameters.getRequestedWallTime());
            pbsNodeCount.setText(Integer.toString(pbsParameters.getRequestedNodeCount()));
            pbsActiveCoresPerNode.setText(Integer.toString(pbsParameters.getActiveCoresPerNode()));
            pbsCost.setText(String.format("%.2f", pbsParameters.getEstimatedCost()));
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

    /**
     * Updates remote parameters. This means that any parameter changes the user made in this dialog
     * box are returned to the edit pipeline dialog box. When the Save action for that dialog box
     * happens, the parameters will be saved to the database; conversely, if the user chooses Cancel
     * at that point, any changes made here are discarded.
     */
    private void updateRemoteParameters(ActionEvent evt) {

        // If the user has made parameter changes that cause the remote parameters instance to be
        // invalid, don't save them to the edit pipeline dialog box.
        if (calculateButton.isEnabled()) {
            parameterSet.setTypedParameters(currentParameters.getParameters());
        }
        dispose();
    }

    private void cancel(ActionEvent evt) {
        resetAction(null);
        dispose();
    }
}
