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
import java.awt.event.HierarchyEvent;
import java.awt.event.ItemEvent;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.ParseException;
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
import javax.swing.JTextArea;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingUtilities;
import javax.swing.text.DefaultFormatter;
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
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ValidityTestingFormattedTextField;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.util.TimeFormatter;

/**
 * Dialog box that allows the user to set values in an instance of {@link RemoteParameters} and
 * determine the resulting values in a corresponding {@link PbsParameters} instance.
 *
 * @author PT
 * @author Bill Wohler
 */
public class RemoteExecutionDialog extends JDialog {

    private static final Logger log = LoggerFactory.getLogger(RemoteExecutionDialog.class);

    private static final long serialVersionUID = 20240614L;

    private static final int COLUMNS = 6;

    /** Minimum width to ensure {@code pack()} allows for the initially empty fields. */
    private static final int PBS_PARAMETERS_MINIMUM_WIDTH = 120;

    // Reserved queue name: this name is a static variable so that it's "sticky,"
    // i.e., once the user sets a reserved queue name it sticks around until the user
    // changes it to a different value, loads a different value by selecting a pipeline
    // node that's using a different reserved queue, or stops and restarts the console.
    private static String reservedQueueName = "";

    // Data model
    private PipelineDefinitionNodeExecutionResources originalConfiguration;
    private PipelineDefinitionNodeExecutionResources currentConfiguration;
    private PipelineDefinitionNode node;
    private int taskCount;
    private int originalTaskCount;
    private int subtaskCount;
    private int originalSubtaskCount;
    private List<SubtaskInformation> tasksInformation;

    // Dialog box elements
    private ValidityTestingFormattedTextField tasksField;
    private ValidityTestingFormattedTextField subtasksField;
    private ValidityTestingFormattedTextField gigsPerSubtaskField;
    private ValidityTestingFormattedTextField maxWallTimeField;
    private ValidityTestingFormattedTextField typicalWallTimeField;
    private JComboBox<RemoteArchitectureOptimizer> optimizerComboBox;
    private JComboBox<RemoteNodeDescriptor> architectureComboBox;
    private RemoteNodeDescriptor lastArchitectureComboBoxSelection;
    private JLabel architectureLimits;
    private JComboBox<RemoteQueueDescriptor> queueComboBox;
    private RemoteQueueDescriptor lastQueueComboBoxSelection;
    private JLabel queueName;
    private ValidityTestingFormattedTextField queueNameField;
    private JLabel queueLimits;
    private ValidityTestingFormattedTextField maxNodesField;
    private ValidityTestingFormattedTextField subtasksPerCoreField;
    private ValidityTestingFormattedTextField minSubtasksRemoteExecutionField;
    private JCheckBox oneSubtaskCheckBox;
    private JCheckBox wallTimeScalingCheckBox;
    private JCheckBox remoteExecutionEnabledCheckBox;
    private JButton closeButton;

    private JLabel pbsArch;
    private JLabel pbsQueue;
    private JLabel pbsWallTime;
    private JLabel pbsNodeCount;
    private JLabel pbsActiveCoresPerNode;
    private JLabel pbsCost;
    private JTextArea pbsLimits;

    private Set<ValidityTestingFormattedTextField> validityTestingFormattedTextFields = new HashSet<>();
    private Consumer<Boolean> checkFieldsAndRecalculate = this::checkFieldsAndRecalculate;
    private boolean skipCheck;
    private boolean pbsParametersValid = true;

    private final PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();

    public RemoteExecutionDialog(Window owner,
        PipelineDefinitionNodeExecutionResources originalConfiguration, PipelineDefinitionNode node,
        List<SubtaskInformation> tasksInformation) {

        super(owner, DEFAULT_MODALITY_TYPE);

        // Note that the current configuration is a copy of the original. If the user elects
        // to reset the configuration, the current configuration is repopulated from the
        // original; if the user elects to close the dialog box, the original configuration is
        // repopulated from the current one. This ensures that the configuration that is eventually
        // saved from the Edit Pipeline dialog box is the one retrieved from the database, so it
        // can be merged safely.
        this.originalConfiguration = originalConfiguration;
        currentConfiguration = new PipelineDefinitionNodeExecutionResources(originalConfiguration);
        this.tasksInformation = tasksInformation;
        this.node = node;

        taskCount = tasksInformation.size();
        originalTaskCount = taskCount;
        for (SubtaskInformation taskInformation : tasksInformation) {
            subtaskCount += taskInformation.getSubtaskCount();
        }
        originalSubtaskCount = subtaskCount;

        buildComponent();
        setLocationRelativeTo(owner);
        addHierarchyListener(this::hierarchyChanged);
    }

    private void buildComponent() {
        setTitle("Edit remote execution parameters");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        closeButton = createButton(CLOSE, htmlBuilder("Close this dialog box.").appendBreak()
            .append(
                "Your remote parameter changes won't be saved until you press the Save button on the edit pipeline dialog.")
            .toString(), this::close);
        getContentPane().add(
            createButtonPanel(closeButton, createButton(CANCEL,
                "Clear all changes made in this dialog box and close it.", this::cancel)),
            BorderLayout.SOUTH);

        populateTextFieldsAndComboBoxes();
        pack();
    }

    private JPanel createDataPanel() {
        JPanel executionResourcesToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("Reset",
                "Reset RemoteParameters values to the values at the start of this dialog box.",
                this::resetAction),
            createButton("Display task info",
                "Display task and subtask information for this pipeline node.",
                this::displayTaskInformation));
        executionResourcesToolBar
            .setToolTipText("Controls user-settable parameters for remote execution");

        JLabel moduleGroup = boldLabel("Module", LabelType.HEADING1);

        JLabel pipeline = boldLabel("Pipeline");
        ProcessingMode processingMode = pipelineDefinitionOperations()
            .processingMode(originalConfiguration.getPipelineName());
        JLabel pipelineText = new JLabel(MessageFormat.format("{0} (processing {1} data)",
            originalConfiguration.getPipelineName(), processingMode.toString()));

        JLabel module = boldLabel("Module");
        JLabel moduleText = new JLabel(originalConfiguration.getPipelineModuleName());

        JLabel requiredRemoteParametersGroup = boldLabel("Required parameters", LabelType.HEADING1);
        requiredRemoteParametersGroup
            .setToolTipText("Parameters needed for PBS parameter calculation.");

        remoteExecutionEnabledCheckBox = new JCheckBox("Enable remote execution");
        remoteExecutionEnabledCheckBox.addItemListener(this::itemStateChanged);
        remoteExecutionEnabledCheckBox.setToolTipText("Enables or disables remote execution.");

        oneSubtaskCheckBox = new JCheckBox("Run one subtask per node");
        oneSubtaskCheckBox
            .setToolTipText("Disables concurrent processing of multiple subtasks on each node.");
        oneSubtaskCheckBox.addItemListener(this::nodeSharingCheckBoxEvent);

        wallTimeScalingCheckBox = new JCheckBox("Scale wall time by number of cores");
        wallTimeScalingCheckBox.addItemListener(this::itemStateChanged);
        wallTimeScalingCheckBox.setToolTipText(
            htmlBuilder("Scales subtask wall times inversely to the number of cores per node.")
                .appendBreak()
                .append("Only enabled when running one subtask per node.")
                .toString());

        JLabel tasks = boldLabel("Total tasks");
        tasksField = createIntegerField(
            "Set the total number of tasks for the selected module (must be >= 1).");
        validityTestingFormattedTextFields.add(tasksField);

        JLabel subtasks = boldLabel("Total subtasks");
        subtasksField = createIntegerField(
            "Set the total number of subtasks for the selected module (must be >= 1).");
        validityTestingFormattedTextFields.add(subtasksField);

        JLabel gigsPerSubtask = boldLabel("Gigs per subtask");
        gigsPerSubtaskField = createDoubleField("");
        validityTestingFormattedTextFields.add(gigsPerSubtaskField);

        JLabel maxWallTime = boldLabel("Max subtask wall time");
        maxWallTimeField = createMaxWallTimeField();
        validityTestingFormattedTextFields.add(maxWallTimeField);

        JLabel typicalWallTime = boldLabel("Typical subtask wall time");
        typicalWallTimeField = createDoubleField(Double.MIN_VALUE,
            currentConfiguration.getSubtaskMaxWallTimeHours(),
            "Enter the TYPICAL wall time needed by subtasks, in hours (>0, <= max wall time)");
        validityTestingFormattedTextFields.add(typicalWallTimeField);

        JLabel optimizer = boldLabel("Optimizer");
        optimizerComboBox = createOptimizerComboBox();

        JLabel optionalRemoteParametersGroup = boldLabel("Optional parameters", LabelType.HEADING1);
        optionalRemoteParametersGroup.setToolTipText(
            htmlBuilder("Parameters that can be calculated if not set.").appendBreak()
                .append("Values set by users will be included when calculating PBS parameters.")
                .toString());

        JLabel architecture = boldLabel("Architecture");
        architectureComboBox = createArchitectureComboBox();

        architectureLimits = new JLabel();

        JLabel queue = boldLabel("Queue");
        queueComboBox = createQueueComboBox();

        queueName = boldLabel("Reserved queue name");
        queueNameField = createQueueNameField();
        validityTestingFormattedTextFields.add(queueNameField);

        queueLimits = new JLabel();

        JLabel maxNodesPerTask = boldLabel("Max nodes per task");
        maxNodesField = createIntegerField(true,
            htmlBuilder("Enter the maximum number of nodes to request for each task (>=1)")
                .appendBreak()
                .append("or leave empty to let the algorithm determine the number.")
                .toString());
        validityTestingFormattedTextFields.add(maxNodesField);

        JLabel subtasksPerCore = boldLabel("Subtasks per core");
        subtasksPerCoreField = createDoubleField(1.0, Double.MAX_VALUE, true,
            htmlBuilder("Enter the number of subtasks per active core (>=1)").appendBreak()
                .append("or leave empty to let the algorithm decide the number")
                .toString());
        validityTestingFormattedTextFields.add(subtasksPerCoreField);

        JLabel minSubtasksRemoteExecution = boldLabel("Minimum subtasks for remote execution");
        minSubtasksRemoteExecutionField = createIntegerField(0, Integer.MAX_VALUE, true,
            htmlBuilder(
                "Enter the minimum number of subtasks that are required to use remote execution;")
                    .appendBreak()
                    .append(
                        "otherwise, the subtasks will be processed locally, even if Enable remote execution is checked.")
                    .toString());
        validityTestingFormattedTextFields.add(minSubtasksRemoteExecutionField);

        JLabel pbsParametersGroup = boldLabel("PBS parameters", LabelType.HEADING1);
        pbsParametersGroup.setToolTipText("Displays parameters that will be sent to PBS.");

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

        pbsLimits = new JTextArea();
        pbsLimits.setEditable(false);
        pbsLimits.setLineWrap(true);
        pbsLimits.setWrapStyleWord(true);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(executionResourcesToolBar)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(moduleGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(pipeline)
                            .addComponent(pipelineText)
                            .addComponent(module)
                            .addComponent(moduleText)))
                    .addComponent(requiredRemoteParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(oneSubtaskCheckBox)
                            .addComponent(wallTimeScalingCheckBox)
                            .addComponent(remoteExecutionEnabledCheckBox)
                            .addComponent(tasks)
                            .addComponent(tasksField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
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
                            .addComponent(typicalWallTimeField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(optimizer)
                            .addComponent(optimizerComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))))
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(pbsParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addGroup(dataPanelLayout.createSequentialGroup()
                                .addGroup(dataPanelLayout.createParallelGroup()
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
                                    .addComponent(pbsActiveCoresPerNode,
                                        PBS_PARAMETERS_MINIMUM_WIDTH, GroupLayout.DEFAULT_SIZE,
                                        GroupLayout.DEFAULT_SIZE)
                                    .addComponent(pbsCost, PBS_PARAMETERS_MINIMUM_WIDTH,
                                        GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE)))
                            .addComponent(pbsLimits)))))
            .addComponent(optionalRemoteParametersGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggyGuiConstants.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(architecture)
                            .addComponent(architectureComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(ComponentPlacement.RELATED)
                        .addComponent(architectureLimits))
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(queue)
                            .addComponent(queueComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(ComponentPlacement.RELATED)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(queueName)
                            .addComponent(queueNameField, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(ComponentPlacement.RELATED)
                        .addComponent(queueLimits))
                    .addComponent(maxNodesPerTask)
                    .addComponent(maxNodesField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addComponent(subtasksPerCore)
                    .addComponent(subtasksPerCoreField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addComponent(minSubtasksRemoteExecution)
                    .addComponent(minSubtasksRemoteExecutionField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(executionResourcesToolBar, GroupLayout.PREFERRED_SIZE,
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
                    .addGap(ZiggyGuiConstants.GROUP_GAP)
                    .addComponent(requiredRemoteParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(remoteExecutionEnabledCheckBox)
                    .addComponent(oneSubtaskCheckBox)
                    .addComponent(wallTimeScalingCheckBox)
                    .addPreferredGap(ComponentPlacement.UNRELATED)
                    .addComponent(tasks)
                    .addComponent(tasksField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                        GroupLayout.PREFERRED_SIZE)
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
                    .addComponent(typicalWallTimeField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(optimizer)
                    .addComponent(optimizerComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(pbsParametersGroup)
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
                            .addComponent(pbsCost)))
                    .addPreferredGap(ComponentPlacement.UNRELATED)
                    .addComponent(pbsLimits)))
            .addGap(ZiggyGuiConstants.GROUP_GAP)
            .addComponent(optionalRemoteParametersGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(architecture)
                    .addComponent(architectureComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addPreferredGap(ComponentPlacement.RELATED, GroupLayout.PREFERRED_SIZE,
                        Short.MAX_VALUE)
                    .addComponent(architectureLimits)))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(queue)
                    .addComponent(queueComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(queueName)
                    .addComponent(queueNameField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addPreferredGap(ComponentPlacement.RELATED, GroupLayout.PREFERRED_SIZE,
                        Short.MAX_VALUE)
                    .addComponent(queueLimits)))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(maxNodesPerTask)
            .addComponent(maxNodesField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(subtasksPerCore)
            .addComponent(subtasksPerCoreField, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(minSubtasksRemoteExecution)
            .addComponent(minSubtasksRemoteExecutionField, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE));

        return dataPanel;
    }

    private void itemStateChanged(ItemEvent evt) {
        checkFieldsAndRecalculate(null);
    }

    // Validation is postponed until the dialog is visible.
    private void hierarchyChanged(HierarchyEvent evt) {
        if ((evt.getChangeFlags() & HierarchyEvent.SHOWING_CHANGED) != 0 && isVisible()) {
            checkFieldsAndRecalculate(null);
        }
    }

    private void checkFieldsAndRecalculate(Boolean valid) {
        log.debug("valid={}, visible={}, skipCheck={}", valid, isVisible(), skipCheck);
        if (!isVisible() || skipCheck) {
            return;
        }

        if (allFieldsRequiredForCalculationValid()) {
            pbsParametersValid = calculatePbsParameters();
        } else {
            pbsParametersValid = false;
            displayPbsValues(null);
        }
        closeButton.setEnabled(!remoteExecutionEnabledCheckBox.isSelected()
            || allFieldsRequiredForCloseValid() && pbsParametersValid);

        log.debug(
            "remoteExecutionEnabled={}, allFieldsRequiredForCalculationValid()={}, pbsParametersValid={}, allFieldsRequiredForCloseValid()={}",
            remoteExecutionEnabledCheckBox.isSelected(), allFieldsRequiredForCloseValid(),
            pbsParametersValid);
    }

    /**
     * Determines whether all the validity-checking fields are valid. Used to determine whether to
     * calculate the PBS parameters.
     */
    private boolean allFieldsRequiredForCalculationValid() {
        return allFieldsRequiredForCloseValid() && tasksField.isValidState()
            && subtasksField.isValidState();
    }

    /**
     * Determines whether all the validity-checking fields are valid except for the subtask count.
     * This is used to determine whether to enable the Close button, which is used to update the
     * remote execution configuration when the dialog box closes.
     */
    private boolean allFieldsRequiredForCloseValid() {
        boolean allFieldsValid = true;
        for (ValidityTestingFormattedTextField field : validityTestingFormattedTextFields) {
            if (!field.equals(tasksField) && !field.equals(subtasksField)) {
                allFieldsValid = allFieldsValid && field.isValidState();
            }
        }
        return allFieldsValid;
    }

    private ValidityTestingFormattedTextField createIntegerField(String toolTipText) {
        return createIntegerField(false, toolTipText);
    }

    private ValidityTestingFormattedTextField createIntegerField(boolean emptyIsValid,
        String toolTipText) {
        return createIntegerField(1, Integer.MAX_VALUE, emptyIsValid, toolTipText);
    }

    private ValidityTestingFormattedTextField createIntegerField(int min, int max,
        boolean emptyIsValid, String toolTipText) {
        NumberFormatter formatter = new NumberFormatter(NumberFormat.getInstance());
        formatter.setValueClass(Integer.class);
        formatter.setMinimum(min);
        formatter.setMaximum(max);
        ValidityTestingFormattedTextField integerField = new ValidityTestingFormattedTextField(
            formatter);
        integerField.setColumns(COLUMNS);
        integerField.setEmptyIsValid(emptyIsValid);
        integerField.setToolTipText(toolTipText);
        integerField.setExecuteOnValidityCheck(checkFieldsAndRecalculate);

        return integerField;
    }

    private ValidityTestingFormattedTextField createDoubleField(String toolTipText) {
        return createDoubleField(Double.MIN_VALUE, Double.MAX_VALUE, toolTipText);
    }

    private ValidityTestingFormattedTextField createDoubleField(double min, double max,
        String toolTipText) {
        return createDoubleField(min, max, false, toolTipText);
    }

    private ValidityTestingFormattedTextField createDoubleField(double min, double max,
        boolean emptyIsValid, String toolTipText) {
        NumberFormatter formatter = new NumberFormatter(NumberFormat.getInstance());
        formatter.setValueClass(Double.class);
        formatter.setMinimum(min);
        formatter.setMaximum(max);
        ValidityTestingFormattedTextField doubleField = new ValidityTestingFormattedTextField(
            formatter);
        doubleField.setColumns(COLUMNS);
        doubleField.setEmptyIsValid(emptyIsValid);
        doubleField.setToolTipText(toolTipText);
        doubleField.setExecuteOnValidityCheck(checkFieldsAndRecalculate);

        return doubleField;
    }

    private void nodeSharingCheckBoxEvent(ItemEvent evt) {
        wallTimeScalingCheckBox.setEnabled(oneSubtaskCheckBox.isSelected());
        gigsPerSubtaskField.setEnabled(!oneSubtaskCheckBox.isSelected());
        gigsPerSubtaskField.setToolTipText(gigsPerSubtaskToolTip());
        checkFieldsAndRecalculate(null);
    }

    private String gigsPerSubtaskToolTip() {
        String enabledTip = "Enter the number of GB needed for each subtask (>0).";
        String disabledTip = "Gigs per subtask is disabled when running one subtask per node.";
        return gigsPerSubtaskField.isEnabled() ? enabledTip : disabledTip;
    }

    private ValidityTestingFormattedTextField createMaxWallTimeField() {
        maxWallTimeField = createDoubleField(Double.MIN_VALUE, Double.MAX_VALUE,
            "Enter the MAXIMUM wall time needed by any subtask, in hours (>0).");

        // Update the typical field when the max field has been updated.
        maxWallTimeField.addPropertyChangeListener(evt -> {
            if (!maxWallTimeField.isValidState()) {
                return;
            }
            double maxValue = (double) maxWallTimeField.getValue();
            double typicalValue = (double) typicalWallTimeField.getValue();
            ((NumberFormatter) typicalWallTimeField.getFormatter()).setMaximum(maxValue);
            typicalWallTimeField.updateBorder(typicalValue <= maxValue);
        });

        return maxWallTimeField;
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
        optimizerComboBox.addItemListener(this::itemStateChanged);
        return optimizerComboBox;
    }

    private JComboBox<RemoteNodeDescriptor> createArchitectureComboBox() {
        JComboBox<RemoteNodeDescriptor> architectureComboBox = new JComboBox<>(
            RemoteNodeDescriptor.allDescriptors());
        architectureComboBox.setToolTipText("Select remote node architecture.");
        architectureComboBox.addItemListener(this::validateArchitectureComboBox);
        return architectureComboBox;
    }

    private void validateArchitectureComboBox(ItemEvent evt) {
        if (evt.getStateChange() == ItemEvent.DESELECTED || !isVisible() || skipCheck) {
            return;
        }

        pbsParametersValid = calculatePbsParameters();

        // Reset combo box if the chosen value is invalid. Note that this method is also called when
        // the dialog is first displayed, so if the configuration is invalid, simply set the last
        // selection to the current one and let the warning dialog shown by
        // handlePbsParametersException() guide the user.
        if (pbsParametersValid || lastArchitectureComboBoxSelection == null) {
            lastArchitectureComboBoxSelection = (RemoteNodeDescriptor) architectureComboBox
                .getSelectedItem();
        } else {
            architectureComboBox.setSelectedItem(lastArchitectureComboBoxSelection);
        }

        if (lastArchitectureComboBoxSelection != RemoteNodeDescriptor.ANY) {
            architectureLimits
                .setText(MessageFormat.format("{0} cores, {1} GB/core, {2} fractional SBUs",
                    lastArchitectureComboBoxSelection.getMaxCores(),
                    lastArchitectureComboBoxSelection.getGigsPerCore(),
                    lastArchitectureComboBoxSelection.getCostFactor()));
        } else {
            architectureLimits.setText("");
        }
    }

    private JComboBox<RemoteQueueDescriptor> createQueueComboBox() {
        JComboBox<RemoteQueueDescriptor> queueComboBox = new JComboBox<>(
            RemoteQueueDescriptor.allDescriptors());
        queueComboBox.setToolTipText("Select batch queue for use with these jobs.");
        queueComboBox.addItemListener(this::validateQueueComboBox);
        return queueComboBox;
    }

    /**
     * Enables the queue name fields if RESERVED is selected, shows the queue limits, and populates
     * the max nodes fields if DEBUG or DEVEL are selected.
     */
    private void validateQueueComboBox(ItemEvent evt) {
        if (evt.getStateChange() == ItemEvent.DESELECTED || !isVisible() || skipCheck) {
            return;
        }

        boolean reservedQueue = queueComboBox.getSelectedItem() != null
            && queueComboBox.getSelectedItem() == RemoteQueueDescriptor.RESERVED;
        if (queueName.isEnabled() != reservedQueue) {
            queueName.setEnabled(reservedQueue);
            queueNameField.setEnabled(reservedQueue);
        }

        pbsParametersValid = calculatePbsParameters();

        // Reset combo box if the chosen value is invalid, but allow the user to enter a reserved
        // queue name.
        if (pbsParametersValid || reservedQueue) {
            lastQueueComboBoxSelection = (RemoteQueueDescriptor) queueComboBox.getSelectedItem();
        } else {
            queueComboBox.setSelectedItem(lastQueueComboBoxSelection);
        }

        // Show queue limits.
        if (lastQueueComboBoxSelection.getMaxWallTimeHours() > 0
            && lastQueueComboBoxSelection.getMaxWallTimeHours() < Double.MAX_VALUE) {
            queueLimits.setText(MessageFormat.format("{0} hrs max wall time",
                lastQueueComboBoxSelection.getMaxWallTimeHours()));
        } else {
            queueLimits.setText("");
        }

        // Populate maxNodes field if applicable.
        int maxNodes = ((RemoteQueueDescriptor) queueComboBox.getSelectedItem()).getMaxNodes();
        if (maxNodes > 0 && maxNodes < Integer.MAX_VALUE) {
            maxNodesField.setValue(maxNodes);
        } else if (originalConfiguration.getMaxNodes() > 0) {
            maxNodesField.setValue(originalConfiguration.getMaxNodes());
        } else {
            maxNodesField.setValue(null);
        }
    }

    private ValidityTestingFormattedTextField createQueueNameField() {
        @SuppressWarnings("serial")
        ValidityTestingFormattedTextField queueNameField = new ValidityTestingFormattedTextField(
            new DefaultFormatter() {
                @Override
                public Object stringToValue(String s) throws ParseException {
                    if (!s.matches("^\\s*R\\d+\\s*$")) {
                        throw new ParseException(
                            "Queue is named with the letter R followed by a number", 1);
                    }
                    return super.stringToValue(s);
                }
            });
        queueNameField.setColumns(10);
        queueNameField.setName("Queue name");
        queueNameField.setToolTipText(
            "The reservation queue is named with the letter R followed by a number.");
        queueNameField.setExecuteOnValidityCheck(checkFieldsAndRecalculate);

        return queueNameField;
    }

    private void populateTextFieldsAndComboBoxes() {

        // Subtask counts.
        tasksField.setValue(taskCount);
        subtasksField.setValue(subtaskCount);

        // Required parameters.
        optimizerComboBox.setSelectedItem(currentConfiguration.getOptimizer());

        remoteExecutionEnabledCheckBox.setSelected(currentConfiguration.isRemoteExecutionEnabled());
        typicalWallTimeField.setValue(currentConfiguration.getSubtaskTypicalWallTimeHours());
        maxWallTimeField.setValue(currentConfiguration.getSubtaskMaxWallTimeHours());
        gigsPerSubtaskField.setValue(currentConfiguration.getGigsPerSubtask());
        oneSubtaskCheckBox.setSelected(!currentConfiguration.isNodeSharing());
        wallTimeScalingCheckBox.setSelected(currentConfiguration.isWallTimeScaling());
        wallTimeScalingCheckBox.setEnabled(!currentConfiguration.isNodeSharing());
        gigsPerSubtaskField.setEnabled(currentConfiguration.isNodeSharing());
        gigsPerSubtaskField.setToolTipText(gigsPerSubtaskToolTip());

        // Optional parameters.
        if (!StringUtils.isBlank(currentConfiguration.getRemoteNodeArchitecture())) {
            architectureComboBox.setSelectedItem(
                RemoteNodeDescriptor.fromName(currentConfiguration.getRemoteNodeArchitecture()));
        } else {
            architectureComboBox.setSelectedItem(RemoteNodeDescriptor.ANY);
        }
        lastArchitectureComboBoxSelection = (RemoteNodeDescriptor) architectureComboBox
            .getSelectedItem();

        if (!StringUtils.isBlank(currentConfiguration.getQueueName())) {
            queueComboBox.setSelectedItem(
                RemoteQueueDescriptor.fromQueueName(currentConfiguration.getQueueName()));
            if (queueComboBox.getSelectedItem() == RemoteQueueDescriptor.RESERVED) {
                reservedQueueName = currentConfiguration.getQueueName();
            }
            queueName.setEnabled(queueComboBox.getSelectedItem() == RemoteQueueDescriptor.RESERVED);
            queueNameField
                .setEnabled(queueComboBox.getSelectedItem() == RemoteQueueDescriptor.RESERVED);
        } else {
            queueComboBox.setSelectedItem(RemoteQueueDescriptor.ANY);
            queueName.setEnabled(false);
            queueNameField.setEnabled(false);
        }
        lastQueueComboBoxSelection = (RemoteQueueDescriptor) queueComboBox.getSelectedItem();
        queueNameField.setText(reservedQueueName);

        Integer maxNodes = currentConfiguration.getMaxNodes() > 0
            ? currentConfiguration.getMaxNodes()
            : null;
        maxNodesField.setValue(maxNodes);

        Double subtasksPerCore = currentConfiguration.getSubtasksPerCore() > 0
            ? currentConfiguration.getSubtasksPerCore()
            : null;
        subtasksPerCoreField.setValue(subtasksPerCore);

        Integer minSubtasksRemoteExecution = currentConfiguration
            .getMinSubtasksForRemoteExecution() >= 0
                ? currentConfiguration.getMinSubtasksForRemoteExecution()
                : null;
        minSubtasksRemoteExecutionField.setValue(minSubtasksRemoteExecution);
    }

    private void resetAction(ActionEvent evt) {
        reset(true);
    }

    private void reset(boolean checkFields) {
        skipCheck = true;
        PipelineTaskInformation.reset(node);
        currentConfiguration = new PipelineDefinitionNodeExecutionResources(originalConfiguration);
        taskCount = originalTaskCount;
        subtaskCount = originalSubtaskCount;
        populateTextFieldsAndComboBoxes();
        skipCheck = false;
        if (checkFields) {
            checkFieldsAndRecalculate(null);
        }
    }

    private void displayTaskInformation(ActionEvent evt) {
        TaskInformationDialog infoTable = new TaskInformationDialog(this, node);
        infoTable.setVisible(true);
    }

    /**
     * Calculates the PBS parameters from the user-defined parameters.
     *
     * @return Returns true if the parameters that contribute to the PBS parameters are valid;
     * otherwise, false
     */
    private boolean calculatePbsParameters() {

        try {
            populateCurrentParameters();

            RemoteArchitectureOptimizer currentOptimizer = currentConfiguration.getOptimizer();
            if (!currentConfiguration.isNodeSharing()
                && currentOptimizer == RemoteArchitectureOptimizer.CORES) {
                JOptionPane.showMessageDialog(this,
                    "Cores optimization disabled when running one subtask per node.\n"
                        + "Cost optimization will be used instead.");
                currentConfiguration.setOptimizer(RemoteArchitectureOptimizer.COST);
            }

            // If the user has changed the task count, use it in lieu of the calculated task count.
            // Otherwise, if the user has not changed the subtask counts parameter, use the original
            // subtask counts that were generated for each task. Otherwise, use the total subtasks
            // given.
            AlgorithmExecutor executor = AlgorithmExecutor.newRemoteInstance(null);
            PbsParameters pbsParameters = null;
            if (taskCount != originalTaskCount || subtaskCount != originalSubtaskCount) {
                Set<PbsParameters> perTaskPbsParameters = new HashSet<>();
                for (int i = 0; i < taskCount; i++) {
                    perTaskPbsParameters.add(executor.generatePbsParameters(currentConfiguration,
                        subtaskCount / taskCount));
                }
                pbsParameters = PbsParameters.aggregatePbsParameters(perTaskPbsParameters);
            } else {
                Set<PbsParameters> perTaskPbsParameters = new HashSet<>();
                for (SubtaskInformation taskInformation : tasksInformation) {
                    perTaskPbsParameters.add(executor.generatePbsParameters(currentConfiguration,
                        taskInformation.getSubtaskCount()));
                }
                pbsParameters = PbsParameters.aggregatePbsParameters(perTaskPbsParameters);
            }
            displayPbsValues(pbsParameters);
            currentConfiguration.setOptimizer(currentOptimizer);
        } catch (Exception e) {
            if ((e instanceof IllegalArgumentException || e instanceof IllegalStateException
                || e instanceof PipelineException) && handlePbsParametersException(e)) {
                return false;
            }
            throw e;
        }
        return true;
    }

    private void populateCurrentParameters() {

        // Task counts.
        taskCount = textToInt(tasksField);
        subtaskCount = textToInt(subtasksField);

        // Required parameters.
        currentConfiguration.setRemoteExecutionEnabled(remoteExecutionEnabledCheckBox.isSelected());
        RemoteArchitectureOptimizer optimizerSelection = (RemoteArchitectureOptimizer) optimizerComboBox
            .getSelectedItem();
        currentConfiguration.setOptimizer(optimizerSelection);
        currentConfiguration.setSubtaskTypicalWallTimeHours(textToDouble(typicalWallTimeField));
        currentConfiguration.setSubtaskMaxWallTimeHours(textToDouble(maxWallTimeField));
        currentConfiguration.setGigsPerSubtask(textToDouble(gigsPerSubtaskField));
        currentConfiguration.setNodeSharing(!oneSubtaskCheckBox.isSelected());
        currentConfiguration.setWallTimeScaling(wallTimeScalingCheckBox.isSelected());

        // Optional parameters.
        if (architectureComboBox.getSelectedItem() == null
            || architectureComboBox.getSelectedItem() == RemoteNodeDescriptor.ANY) {
            currentConfiguration.setRemoteNodeArchitecture("");
        } else {
            currentConfiguration.setRemoteNodeArchitecture(
                ((RemoteNodeDescriptor) architectureComboBox.getSelectedItem()).getNodeName());
        }

        if (queueComboBox.getSelectedItem() == null
            || queueComboBox.getSelectedItem() == RemoteQueueDescriptor.ANY) {
            currentConfiguration.setQueueName("");
        } else {
            RemoteQueueDescriptor queue = (RemoteQueueDescriptor) queueComboBox.getSelectedItem();
            if (queue == RemoteQueueDescriptor.RESERVED) {
                reservedQueueName = queueNameField.getText().trim();
                currentConfiguration.setQueueName(reservedQueueName);
            } else {
                currentConfiguration.setQueueName(queue.getQueueName());
            }
        }

        currentConfiguration.setMaxNodes(textToInt(maxNodesField));
        currentConfiguration.setSubtasksPerCore(textToDouble(subtasksPerCoreField));
        currentConfiguration.setMinSubtasksForRemoteExecution(
            minSubtasksRemoteExecutionField.getText().isBlank() ? -1
                : textToInt(minSubtasksRemoteExecutionField));

        log.debug("Updated currentConfiguration");
    }

    private int textToInt(ValidityTestingFormattedTextField field) {
        try {
            return NumberFormat.getInstance().parse(field.getText()).intValue();
        } catch (ParseException e) {
            return 0;
        }
    }

    private double textToDouble(ValidityTestingFormattedTextField field) {
        try {
            return NumberFormat.getInstance().parse(field.getText()).doubleValue();
        } catch (ParseException e) {
            return 0.0;
        }
    }

    private void displayPbsValues(PbsParameters pbsParameters) {
        if (pbsParameters == null) {
            pbsArch.setText("");
            pbsQueue.setText("");
            pbsWallTime.setText("");
            pbsNodeCount.setText("");
            pbsActiveCoresPerNode.setText("");
            pbsCost.setText("");
            pbsLimits.setText("");
        } else {
            pbsArch.setText(pbsParameters.getArchitecture().toString());
            pbsQueue.setText(pbsParameters.getQueueName());
            pbsWallTime.setText(TimeFormatter.stripSeconds(pbsParameters.getRequestedWallTime()));
            pbsNodeCount.setText(Integer.toString(pbsParameters.getRequestedNodeCount()));
            pbsActiveCoresPerNode.setText(Integer.toString(pbsParameters.getActiveCoresPerNode()));
            pbsCost.setText(String.format("%.2f", pbsParameters.getEstimatedCost()));

            double maxWallTimeHours = RemoteQueueDescriptor
                .fromQueueName(pbsParameters.getQueueName())
                .getMaxWallTimeHours();
            pbsLimits.setText(MessageFormat.format(
                "The {0} architecture has {1} cores, {2} GB/core, and a cost factor of {3} SBUs, "
                    + "and for the {4} queue, the limit is {5} hrs max wall time.",
                pbsParameters.getArchitecture(), pbsParameters.getArchitecture().getMaxCores(),
                pbsParameters.getArchitecture().getGigsPerCore(),
                pbsParameters.getArchitecture().getCostFactor(), pbsParameters.getQueueName(),
                maxWallTimeHours == Double.MAX_VALUE ? "infinite" : maxWallTimeHours));
        }
    }

    private boolean handlePbsParametersException(Exception e) {
        for (StackTraceElement element : e.getStackTrace()) {
            if (element.getClassName().equals(PbsParameters.class.getName())
                && (element.getMethodName().equals("populateArchitecture")
                    || element.getMethodName().equals("selectArchitecture")
                    || element.getMethodName().equals("computeWallTimeAndQueue"))) {
                SwingUtilities
                    .invokeLater(() -> JOptionPane.showMessageDialog(this, e.getMessage()));
                return true;
            }
        }
        return false;
    }

    /**
     * Close the dialog. Any parameter changes the user made in this dialog box are returned to the
     * edit pipeline dialog box via {@link #getCurrentConfiguration()}, which is updated as the user
     * makes changes. When the Save action for that dialog box happens, the parameters will be saved
     * to the database; conversely, if the user chooses Cancel at that point, any changes made here
     * are discarded.
     * <p>
     * The user can generally make any changes they want. The exception is that if they want to set
     * remote execution to enabled, then the other parameters have to be valid. For example, you
     * can't turn on remote execution if things like the typical and max time per subtask are 0. If
     * this is the case, the Close button should be disabled so that this method can't be called.
     */
    private void close(ActionEvent evt) {
        // Because currentConfiguration isn't updated if a field is invalid, explicitly update
        // configuration so that the parameters are preserved upon re-entry.
        populateCurrentParameters();
        dispose();
    }

    private void cancel(ActionEvent evt) {
        reset(false);
        dispose();
    }

    public PipelineDefinitionNodeExecutionResources getCurrentConfiguration() {
        return currentConfiguration;
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }
}
