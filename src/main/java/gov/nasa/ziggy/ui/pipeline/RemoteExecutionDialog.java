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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.swing.DefaultComboBoxModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Group;
import javax.swing.GroupLayout.SequentialGroup;
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

import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.BatchParameters;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironmentOperations;
import gov.nasa.ziggy.pipeline.step.remote.batch.SupportedBatchSystem;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskInformation;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ValidityTestingFormattedTextField;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Dialog box that allows the user to set values for remote execution and generates an instance of
 * {@link BatchParameters} that can be used by the batch system.
 *
 * @author PT
 * @author Bill Wohler
 */
public class RemoteExecutionDialog extends JDialog {

    private static final Logger log = LoggerFactory.getLogger(RemoteExecutionDialog.class);

    private static final long serialVersionUID = 20240614L;

    private static final int COLUMNS = 6;

    /** Minimum width to ensure {@code pack()} allows for the initially empty labels. */
    private static final int BATCH_LABELS_MINIMUM_WIDTH = 170;

    /** Minimum width to ensure {@code pack()} allows for the initially empty fields. */
    private static final int BATCH_PARAMETERS_MINIMUM_WIDTH = 180;

    // Reserved queue name: this name is a static variable so that it's "sticky,"
    // i.e., once the user sets a reserved queue name it sticks around until the user
    // changes it to a different value, loads a different value by selecting a pipeline
    // node that's using a different reserved queue, or stops and restarts the console.
    private static String reservedQueueName = "";

    // Data model
    private PipelineNodeExecutionResources originalConfiguration;
    private PipelineNodeExecutionResources currentConfiguration;
    private PipelineNode node;
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
    private JComboBox<String> architectureComboBox;
    private JComboBox<String> remoteEnvironmentComboBox;
    private JComboBox<String> queueComboBox;
    private String lastRemoteEnvironmentComboBoxSelection;
    private String lastArchitectureComboBoxSelection;
    private JLabel architectureLimits;
    private String lastQueueComboBoxSelection;
    private JLabel queueName;
    private ValidityTestingFormattedTextField queueNameField;
    private JLabel queueLimits;
    private ValidityTestingFormattedTextField maxNodesField;
    private ValidityTestingFormattedTextField subtasksPerCoreField;
    private ValidityTestingFormattedTextField minSubtasksRemoteExecutionField;
    private JCheckBox oneSubtaskCheckBox;
    private JCheckBox wallTimeScalingCheckBox;
    private JButton closeButton;

    private JLabel batchParametersGroup;
    private JTextArea batchLimits;

    private final List<JLabel> batchParameterLabels;
    private final List<JLabel> batchParameterValues;

    private Map<String, RemoteEnvironment> remoteEnvironmentByName;
    private Map<String, Architecture> architectureByDescription;
    private Map<String, BatchQueue> batchQueueByDescription;

    // Cached remote environment settings for each supported environment.
    private Map<String, RemoteEnvironmentCache> remoteCacheByEnvironmentName = new HashMap<>();

    private Set<ValidityTestingFormattedTextField> validityTestingFormattedTextFields = new HashSet<>();
    private Consumer<Boolean> checkFieldsAndRecalculate = this::checkFieldsAndRecalculate;
    private boolean skipCheck;
    private boolean batchParametersValid = true;

    private final PipelineOperations pipelineOperations = new PipelineOperations();
    private final RemoteEnvironmentOperations remoteEnvironmentOperations = new RemoteEnvironmentOperations();

    private static final RemoteEnvironment DISABLED_REMOTE_ENVIRONMENT = new RemoteEnvironment() {
        @Override
        public String getName() {
            return "Disabled";
        }
    };

    public RemoteExecutionDialog(Window owner, PipelineNodeExecutionResources originalConfiguration,
        PipelineNode node, List<SubtaskInformation> tasksInformation) {

        super(owner, DEFAULT_MODALITY_TYPE);

        // Note that the current configuration is a copy of the original. If the user elects
        // to reset the configuration, the current configuration is repopulated from the
        // original; if the user elects to close the dialog box, the original configuration is
        // repopulated from the current one. This ensures that the configuration that is eventually
        // saved from the Edit Pipeline dialog box is the one retrieved from the database, so it
        // can be merged safely.
        this.originalConfiguration = originalConfiguration;
        currentConfiguration = new PipelineNodeExecutionResources(originalConfiguration);
        this.tasksInformation = tasksInformation;
        this.node = node;

        taskCount = tasksInformation.size();
        originalTaskCount = taskCount;
        for (SubtaskInformation taskInformation : tasksInformation) {
            subtaskCount += taskInformation.getSubtaskCount();
        }
        originalSubtaskCount = subtaskCount;

        // Determine the maximum number of parameters we will ever need to display.
        int maxBatchParameters = 0;
        for (SupportedBatchSystem batchSystem : SupportedBatchSystem.validValues()) {
            maxBatchParameters = Math.max(maxBatchParameters,
                batchSystem.batchParameters().batchParametersByName(null).size());
        }

        // Create the batch parameter JLabels arrays.
        batchParameterLabels = new ArrayList<>();
        batchParameterValues = new ArrayList<>();
        for (int i = 0; i < maxBatchParameters; i++) {
            batchParameterLabels.add(boldLabel(""));
            batchParameterValues.add(new JLabel(""));
        }

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

        JLabel nodeGroup = boldLabel("Node", LabelType.HEADING1);

        JLabel pipeline = boldLabel("Pipeline");
        ProcessingMode processingMode = pipelineOperations()
            .processingMode(originalConfiguration.getPipelineName());
        remoteEnvironmentByName = retrieveRemoteEnvironmentByName();
        JLabel pipelineText = new JLabel(MessageFormat.format("{0} (processing {1} data)",
            originalConfiguration.getPipelineName(), processingMode.toString()));

        JLabel nodeLabel = boldLabel("Node"); // non-standard name to avoid conflict with field name
        JLabel nodeText = new JLabel(originalConfiguration.getPipelineStepName());

        JLabel requiredRemoteParametersGroup = boldLabel("Required parameters", LabelType.HEADING1);
        requiredRemoteParametersGroup
            .setToolTipText("Parameters needed for batch parameter calculation.");

        JLabel environment = boldLabel("Remote environment");
        remoteEnvironmentComboBox = createRemoteEnvironmentComboBox();

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
            "Set the total number of tasks for the selected node (must be >= 1).");
        validityTestingFormattedTextFields.add(tasksField);

        JLabel subtasks = boldLabel("Total subtasks");
        subtasksField = createIntegerField(
            "Set the total number of subtasks for the selected node (must be >= 1).");
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
                .append("Values set by users will be included when calculating batch parameters.")
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

        batchParametersGroup = boldLabel("", LabelType.HEADING1);
        batchParametersGroup
            .setToolTipText("Displays parameters that will be sent to the batch system.");

        batchLimits = new JTextArea();
        batchLimits.setEditable(false);
        batchLimits.setLineWrap(true);
        batchLimits.setWrapStyleWord(true);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(executionResourcesToolBar)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(nodeGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(pipeline)
                            .addComponent(pipelineText)
                            .addComponent(nodeLabel)
                            .addComponent(nodeText)))
                    .addComponent(requiredRemoteParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addComponent(remoteEnvironmentComboBox, GroupLayout.PREFERRED_SIZE,
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(oneSubtaskCheckBox)
                            .addComponent(wallTimeScalingCheckBox)
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
                                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addComponent(environment))))
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(batchParametersGroup)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGap(ZiggyGuiConstants.INDENT)
                        .addGroup(dataPanelLayout.createParallelGroup()
                            .addGroup(dataPanelLayout.createSequentialGroup()
                                .addGroup(labelGroup(dataPanelLayout.createParallelGroup(),
                                    batchParameterLabels, BATCH_LABELS_MINIMUM_WIDTH))
                                .addPreferredGap(ComponentPlacement.RELATED)
                                .addGroup(labelGroup(dataPanelLayout.createParallelGroup(),
                                    batchParameterValues, BATCH_PARAMETERS_MINIMUM_WIDTH)))
                            .addComponent(batchLimits)))))
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
                    .addComponent(nodeGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(pipeline)
                    .addComponent(pipelineText)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(nodeLabel)
                    .addComponent(nodeText)
                    .addGap(ZiggyGuiConstants.GROUP_GAP)
                    .addComponent(requiredRemoteParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(environment)
                    .addComponent(remoteEnvironmentComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(oneSubtaskCheckBox)
                    .addComponent(wallTimeScalingCheckBox)
                    .addPreferredGap(ComponentPlacement.RELATED)
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
                    .addComponent(batchParametersGroup)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addGroup(dataPanelLayout.createParallelGroup()
                        .addGroup(labelGroup(dataPanelLayout.createSequentialGroup(),
                            batchParameterLabels, GroupLayout.DEFAULT_SIZE))
                        .addGroup(labelGroup(dataPanelLayout.createSequentialGroup(),
                            batchParameterValues, GroupLayout.DEFAULT_SIZE)))
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(batchLimits)))
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

    private Map<String, RemoteEnvironment> retrieveRemoteEnvironmentByName() {
        Map<String, RemoteEnvironment> remoteEnvironmentByName = new LinkedHashMap<>();
        remoteEnvironmentByName.put(DISABLED_REMOTE_ENVIRONMENT.getName(),
            DISABLED_REMOTE_ENVIRONMENT);
        remoteEnvironmentByName.putAll(remoteEnvironmentOperations().remoteEnvironmentByName());

        // Filter this map by the configured environments.
        List<String> configuredNames = ZiggyStringUtils.toList(ZiggyConfiguration.getInstance()
            .getString(PropertyName.REMOTE_ENVIRONMENTS.property(), null));
        remoteEnvironmentByName.entrySet()
            .removeIf(entry -> !configuredNames.stream()
                .anyMatch(name -> name.equalsIgnoreCase(entry.getKey()))
                && !entry.getKey().equals(DISABLED_REMOTE_ENVIRONMENT.getName()));

        return remoteEnvironmentByName;
    }

    private Group labelGroup(Group group, List<JLabel> labels, int minimumWidth) {
        for (JLabel label : labels) {
            group.addComponent(label, minimumWidth, GroupLayout.DEFAULT_SIZE,
                GroupLayout.DEFAULT_SIZE);
            if (group instanceof SequentialGroup) {
                ((SequentialGroup) group).addPreferredGap(ComponentPlacement.RELATED);
            }
        }
        return group;
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

        if (!remoteExecutionEnabled()) {
            displayBatchParameters(null);
            batchParametersValid = true;
        } else if (allFieldsRequiredForCalculationValid()) {
            batchParametersValid = calculateBatchParameters();
        } else {
            displayBatchParameters(remoteEnvironment().getBatchSystem().batchParameters());
            batchParametersValid = false;
        }

        closeButton.setEnabled(
            !remoteExecutionEnabled() || allFieldsRequiredForCloseValid() && batchParametersValid);

        log.debug(
            "remoteExecutionEnabled={}, allFieldsRequiredForCalculationValid()={}, batchParametersValid={}, allFieldsRequiredForCloseValid()={}",
            remoteExecutionEnabled(), allFieldsRequiredForCloseValid(), batchParametersValid,
            allFieldsRequiredForCloseValid());
    }

    private RemoteEnvironment remoteEnvironment() {
        return remoteEnvironmentByName.get(remoteEnvironmentComboBox.getSelectedItem());
    }

    private boolean remoteExecutionEnabled() {
        return remoteEnvironment() != DISABLED_REMOTE_ENVIRONMENT;
    }

    /**
     * Determines whether all the validity-checking fields are valid. Used to determine whether to
     * calculate the batch parameters.
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

    private JComboBox<String> createRemoteEnvironmentComboBox() {
        JComboBox<String> remoteEnvironmentComboBox = new JComboBox<>(
            remoteEnvironmentByName.keySet().toArray(new String[0]));
        if (originalConfiguration.getRemoteEnvironment() != null) {
            remoteEnvironmentComboBox
                .setSelectedItem(originalConfiguration.getRemoteEnvironment().getName());
        }
        architectureByDescription = remoteEnvironmentByName
            .get(remoteEnvironmentComboBox.getSelectedItem())
            .architectureByDescription();
        batchQueueByDescription = remoteEnvironmentByName
            .get(remoteEnvironmentComboBox.getSelectedItem())
            .queueByDescription();
        lastRemoteEnvironmentComboBoxSelection = (String) remoteEnvironmentComboBox
            .getSelectedItem();
        remoteEnvironmentComboBox.setToolTipText("Select remote environment for execution.");

        remoteEnvironmentComboBox.addItemListener(this::updateRemoteEnvironment);

        return remoteEnvironmentComboBox;
    }

    /** Perform the necessary operations when the user changes the remote environment. */
    private void updateRemoteEnvironment(ItemEvent evt) {

        if (evt.getStateChange() == ItemEvent.DESELECTED || !isVisible() || skipCheck) {
            return;
        }

        // Create the cache of remote environment parameters.
        log.debug("Creating remote environment cache for {}",
            lastRemoteEnvironmentComboBoxSelection);
        remoteCacheByEnvironmentName.put(lastRemoteEnvironmentComboBoxSelection,
            new RemoteEnvironmentCache());

        lastRemoteEnvironmentComboBoxSelection = (String) remoteEnvironmentComboBox
            .getSelectedItem();

        // Update the caches of architecture and batch queue.
        updateArchitectures();
        updateBatchQueues();

        // Set the fields on the dialog box based on the cache, if any.
        RemoteEnvironmentCache environmentCache = remoteCacheByEnvironmentName
            .get(remoteEnvironmentComboBox.getSelectedItem());
        if (environmentCache == null) {
            setEnvironmentDefaults();
            lastArchitectureComboBoxSelection = (String) architectureComboBox.getSelectedItem();
            updateArchitectureLimits();
            enableReservedQueue(true);
            updateLastQueueComboBoxSelection();
            updateBatchQueueDisplay();
            enableTextFieldsAndComboBoxes(remoteExecutionEnabled());
            checkFieldsAndRecalculate(null);
            return;
        }

        architectureComboBox.setSelectedItem(environmentCache.architectureDescription());
        lastArchitectureComboBoxSelection = (String) architectureComboBox.getSelectedItem();
        updateArchitectureLimits();
        queueComboBox.setSelectedItem(environmentCache.batchQueueDescription());
        reservedQueueName = environmentCache.getReservedQueueNameForEnvironment();
        enableReservedQueue(true);
        updateLastQueueComboBoxSelection();
        updateBatchQueueDisplay();
        maxNodesField.setValue(environmentCache.getMaxNodesPerTask());
        subtasksPerCoreField.setValue(environmentCache.getSubtasksPerCore());
        minSubtasksRemoteExecutionField
            .setValue(environmentCache.getMinSubtasksForRemoteExecution());

        enableTextFieldsAndComboBoxes(remoteExecutionEnabled());
        checkFieldsAndRecalculate(null);
    }

    /** Set the architectures for the new environment into the map. */
    private void updateArchitectures() {
        skipCheck = true;
        architectureByDescription = remoteEnvironment().architectureByDescription();
        DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) architectureComboBox
            .getModel();
        model.removeAllElements();
        model.addElement(RemoteEnvironment.ANY_ARCHITECTURE);
        for (String description : architectureByDescription.keySet()) {
            model.addElement(description);
        }
        skipCheck = false;
        architectureComboBox.setModel(model);
    }

    /** Set the batch queues for the new architecture into the map. */
    private void updateBatchQueues() {
        skipCheck = true;
        batchQueueByDescription = remoteEnvironment().queueByDescription();
        DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) queueComboBox
            .getModel();
        model.removeAllElements();
        model.addElement(RemoteEnvironment.ANY_QUEUE);
        for (String description : batchQueueByDescription.keySet()) {
            model.addElement(description);
        }
        skipCheck = false;
        queueComboBox.setModel(model);
    }

    /**
     * Set default values for environment-specific parameters, in the event that no cache exists.
     */
    private void setEnvironmentDefaults() {
        architectureComboBox.setSelectedItem(RemoteEnvironment.ANY_ARCHITECTURE);
        queueComboBox.setSelectedItem(RemoteEnvironment.ANY_QUEUE);
        reservedQueueName = "";
        maxNodesField.setValue(null);
        subtasksPerCoreField.setValue(null);
        minSubtasksRemoteExecutionField.setValue(null);
    }

    private JComboBox<String> createArchitectureComboBox() {
        JComboBox<String> architectureComboBox = new JComboBox<>(
            remoteEnvironment().architectureDescriptions().toArray(new String[0]));
        architectureComboBox.setToolTipText("Select remote node architecture.");
        architectureComboBox.addItemListener(this::validateArchitectureComboBox);
        return architectureComboBox;
    }

    private void validateArchitectureComboBox(ItemEvent evt) {

        if (evt.getStateChange() == ItemEvent.DESELECTED || !isVisible() || skipCheck) {
            return;
        }

        batchParametersValid = calculateBatchParameters();

        // Reset combo box if the chosen value is invalid. Note that this method is also called when
        // the dialog is first displayed, so if the configuration is invalid, simply set the last
        // selection to the current one and let the warning dialog shown by
        // handleBatchParametersException() guide the user.
        if (batchParametersValid || lastArchitectureComboBoxSelection == null) {
            lastArchitectureComboBoxSelection = (String) architectureComboBox.getSelectedItem();
        } else {
            architectureComboBox.setSelectedItem(lastArchitectureComboBoxSelection);
        }

        updateArchitectureLimits();
    }

    private void updateArchitectureLimits() {
        if (!lastArchitectureComboBoxSelection.equals(RemoteEnvironment.ANY_ARCHITECTURE)) {
            Architecture architecture = architectureByDescription
                .get(lastArchitectureComboBoxSelection);
            architectureLimits
                .setText(MessageFormat.format("{0} cores, {1} GB/core, {2} fractional {4}",
                    architecture.getCores(), architecture.gigsPerCore(), architecture.getCost(),
                    remoteEnvironment().getCostUnit()));
        } else {
            architectureLimits.setText("");
        }
    }

    private JComboBox<String> createQueueComboBox() {
        JComboBox<String> queueComboBox = new JComboBox<>(
            remoteEnvironment().queueDescriptions().toArray(new String[0]));
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

        boolean reservedQueue = enableReservedQueue(true);
        batchParametersValid = calculateBatchParameters();

        // Reset combo box if the chosen value is invalid, but allow the user to enter a reserved
        // queue name.
        if (batchParametersValid || reservedQueue) {
            updateLastQueueComboBoxSelection();
        } else {
            applyLastQueueComboBoxSelection();
        }
        if (lastQueueComboBoxSelection.equals(RemoteEnvironment.ANY_QUEUE)) {
            queueLimits.setText("");
            maxNodesField.setValue(null);
            return;
        }
        updateBatchQueueDisplay();
    }

    private boolean enableReservedQueue(boolean enabled) {
        boolean reservedQueue = enabled
            && queueComboBox.getSelectedItem() != RemoteEnvironment.ANY_QUEUE
            && batchQueueByDescription.get(queueComboBox.getSelectedItem()).isReserved();
        queueName.setEnabled(reservedQueue);
        queueNameField.setEnabled(reservedQueue);
        return reservedQueue;
    }

    private void updateLastQueueComboBoxSelection() {
        lastQueueComboBoxSelection = (String) queueComboBox.getSelectedItem();
    }

    private void applyLastQueueComboBoxSelection() {
        queueComboBox.setSelectedItem(lastQueueComboBoxSelection);
    }

    private void updateBatchQueueDisplay() {
        if (lastQueueComboBoxSelection.equals(RemoteEnvironment.ANY_QUEUE)) {
            queueLimits.setText("");
            maxNodesField.setValue(null);
            return;
        }
        BatchQueue queue = batchQueueByDescription.get(lastQueueComboBoxSelection);

        // Show queue limits.
        if (queue.getMaxWallTimeHours() > 0 && queue.getMaxWallTimeHours() < Float.MAX_VALUE) {
            queueLimits.setText(
                MessageFormat.format("{0} hrs max wall time", queue.getMaxWallTimeHours()));
        } else {
            queueLimits.setText("");
        }

        // Populate maxNodes field if applicable.
        int maxNodes = queue.getMaxNodes();
        if (maxNodes > 0 && maxNodes < Integer.MAX_VALUE) {
            maxNodesField.setValue(maxNodes);
        } else if (originalConfiguration.getMaxNodes() > 0) {
            maxNodesField.setValue(originalConfiguration.getMaxNodes());
        } else {
            maxNodesField.setValue(null);
        }
    }

    private ValidityTestingFormattedTextField createQueueNameField() {
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

        // Required parameters in the order they appear in the dialog, with one exception.
        // Since maxWallTimeField's error checking depends on the value of the typicalWallTimeField,
        // set the latter field first to avoid an error the first time this method is called.
        remoteEnvironmentComboBox.setSelectedItem(currentConfiguration.isRemoteExecutionEnabled()
            ? currentConfiguration.getRemoteEnvironment().getName()
            : DISABLED_REMOTE_ENVIRONMENT.getName());
        oneSubtaskCheckBox.setSelected(!currentConfiguration.isNodeSharing());
        wallTimeScalingCheckBox.setSelected(currentConfiguration.isWallTimeScaling());
        wallTimeScalingCheckBox.setEnabled(!currentConfiguration.isNodeSharing());
        tasksField.setValue(taskCount);
        subtasksField.setValue(subtaskCount);
        gigsPerSubtaskField.setValue(currentConfiguration.subtaskRamGigabytes());
        gigsPerSubtaskField.setEnabled(currentConfiguration.isNodeSharing());
        gigsPerSubtaskField.setToolTipText(gigsPerSubtaskToolTip());
        typicalWallTimeField.setValue(currentConfiguration.getSubtaskTypicalWallTimeHours());
        maxWallTimeField.setValue(currentConfiguration.getSubtaskMaxWallTimeHours());
        optimizerComboBox.setSelectedItem(currentConfiguration.getOptimizer());

        // Optional parameters.
        architectureComboBox.setSelectedItem(currentConfiguration.getArchitecture() != null
            ? currentConfiguration.getArchitecture().getDescription()
            : RemoteEnvironment.ANY_ARCHITECTURE);
        lastArchitectureComboBoxSelection = (String) architectureComboBox.getSelectedItem();

        if (!StringUtils.isBlank(currentConfiguration.getReservedQueueName())) {
            queueComboBox.setSelectedItem(currentConfiguration.getReservedQueueName());
            if (batchQueueByDescription.get(queueComboBox.getSelectedItem()).isReserved()) {
                reservedQueueName = currentConfiguration.getReservedQueueName();
            }
        }
        enableReservedQueue(true);
        queueComboBox.setSelectedItem(currentConfiguration.getBatchQueue() != null
            ? currentConfiguration.getBatchQueue().getDescription()
            : RemoteEnvironment.ANY_QUEUE);
        lastQueueComboBoxSelection = (String) queueComboBox.getSelectedItem();
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

        enableTextFieldsAndComboBoxes(remoteExecutionEnabled());
    }

    private void enableTextFieldsAndComboBoxes(boolean enabled) {
        skipCheck = true;
        oneSubtaskCheckBox.setEnabled(enabled);
        wallTimeScalingCheckBox.setEnabled(enabled && !currentConfiguration.isNodeSharing());
        tasksField.setEnabled(enabled);
        subtasksField.setEnabled(enabled);
        gigsPerSubtaskField.setEnabled(enabled && currentConfiguration.isNodeSharing());
        maxWallTimeField.setEnabled(enabled);
        typicalWallTimeField.setEnabled(enabled);
        optimizerComboBox.setEnabled(enabled);
        architectureComboBox.setEnabled(enabled);
        queueComboBox.setEnabled(enabled);
        enableReservedQueue(enabled);
        maxNodesField.setEnabled(enabled);
        subtasksPerCoreField.setEnabled(enabled);
        minSubtasksRemoteExecutionField.setEnabled(enabled);
        skipCheck = false;
    }

    private void resetAction(ActionEvent evt) {
        reset(true);
    }

    private void reset(boolean checkFields) {
        skipCheck = true;
        PipelineTaskInformation.reset(node);
        currentConfiguration = new PipelineNodeExecutionResources(originalConfiguration);
        taskCount = originalTaskCount;
        subtaskCount = originalSubtaskCount;
        updateArchitectures();
        updateBatchQueues();
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
     * Calculates the batch parameters from the user-defined parameters.
     *
     * @return true if the entries that contribute to the batch parameters are valid; otherwise,
     * false
     */
    private boolean calculateBatchParameters() {

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
            BatchParameters batchParameters = null;
            SupportedBatchSystem batchSystem = currentConfiguration.getRemoteEnvironment()
                .getBatchSystem();
            if (taskCount != originalTaskCount || subtaskCount != originalSubtaskCount) {
                Set<BatchParameters> perTaskBatchParameters = new HashSet<>();
                for (int i = 0; i < taskCount; i++) {
                    BatchParameters taskBatchParameters = batchSystem.batchParameters();
                    taskBatchParameters.computeParameterValues(currentConfiguration,
                        subtaskCount / taskCount);
                    perTaskBatchParameters.add(taskBatchParameters);
                }
                batchParameters = batchSystem.batchParametersAggregator()
                    .aggregate(perTaskBatchParameters);
            } else {
                Set<BatchParameters> perTaskBatchParameters = new HashSet<>();
                for (SubtaskInformation taskInformation : tasksInformation) {
                    BatchParameters taskBatchParameters = batchSystem.batchParameters();
                    taskBatchParameters.computeParameterValues(currentConfiguration,
                        taskInformation.getSubtaskCount());
                    perTaskBatchParameters.add(taskBatchParameters);
                }
                batchParameters = batchSystem.batchParametersAggregator()
                    .aggregate(perTaskBatchParameters);
            }
            displayBatchParameters(batchParameters);
        } catch (Exception e) {
            if ((e instanceof IllegalArgumentException || e instanceof IllegalStateException
                || e instanceof PipelineException) && handleBatchParametersException(e)) {
                displayBatchParameters(remoteExecutionEnabled()
                    ? remoteEnvironment().getBatchSystem().batchParameters()
                    : null);
                return false;
            }
            throw e;
        }
        return true;
    }

    private void populateCurrentParameters() {

        // Required parameters in the order they appear in the dialog
        currentConfiguration.setRemoteExecutionEnabled(remoteExecutionEnabled());
        currentConfiguration
            .setRemoteEnvironment(remoteExecutionEnabled() ? remoteEnvironment() : null);
        currentConfiguration.setNodeSharing(!oneSubtaskCheckBox.isSelected());
        currentConfiguration.setWallTimeScaling(wallTimeScalingCheckBox.isSelected());
        taskCount = textToInt(tasksField);
        subtaskCount = textToInt(subtasksField);
        currentConfiguration.setSubtaskRamGigabytes(textToDouble(gigsPerSubtaskField));
        currentConfiguration.setSubtaskMaxWallTimeHours(textToDouble(maxWallTimeField));
        currentConfiguration.setSubtaskTypicalWallTimeHours(textToDouble(typicalWallTimeField));
        currentConfiguration
            .setOptimizer((RemoteArchitectureOptimizer) optimizerComboBox.getSelectedItem());

        // Optional parameters.
        currentConfiguration.setArchitecture(architectureComboBox.getSelectedItem() == null
            || architectureComboBox.getSelectedItem().equals(RemoteEnvironment.ANY_ARCHITECTURE)
                ? null
                : architectureByDescription.get(architectureComboBox.getSelectedItem()));

        if (queueComboBox.getSelectedItem() == null
            || queueComboBox.getSelectedItem().equals(RemoteEnvironment.ANY_QUEUE)) {
            currentConfiguration.setReservedQueueName("");
            currentConfiguration.setBatchQueue(null);
        } else {
            BatchQueue queue = batchQueueByDescription.get(queueComboBox.getSelectedItem());
            if (queue.isReserved()) {
                reservedQueueName = queueNameField.getText().trim();
                currentConfiguration.setReservedQueueName(reservedQueueName);
                currentConfiguration.setBatchQueue(
                    BatchQueue.reservedBatchQueueWithQueueName(queue, reservedQueueName));
            } else {
                currentConfiguration.setReservedQueueName(reservedQueueName);
                currentConfiguration.setBatchQueue(queue);
            }
        }

        currentConfiguration.setMaxNodes(textToInt(maxNodesField));
        currentConfiguration.setSubtasksPerCore(textToDouble(subtasksPerCoreField));
        currentConfiguration.setMinSubtasksForRemoteExecution(
            minSubtasksRemoteExecutionField.getText().isBlank() ? -1
                : textToInt(minSubtasksRemoteExecutionField));
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

    private void displayBatchParameters(BatchParameters batchParameters) {
        if (batchParameters == null) {
            batchParametersGroup.setText("Batch parameters");
            batchLimits.setText("Remote execution disabled");
            for (JLabel label : batchParameterLabels) {
                label.setText("");
            }
            for (JLabel label : batchParameterValues) {
                label.setText("");
            }
            return;
        }

        batchParametersGroup.setText(batchParameters.batchParameterSetName());
        batchLimits.setText(batchParameters.displayMessage());
        Map<String, String> parametersByName = batchParameters
            .batchParametersByName(remoteEnvironment().getCostUnit());
        int i = 0;
        for (Map.Entry<String, String> entry : parametersByName.entrySet()) {
            batchParameterLabels.get(i).setText(entry.getKey());
            batchParameterValues.get(i).setText(entry.getValue());
            i++;
        }
    }

    private boolean handleBatchParametersException(Exception e) {
        for (StackTraceElement element : e.getStackTrace()) {
            if (element.getMethodName().equals("computeParameterValues")) {
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

    public PipelineNodeExecutionResources getCurrentConfiguration() {
        return currentConfiguration;
    }

    private PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }

    private RemoteEnvironmentOperations remoteEnvironmentOperations() {
        return remoteEnvironmentOperations;
    }

    /**
     * Allows remote environment parameters to be cached when the environment is switched, thus
     * permitting the cached parameters to be put back if the environment is switched back.
     */
    private class RemoteEnvironmentCache {
        private final String remoteEnvironmentName;
        private final Architecture architecture;
        private final BatchQueue batchQueue;
        private final String reservedQueueNameForEnvironment;
        private final int maxNodesPerTask;
        private final double subtasksPerCore;
        private final int minSubtasksForRemoteExecution;

        public RemoteEnvironmentCache() {
            remoteEnvironmentName = lastRemoteEnvironmentComboBoxSelection;
            architecture = !RemoteEnvironment.ANY_ARCHITECTURE
                .equals(lastArchitectureComboBoxSelection)
                    ? architectureByDescription.get(lastArchitectureComboBoxSelection)
                    : null;
            batchQueue = !RemoteEnvironment.ANY_QUEUE.equals(lastQueueComboBoxSelection)
                ? batchQueueByDescription.get(lastQueueComboBoxSelection)
                : null;
            reservedQueueNameForEnvironment = reservedQueueName;
            maxNodesPerTask = textToInt(maxNodesField);
            subtasksPerCore = textToDouble(subtasksPerCoreField);
            minSubtasksForRemoteExecution = textToInt(minSubtasksRemoteExecutionField);
        }

        public String architectureDescription() {
            return architecture != null ? architecture.getDescription()
                : RemoteEnvironment.ANY_ARCHITECTURE;
        }

        public String batchQueueDescription() {
            return batchQueue != null ? batchQueue.getDescription() : RemoteEnvironment.ANY_QUEUE;
        }

        public String getReservedQueueNameForEnvironment() {
            return reservedQueueNameForEnvironment;
        }

        public Integer getMaxNodesPerTask() {
            return maxNodesPerTask > 0 ? maxNodesPerTask : null;
        }

        public Double getSubtasksPerCore() {
            return subtasksPerCore > 0.0 ? subtasksPerCore : null;
        }

        public Integer getMinSubtasksForRemoteExecution() {
            return minSubtasksForRemoteExecution > 0 ? minSubtasksForRemoteExecution : null;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getEnclosingInstance().hashCode();
            return prime * result + Objects.hash(remoteEnvironmentName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RemoteEnvironmentCache other = (RemoteEnvironmentCache) obj;
            if (!getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(remoteEnvironmentName, other.remoteEnvironmentName);
        }

        private RemoteExecutionDialog getEnclosingInstance() {
            return RemoteExecutionDialog.this;
        }
    }
}
