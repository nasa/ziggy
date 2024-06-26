package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.GroupLayout;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SpinnerListModel;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.TriggerValidationResults;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.proxy.PipelineDefinitionCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineDefinitionNodeCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditPipelineDialog extends javax.swing.JDialog {

    private static final Logger log = LoggerFactory.getLogger(EditPipelineDialog.class);

    private JSpinner prioritySpinner;
    private JLabel pipelineNameTextField;
    private JCheckBox validCheckBox;
    private JList<String> modulesList;

    private JRadioButton reprocessButton;

    private PipelineDefinition pipeline;
    private String pipelineName;
    private PipelineModulesListModel pipelineModulesListModel;
    private ZiggyTreeModel<PipelineDefinition> pipelineModel;

    // Contains all parameter sets that have been edited since this
    // window was opened.
    private Map<String, ParametersInterface> editedParameterSets = new HashMap<>();

    // Contains all pipeline definition nodes with updated execution resources.
    private Map<Long, PipelineDefinitionNodeExecutionResources> updatedExecutionResources = new HashMap<>();

    public EditPipelineDialog(Window owner, String pipelineName,
        ZiggyTreeModel<PipelineDefinition> pipelineModel) {
        this(owner, pipelineName, null, pipelineModel);
    }

    public EditPipelineDialog(Window owner, PipelineDefinition pipeline,
        ZiggyTreeModel<PipelineDefinition> pipelineModel) {
        this(owner, null, pipeline, pipelineModel);
    }

    public EditPipelineDialog(Window owner, String pipelineName, PipelineDefinition pipeline,
        ZiggyTreeModel<PipelineDefinition> pipelineModel) {

        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipeline = pipeline;
        this.pipelineName = !StringUtils.isEmpty(pipelineName) ? pipelineName
            : this.pipeline.getName();
        this.pipelineModel = pipelineModel;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit pipeline");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane()
            .add(ZiggySwingUtils.createButtonPanel(ZiggySwingUtils.createButton(SAVE, this::save),
                ZiggySwingUtils.createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        if (pipeline != null) {
            pipelineNameTextField.setText(pipeline.getName());
        }
        if (pipelineName != null) {
            pipelineNameTextField.setText(pipelineName);
        }

        pack();
    }

    private JPanel createDataPanel() {
        JLabel pipelineGroup = boldLabel("Pipeline", LabelType.HEADING1);

        JPanel pipelineToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("Validate", this::validate), createButton(REPORT, this::report),
            createButton("Export parameters", this::exportPipelineParameters));

        JLabel pipelineName = boldLabel("Name");
        pipelineNameTextField = new JLabel();

        JLabel priority = boldLabel("Priority");
        SpinnerListModel spinnerListModel = new SpinnerListModel(prioritySpinnerListModel());
        prioritySpinner = new JSpinner(spinnerListModel);
        prioritySpinner.setPreferredSize(new java.awt.Dimension(100, 22));
        spinnerListModel.setValue(pipeline.getInstancePriority().name() + "");

        JLabel valid = boldLabel("Valid? ");
        validCheckBox = new JCheckBox();
        validCheckBox.setEnabled(false);

        JLabel processingMode = boldLabel("Processing mode");
        ButtonGroup processConfigButtonGroup = new ButtonGroup();
        reprocessButton = new JRadioButton("Process all data");
        JRadioButton forwardProcessButton = new JRadioButton("Process new data");
        processConfigButtonGroup.add(reprocessButton);
        processConfigButtonGroup.add(forwardProcessButton);

        if (new PipelineDefinitionCrudProxy()
            .retrieveProcessingMode(this.pipelineName) == ProcessingMode.PROCESS_ALL) {
            reprocessButton.setSelected(true);
        } else {
            forwardProcessButton.setSelected(true);
        }

        JLabel pipelineParameterSetsGroup = boldLabel("Pipeline parameter sets",
            LabelType.HEADING1);

        ParameterSetMapEditorPanel pipelineParameterSetMapEditorPanel = new ParameterSetMapEditorPanel(
            pipeline.getPipelineParameterSetNames(), new HashMap<>(), editedParameterSets);
        pipelineParameterSetMapEditorPanel
            .setMapListener(source -> pipeline.setPipelineParameterSetNames(
                pipelineParameterSetMapEditorPanel.getParameterSetsMap()));

        JLabel modulesGroup = boldLabel("Modules", LabelType.HEADING1);

        JPanel modulesToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("Task information", this::displayTaskInformation),
            createButton("Resources", this::configureResources),
            createButton("Parameters", this::editModuleParameters),
            createButton("Remote execution", this::configureRemoteExecution));

        pipelineModulesListModel = new PipelineModulesListModel(pipeline);
        modulesList = new JList<>(pipelineModulesListModel);
        modulesList.setVisibleRowCount(pipelineModulesListModel.getSize());
        JScrollPane modulesListScrollPane = new JScrollPane(modulesList);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(pipelineGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggySwingUtils.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(pipelineToolBar)
                    .addComponent(pipelineName)
                    .addComponent(pipelineNameTextField)
                    .addComponent(priority)
                    .addComponent(prioritySpinner, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addComponent(valid)
                            .addComponent(validCheckBox)))
                    .addComponent(processingMode)
                    .addComponent(reprocessButton)
                    .addComponent(forwardProcessButton)))
            .addComponent(pipelineParameterSetsGroup)
            .addComponent(pipelineParameterSetMapEditorPanel)
            .addComponent(modulesGroup)
            .addComponent(modulesToolBar)
            .addComponent(modulesListScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(pipelineGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(pipelineToolBar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(pipelineName)
            .addComponent(pipelineNameTextField, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(priority)
            .addComponent(prioritySpinner, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(valid)
                .addComponent(validCheckBox))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(processingMode)
            .addComponent(reprocessButton)
            .addComponent(forwardProcessButton)
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(pipelineParameterSetsGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(pipelineParameterSetMapEditorPanel)
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(modulesGroup)
            .addComponent(modulesToolBar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(modulesListScrollPane));

        return dataPanel;
    }

    private void validate(ActionEvent evt) {

        PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();

        TriggerValidationResults results = null;
        try {
            results = pipelineOps.validatePipeline(pipeline);
            validCheckBox.setSelected(!results.hasErrors());
            if (results.hasErrors()) {
                PipelineValidationResultsDialog.showValidationResults(this, results);
            } else {
                JOptionPane.showMessageDialog(this, "This pipeline is valid", "Validation OK",
                    JOptionPane.INFORMATION_MESSAGE);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void report(ActionEvent evt) {

        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generatePipelineReport(pipeline);

        TextualReportDialog.showReport(this, report, "Pipeline report",
            ReportFilePaths.triggerReportPath(pipeline.getName()));
    }

    /**
     * @param evt
     */
    private void exportPipelineParameters(ActionEvent evt) {

        try {
            JFileChooser fc = new JFileChooser();
            fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

            int returnVal = fc.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();

                PipelineOperationsProxy ops = new PipelineOperationsProxy();
                ops.exportPipelineParams(pipeline, file);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void editModuleParameters(ActionEvent evt) {

        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
                .getPipelineNodeAt(selectedRow);

            Map<ClassWrapper<ParametersInterface>, String> currentModuleParams = pipelineNode
                .getModuleParameterSetNames();
            Map<ClassWrapper<ParametersInterface>, String> currentPipelineParams = pipeline
                .getPipelineParameterSetNames();

            try {
                final ModuleParameterSetMapEditorDialog dialog = new ModuleParameterSetMapEditorDialog(
                    this, currentModuleParams, currentPipelineParams, editedParameterSets);

                dialog.setMapListener(source -> pipelineNode
                    .setModuleParameterSetNames(dialog.getParameterSetsMap()));

                dialog.setVisible(true);
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    private void save(ActionEvent evt) {

        PipelineDefinitionCrudProxy pipelineCrud = new PipelineDefinitionCrudProxy();

        try {
            String newName = pipelineNameTextField.getText();

            PipelineDefinition existingPipeline = pipelineModel.objectByName(newName);

            if (existingPipeline != null && !newName.equals(pipeline.getName())) {
                // Operator changed pipeline name & it conflicts with an existing
                // pipeline.
                JOptionPane.showMessageDialog(this,
                    "Pipeline name already used, please enter a different name.",
                    "Duplicate pipeline name", JOptionPane.ERROR_MESSAGE);
                return;
            }

            pipeline.rename(newName);
            pipeline.setInstancePriority(Priority.valueOf((String) prioritySpinner.getValue()));

            pipelineCrud.createOrUpdate(pipeline);

            // Save any parameter sets that have been touched since the dialog box was opened.
            PipelineOperationsProxy pipelineOperationsProxy = new PipelineOperationsProxy();
            for (Map.Entry<String, ParametersInterface> mapEntry : editedParameterSets.entrySet()) {
                pipelineOperationsProxy.updateParameterSet(mapEntry.getKey(), mapEntry.getValue());
            }

            // Save any pipeline definition nodes that have been touched since the dialog box
            // was opened.
            PipelineDefinitionNodeCrudProxy nodeProxy = new PipelineDefinitionNodeCrudProxy();
            for (PipelineDefinitionNodeExecutionResources executionResources : updatedExecutionResources
                .values()) {
                updatedExecutionResources.put(executionResources.getId(),
                    nodeProxy.merge(executionResources));
            }

            // Update the reprocess selection.
            ProcessingMode processingMode = reprocessButton.isSelected()
                ? ProcessingMode.PROCESS_ALL
                : ProcessingMode.PROCESS_NEW;
            new PipelineDefinitionCrudProxy().updateProcessingMode(newName, processingMode);

            setVisible(false);
        } catch (Throwable e) {
            MessageUtil.showError(this, "Error saving pipeline", e.getMessage(), e);
        }
    }

    private void cancel(ActionEvent evt) {
        setVisible(false);
    }

    private void displayTaskInformation(ActionEvent evt) {
        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
                .getPipelineNodeAt(selectedRow);
            TaskInformationDialog infoTable = new TaskInformationDialog(this, pipelineNode);
            infoTable.setVisible(true);
        }
    }

    private void configureRemoteExecution(ActionEvent evt) {
        PipelineDefinitionNode pipelineNode = prepNodeForResourcesUpdate();
        if (pipelineNode == null) {
            return;
        }

        PipelineDefinitionNodeExecutionResources executionResources = updatedExecutionResources
            .get(pipelineNode.getId());
        RemoteExecutionDialog remoteExecutionDialog = new RemoteExecutionDialog(this,
            executionResources, pipelineNode,
            PipelineTaskInformation.subtaskInformation(pipelineNode));
        remoteExecutionDialog.setVisible(true);
        log.debug("original == current? {}",
            executionResources.equals(remoteExecutionDialog.getCurrentConfiguration()));
        executionResources.populateFrom(remoteExecutionDialog.getCurrentConfiguration());
    }

    /**
     * Helper method for locating a {@link PipelineDefinitionNode} instance from the dialog and
     * retrieving (or creating) its {@link PipelineDefinitionNodeExecutionResources} instance. The
     * pair are added to the map used to track nodes with updated resources.
     */
    private PipelineDefinitionNode prepNodeForResourcesUpdate() {
        int selectedRow = modulesList.getSelectedIndex();
        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
            return null;
        }

        final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
            .getPipelineNodeAt(selectedRow);

        // Make sure the pipeline node is in the set of nodes with edited remote parameters.
        if (!updatedExecutionResources.containsKey(pipelineNode.getId())) {
            updatedExecutionResources.put(pipelineNode.getId(),
                new PipelineDefinitionNodeCrudProxy()
                    .retrieveRemoteExecutionConfiguration(pipelineNode));
        }
        return pipelineNode;
    }

    private void configureResources(ActionEvent evt) {
        PipelineDefinitionNode pipelineNode = prepNodeForResourcesUpdate();
        if (pipelineNode == null) {
            return;
        }
        PipelineDefinitionNodeExecutionResources executionResources = updatedExecutionResources
            .get(pipelineNode.getId());
        new PipelineDefinitionNodeResourcesDialog(this, pipelineName, pipelineNode,
            executionResources).setVisible(true);
    }

    /**
     * Constructs a String array with the names of the {@link Priority} enum. The list is generated
     * in reverse order so that on the spinner the up-arrow button leads to higher priorities, the
     * down-arrow to lower ones.
     */
    private String[] prioritySpinnerListModel() {
        int priorityCount = Priority.values().length;
        String[] prioritySpinnerListModel = new String[priorityCount];
        for (int priorityCounter = 0; priorityCounter < prioritySpinnerListModel.length; priorityCounter++) {
            prioritySpinnerListModel[priorityCounter] = Priority.values()[priorityCount - 1
                - priorityCounter].name();
        }
        return prioritySpinnerListModel;
    }
}
