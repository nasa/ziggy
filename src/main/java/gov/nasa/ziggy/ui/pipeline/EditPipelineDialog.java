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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.ButtonGroup;
import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SpinnerListModel;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.services.messages.ParametersChangedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditPipelineDialog extends javax.swing.JDialog {

    private static final Logger log = LoggerFactory.getLogger(EditPipelineDialog.class);

    private JSpinner prioritySpinner;
    private JLabel pipelineNameTextField;
    private JList<String> modulesList;

    private JRadioButton reprocessButton;

    private PipelineDefinition pipeline;
    private Map<String, ParameterSet> parameterSetByName;
    private PipelineModulesListModel pipelineModulesListModel;

    // Contains all parameter sets that have been edited since this
    // window was opened.
    private Map<String, ParameterSet> editedParameterSetByName = new HashMap<>();

    // Contains all pipeline definition nodes with updated execution resources.
    private Map<Long, PipelineDefinitionNodeExecutionResources> updatedExecutionResources = new HashMap<>();

    private boolean cancelled;

    private final PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private final PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private final ParametersOperations parametersOperations = new ParametersOperations();

    public EditPipelineDialog(Window owner, PipelineDefinition pipeline) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipeline = pipeline;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit pipeline");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            ZiggySwingUtils.createButtonPanel(
                ZiggySwingUtils.createButton(SAVE, EditPipelineDialog.this::save),
                ZiggySwingUtils.createButton(CANCEL, EditPipelineDialog.this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel pipelineGroup = boldLabel("Pipeline", LabelType.HEADING1);

        JPanel pipelineToolBar = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(REPORT, this::report));

        JLabel pipelineName = boldLabel("Name");
        pipelineNameTextField = new JLabel(pipeline.getName());

        JLabel priority = boldLabel("Priority");
        SpinnerListModel spinnerListModel = new SpinnerListModel(prioritySpinnerListModel());
        prioritySpinner = new JSpinner(spinnerListModel);
        prioritySpinner.setPreferredSize(new java.awt.Dimension(100, 22));
        spinnerListModel.setValue(pipeline.getInstancePriority().name() + "");

        JLabel processingMode = boldLabel("Processing mode");
        ButtonGroup processConfigButtonGroup = new ButtonGroup();
        reprocessButton = new JRadioButton("Process all data");
        JRadioButton forwardProcessButton = new JRadioButton("Process new data");
        processConfigButtonGroup.add(reprocessButton);
        processConfigButtonGroup.add(forwardProcessButton);

        if (pipelineDefinitionOperations()
            .processingMode(pipeline.getName()) == ProcessingMode.PROCESS_ALL) {
            reprocessButton.setSelected(true);
        } else {
            forwardProcessButton.setSelected(true);
        }

        JLabel pipelineParameterSetsGroup = boldLabel("Pipeline parameter sets",
            LabelType.HEADING1);

        parameterSetByName = ParameterSet.parameterSetByName(parametersOperations()
            .parameterSets(pipelineDefinitionOperations().parameterSetNames(pipeline)));
        ParameterSetMapEditorPanel pipelineParameterSetMapEditorPanel = new ParameterSetMapEditorPanel(
            parameterSetByName, editedParameterSetByName);
        pipelineParameterSetMapEditorPanel.setMapListener(source -> pipeline.setParameterSetNames(
            pipelineParameterSetMapEditorPanel.getModuleParameterSetByName().keySet()));

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
                .addGap(ZiggyGuiConstants.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(pipelineToolBar)
                    .addComponent(pipelineName)
                    .addComponent(pipelineNameTextField)
                    .addComponent(priority)
                    .addComponent(prioritySpinner, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
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
            .addComponent(processingMode)
            .addComponent(reprocessButton)
            .addComponent(forwardProcessButton)
            .addGap(ZiggyGuiConstants.GROUP_GAP)
            .addComponent(pipelineParameterSetsGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(pipelineParameterSetMapEditorPanel)
            .addGap(ZiggyGuiConstants.GROUP_GAP)
            .addComponent(modulesGroup)
            .addComponent(modulesToolBar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(modulesListScrollPane));

        return dataPanel;
    }

    private void report(ActionEvent evt) {
        new SwingWorker<String, String>() {
            @Override
            protected String doInBackground() throws Exception {
                return new PipelineReportGenerator().generatePipelineReport(pipeline);
            }

            @Override
            protected void done() {
                try {
                    TextualReportDialog.showReport(EditPipelineDialog.this, get(),
                        "Pipeline report", ReportFilePaths.triggerReportPath(pipeline.getName()));
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(getRootPane(), e);
                }
            }
        }.execute();
    }

    private void editModuleParameters(ActionEvent evt) {

        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtils.showError(this, "No module selected");
            return;
        }

        final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
            .getPipelineNodeAt(selectedRow);
        new SwingWorker<Map<String, ParameterSet>, Void>() {
            @Override
            protected Map<String, ParameterSet> doInBackground() throws Exception {
                log.debug("Loading latest parameter sets for module {}...",
                    pipelineNode.getModuleName());
                return ParameterSet.parameterSetByName(new ParametersOperations().parameterSets(
                    new PipelineDefinitionNodeOperations().parameterSetNames(pipelineNode)));
            }

            @Override
            protected void done() {
                try {
                    Map<String, ParameterSet> moduleParameterSetsByName = get();
                    log.debug("Loading latest parameter sets for module {}...done",
                        pipelineNode.getModuleName());

                    final ModuleParameterSetMapEditorDialog dialog = new ModuleParameterSetMapEditorDialog(
                        EditPipelineDialog.this, moduleParameterSetsByName, parameterSetByName,
                        editedParameterSetByName);

                    dialog.setMapListener(source -> pipelineNode
                        .setParameterSetNames(dialog.getModuleParameterSetByName().keySet()));

                    dialog.setVisible(true);
                } catch (ExecutionException | InterruptedException e) {
                    MessageUtils.showError(EditPipelineDialog.this, e);
                }
            }
        }.execute();
    }

    private void save(ActionEvent evt) {

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipeline.setInstancePriority(Priority.valueOf((String) prioritySpinner.getValue()));

                pipelineDefinitionOperations().merge(pipeline);

                // Save any parameter sets that have been touched since the dialog box was opened.
                for (Map.Entry<String, ParameterSet> mapEntry : editedParameterSetByName
                    .entrySet()) {
                    parametersOperations().updateParameterSet(mapEntry.getKey(),
                        mapEntry.getValue());
                }

                // Let the parameters view-edit panel know that it has to refresh from the database.
                if (!editedParameterSetByName.isEmpty()) {
                    ZiggyMessenger.publish(new ParametersChangedMessage());
                }

                // Save any pipeline definition nodes that have been touched since the dialog box
                // was opened.
                for (PipelineDefinitionNodeExecutionResources executionResources : updatedExecutionResources
                    .values()) {
                    updatedExecutionResources.put(executionResources.getId(),
                        pipelineDefinitionNodeOperations()
                            .mergeExecutionResources(executionResources));
                }

                // Update the reprocess selection.
                ProcessingMode processingMode = reprocessButton.isSelected()
                    ? ProcessingMode.PROCESS_ALL
                    : ProcessingMode.PROCESS_NEW;
                pipelineDefinitionOperations().updateProcessingMode(pipeline.getName(),
                    processingMode);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    setVisible(false);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(EditPipelineDialog.this, "Error saving pipeline",
                        e.getMessage(), e);
                }
            }
        }.execute();
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    private void displayTaskInformation(ActionEvent evt) {
        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtils.showError(this, "No module selected");
        } else {
            final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
                .getPipelineNodeAt(selectedRow);
            TaskInformationDialog infoTable = new TaskInformationDialog(this, pipelineNode);
            infoTable.setVisible(true);
        }
    }

    private void configureRemoteExecution(ActionEvent evt) {
        new SwingWorker<PipelineDefinitionNode, Void>() {
            @Override
            protected PipelineDefinitionNode doInBackground() throws Exception {
                return prepNodeForResourcesUpdate();
            }

            @Override
            protected void done() {
                try {
                    PipelineDefinitionNode pipelineNode = get();

                    if (pipelineNode == null) {
                        return;
                    }

                    PipelineDefinitionNodeExecutionResources executionResources = updatedExecutionResources
                        .get(pipelineNode.getId());
                    RemoteExecutionDialog remoteExecutionDialog = new RemoteExecutionDialog(
                        EditPipelineDialog.this, executionResources, pipelineNode,
                        PipelineTaskInformation.subtaskInformation(pipelineNode));
                    remoteExecutionDialog.setVisible(true);
                    log.debug("original == current? {}",
                        executionResources.equals(remoteExecutionDialog.getCurrentConfiguration()));
                    executionResources
                        .populateFrom(remoteExecutionDialog.getCurrentConfiguration());
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not load execution resources", e);
                }
            }
        }.execute();
    }

    /**
     * Helper method for locating a {@link PipelineDefinitionNode} instance from the dialog and
     * retrieving (or creating) its {@link PipelineDefinitionNodeExecutionResources} instance. The
     * pair are added to the map used to track nodes with updated resources.
     */
    private PipelineDefinitionNode prepNodeForResourcesUpdate() {
        int selectedRow = modulesList.getSelectedIndex();
        if (selectedRow == -1) {
            MessageUtils.showError(this, "No module selected");
            return null;
        }

        final PipelineDefinitionNode pipelineNode = pipelineModulesListModel
            .getPipelineNodeAt(selectedRow);

        // Make sure the pipeline node is in the set of nodes with edited remote parameters.
        if (!updatedExecutionResources.containsKey(pipelineNode.getId())) {
            updatedExecutionResources.put(pipelineNode.getId(), pipelineDefinitionNodeOperations()
                .pipelineDefinitionNodeExecutionResources(pipelineNode));
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
        new PipelineDefinitionNodeResourcesDialog(this, pipeline.getName(), pipelineNode,
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

    public boolean isCancelled() {
        return cancelled;
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }

    private ParametersOperations parametersOperations() {
        return parametersOperations;
    }
}
