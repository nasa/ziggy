package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerListModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.PipelineTaskInformation;
import gov.nasa.ziggy.pipeline.TriggerValidationResults;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.ops.instances.TextualReportDialog;
import gov.nasa.ziggy.ui.ops.parameters.ParameterSetMapEditorDialog;
import gov.nasa.ziggy.ui.ops.parameters.ParameterSetMapEditorPanel;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditTriggerDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(EditTriggerDialog.class);

    private JPanel modulesPanel;
    private JButton syncButton;
    private JButton validateButton;
    private JSpinner prioritySpinner;
    private JPanel prioriytPanel;
    private JCheckBox validCheckBox;
    private JLabel validLabel;
    private JTextField pipelineDefNameTextField;
    private JLabel pipelineLabel;
    private JTextField triggerNameTextField;
    private JLabel triggerNameLabel;
    private JButton cancelButton;
    private JButton saveButton;
    private JPanel actionPanel;
    private JButton editModulesButton;
    private JPanel modulesButtonPanel;
    private JList<String> modulesList;
    private JScrollPane modulesScrollPane;
    private ParameterSetMapEditorPanel parameterSetMapEditorPanel;
    private JButton exportButton;
    private JPanel labelsPanel;
    private JPanel dataPanel;

    private PipelineDefinition trigger;
    private JButton reportButton;

    private JButton infoButton;
    private JButton remoteButton;

    private TriggerModulesListModel triggerModulesListModel;
    private TriggersTreeModel allTriggersModel;

    public EditTriggerDialog(JFrame frame, PipelineDefinition trigger,
        TriggersTreeModel triggerModel) {
        super(frame, true);
        this.trigger = trigger;
        allTriggersModel = triggerModel;

        initGUI();
    }

    /* For Jigloo use only */
    public EditTriggerDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    /**
     * Disabled for now since it doesn't work in all cases.
     *
     * @param evt
     */
    private void syncButtonActionPerformed(ActionEvent evt) {
        log.debug("syncButton.actionPerformed, event=" + evt);

        // verify
        int ans = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to re-sync this trigger?  "
                + "This will remove all parameter sets at the module level.",
            "Are you sure?", JOptionPane.YES_NO_OPTION);

        if (ans == JOptionPane.YES_OPTION) {
            PipelineDefinitionCrudProxy pipelineCrud = new PipelineDefinitionCrudProxy();
            pipelineCrud.update(trigger);

            triggerModulesListModel.loadFromDatabase();
        }
    }

    private void validateButtonActionPerformed(ActionEvent evt) {
        log.debug("validateButton.actionPerformed, event=" + evt);

        PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();

        TriggerValidationResults results = null;
        try {
            results = pipelineOps.validateTrigger(trigger);

            if (results.hasErrors()) {
                TriggerValidationResultsDialog.showValidationResults(this, results);
            } else {
                JOptionPane.showMessageDialog(this, "This trigger is valid", "Validation OK",
                    JOptionPane.INFORMATION_MESSAGE);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void reportButtonActionPerformed(ActionEvent evt) {
        log.debug("reportButton.actionPerformed, event=" + evt);

        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generateTriggerReport(trigger);

        TextualReportDialog.showReport(this, report,
            ReportFilePaths.triggerReportPath(trigger.getName().getName()));
    }

    /**
     * @param evt
     */
    private void exportButtonActionPerformed(ActionEvent evt) {
        log.debug("exportButton.actionPerformed, event=" + evt);

        try {
            JFileChooser fc = new JFileChooser();
            fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

            int returnVal = fc.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();

                PipelineOperationsProxy ops = new PipelineOperationsProxy();
                ops.exportPipelineParams(trigger, file);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void initGUI() {
        try {
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);

            if (trigger != null) {
                triggerNameTextField.setText(trigger.getName().getName());
                pipelineDefNameTextField.setText(trigger.getName().toString());
            }

            setTitle("Edit Trigger");
            this.setSize(572, 603);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void editModulesButtonActionPerformed(ActionEvent evt) {
        log.debug("editModulesButton.actionPerformed, event=" + evt);

        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No parameter set selected");
        } else {
            final PipelineDefinitionNode triggerNode = triggerModulesListModel
                .getTriggerNodeAt(selectedRow);

            PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
            Set<ClassWrapper<Parameters>> allRequiredParams = pipelineOps
                .retrieveRequiredParameterClassesForNode(triggerNode);

            Map<ClassWrapper<Parameters>, ParameterSetName> currentParams = triggerNode
                .getModuleParameterSetNames();
            Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParams = trigger
                .getPipelineParameterSetNames();

            final ParameterSetMapEditorDialog dialog = new ParameterSetMapEditorDialog(this,
                currentParams, allRequiredParams, currentPipelineParams);

            dialog.setMapListener(
                source -> triggerNode.setModuleParameterSetNames(dialog.getParameterSetsMap()));

            dialog.setVisible(true); // block until user dismisses
        }
    }

    private void saveButtonActionPerformed(ActionEvent evt) {
        log.debug("saveButton.actionPerformed, event=" + evt);

        PipelineDefinitionCrudProxy pipelineCrud = new PipelineDefinitionCrudProxy();

        try {
            String newName = triggerNameTextField.getText();

            PipelineDefinition existingTrigger = allTriggersModel.triggerByName(newName);

            if (existingTrigger != null && existingTrigger.getId() != trigger.getId()) {
                // operator changed trigger name & it conflicts with an existing
                // trigger
                JOptionPane.showMessageDialog(this,
                    "Trigger name already used, please enter a different name.",
                    "Duplicate Trigger Name", JOptionPane.ERROR_MESSAGE);
                return;
            }

            String priorityString = (String) prioritySpinner.getValue();
            int priority;

            try {
                priority = Integer.parseInt(priorityString);
            } catch (NumberFormatException e) {
                JOptionPane.showMessageDialog(this, e.getMessage(),
                    "Error parsing priority: " + priorityString, JOptionPane.ERROR_MESSAGE);
                return;
            }

            trigger.rename(newName);
            trigger.setInstancePriority(priority);

            pipelineCrud.save(trigger);

            setVisible(false);
        } catch (Throwable e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Error Saving Trigger",
                JOptionPane.ERROR_MESSAGE);
        }
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);
        setVisible(false);
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getLabelsPanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getPrioriytPanel(), new GridBagConstraints(0, 1, 4, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getParameterSetMapEditorPanel(),
                new GridBagConstraints(0, 2, 1, 4, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getModulesPanel(), new GridBagConstraints(0, 6, 1, 4, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JPanel getLabelsPanel() {
        if (labelsPanel == null) {
            labelsPanel = new JPanel();
            GridBagLayout labelsPanelLayout = new GridBagLayout();
            labelsPanel.setBorder(BorderFactory.createTitledBorder("Trigger"));
            labelsPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            labelsPanelLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            labelsPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1 };
            labelsPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7 };
            labelsPanel.setLayout(labelsPanelLayout);
            labelsPanel.add(getTriggerNameLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getTriggerNameTextField(),
                new GridBagConstraints(1, 0, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getPipelineLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getPipelineDefNameTextField(),
                new GridBagConstraints(1, 1, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getValidLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getValidCheckBox(), new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getValidateButton(), new GridBagConstraints(4, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getReportButton(), new GridBagConstraints(3, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            labelsPanel.add(getExportButton(), new GridBagConstraints(2, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return labelsPanel;
    }

    private ParameterSetMapEditorPanel getParameterSetMapEditorPanel() {
        if (parameterSetMapEditorPanel == null) {
            parameterSetMapEditorPanel = new ParameterSetMapEditorPanel(
                trigger.getPipelineParameterSetNames(), new HashSet<ClassWrapper<Parameters>>(),
                new HashMap<ClassWrapper<Parameters>, ParameterSetName>());
            parameterSetMapEditorPanel.setMapListener(source -> trigger
                .setPipelineParameterSetNames(parameterSetMapEditorPanel.getParameterSetsMap()));
            parameterSetMapEditorPanel
                .setBorder(BorderFactory.createTitledBorder("Pipeline Parameter Sets"));
        }
        return parameterSetMapEditorPanel;
    }

    private JPanel getModulesPanel() {
        if (modulesPanel == null) {
            modulesPanel = new JPanel();
            BorderLayout ModulesPanelLayout = new BorderLayout();
            modulesPanel.setLayout(ModulesPanelLayout);
            modulesPanel.setBorder(BorderFactory.createTitledBorder("Modules"));
            modulesPanel.add(getModulesScrollPane(), BorderLayout.CENTER);
            modulesPanel.add(getModulesButtonPanel(), BorderLayout.SOUTH);
        }
        return modulesPanel;
    }

    private JScrollPane getModulesScrollPane() {
        if (modulesScrollPane == null) {
            modulesScrollPane = new JScrollPane();
            modulesScrollPane.setViewportView(getModulesList());
        }
        return modulesScrollPane;
    }

    private JList<String> getModulesList() {
        if (modulesList == null) {
            triggerModulesListModel = new TriggerModulesListModel(trigger);

            modulesList = new JList<>();
            modulesList.setModel(triggerModulesListModel);
        }
        return modulesList;
    }

    private JPanel getModulesButtonPanel() {
        if (modulesButtonPanel == null) {
            modulesButtonPanel = new JPanel();
            FlowLayout modulesButtonPanelLayout = new FlowLayout();
            modulesButtonPanel.setLayout(modulesButtonPanelLayout);
            modulesButtonPanel.add(getInfoButton());
            modulesButtonPanel.add(getEditModulesButton());
            modulesButtonPanel.add(getSyncButton());
            modulesButtonPanel.add(getRemoteButton());
        }
        return modulesButtonPanel;
    }

    private JButton getEditModulesButton() {
        if (editModulesButton == null) {
            editModulesButton = new JButton();
            editModulesButton.setText("Edit Parameters");
            editModulesButton.addActionListener(this::editModulesButtonActionPerformed);
        }
        return editModulesButton;
    }

    private JButton getInfoButton() {
        if (infoButton == null) {
            infoButton = new JButton();
            infoButton.setText("Task Information");
            infoButton.addActionListener(this::infoButtonActionPerformed);
        }
        return infoButton;
    }

    private void infoButtonActionPerformed(ActionEvent evt) {
        log.debug("infoButton.actionPerformed, event =" + evt);
        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            final PipelineDefinitionNode triggerNode = triggerModulesListModel
                .getTriggerNodeAt(selectedRow);
            TaskInformationDialog infoTable = new TaskInformationDialog(this, triggerNode);
            infoTable.setVisible(true);
        }
    }

    private JButton getRemoteButton() {
        if (remoteButton == null) {
            remoteButton = new JButton("Remote Execution");
            remoteButton.addActionListener(this::remoteButtonActionPerformed);
        }
        return remoteButton;
    }

    private void remoteButtonActionPerformed(ActionEvent evt) {
        log.debug("infoButton.actionPerformed, event =" + evt);
        int selectedRow = modulesList.getSelectedIndex();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            final PipelineDefinitionNode triggerNode = triggerModulesListModel
                .getTriggerNodeAt(selectedRow);
            ParameterSetName remoteParameters = PipelineTaskInformation
                .remoteParameters(triggerNode);
            if (remoteParameters == null) {
                JOptionPane.showMessageDialog(this,
                    "Selected node has no RemoteParameters instance");
                return;
            }
            List<SubtaskInformation> subtaskInfo = PipelineTaskInformation
                .subtaskInformation(triggerNode);
            int subtasks = 0;
            int maxParallelSubtasks = 0;
            for (SubtaskInformation info : subtaskInfo) {
                subtasks += info.getSubtaskCount();
                maxParallelSubtasks += info.getMaxParallelSubtasks();
            }
            new RemoteExecutionDialog(this,
                triggerNode.getPipelineName() + ":" + triggerNode.getModuleName().getName(),
                remoteParameters, triggerNode, subtasks, maxParallelSubtasks);
        }
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(35);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getCloseButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getCloseButton() {
        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("Save");
            saveButton.addActionListener(this::saveButtonActionPerformed);
        }
        return saveButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }

    private JLabel getTriggerNameLabel() {
        if (triggerNameLabel == null) {
            triggerNameLabel = new JLabel();
            triggerNameLabel.setText("Name ");
        }
        return triggerNameLabel;
    }

    private JTextField getTriggerNameTextField() {
        if (triggerNameTextField == null) {
            triggerNameTextField = new JTextField();
        }
        return triggerNameTextField;
    }

    private JLabel getPipelineLabel() {
        if (pipelineLabel == null) {
            pipelineLabel = new JLabel();
            pipelineLabel.setText("Pipeline ");
        }
        return pipelineLabel;
    }

    private JTextField getPipelineDefNameTextField() {
        if (pipelineDefNameTextField == null) {
            pipelineDefNameTextField = new JTextField();
            pipelineDefNameTextField.setEditable(false);
        }
        return pipelineDefNameTextField;
    }

    private JLabel getValidLabel() {
        if (validLabel == null) {
            validLabel = new JLabel();
            validLabel.setText("Valid? ");
        }
        return validLabel;
    }

    private JCheckBox getValidCheckBox() {
        if (validCheckBox == null) {
            validCheckBox = new JCheckBox();
            validCheckBox.setEnabled(false);
        }
        return validCheckBox;
    }

    private JPanel getPrioriytPanel() {
        if (prioriytPanel == null) {
            prioriytPanel = new JPanel();
            GridBagLayout prioriytPanelLayout = new GridBagLayout();
            prioriytPanelLayout.rowWeights = new double[] { 0.1 };
            prioriytPanelLayout.rowHeights = new int[] { 7 };
            prioriytPanelLayout.columnWeights = new double[] { 0.1 };
            prioriytPanelLayout.columnWidths = new int[] { 7 };
            prioriytPanel.setLayout(prioriytPanelLayout);
            prioriytPanel.setBorder(
                BorderFactory.createTitledBorder("Priority (smaller numbers = higher priority)"));
            prioriytPanel.add(getPrioritySpinner(), new GridBagConstraints(0, -1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return prioriytPanel;
    }

    private JSpinner getPrioritySpinner() {
        if (prioritySpinner == null) {
            SpinnerListModel prioritySpinnerModel = new SpinnerListModel(
                new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" });
            prioritySpinner = new JSpinner();
            prioritySpinner.setModel(prioritySpinnerModel);
            prioritySpinner.setPreferredSize(new java.awt.Dimension(50, 22));
            prioritySpinnerModel.setValue(trigger.getInstancePriority() + "");
        }
        return prioritySpinner;
    }

    private JButton getValidateButton() {
        if (validateButton == null) {
            validateButton = new JButton();
            validateButton.setText("validate");
            validateButton.addActionListener(this::validateButtonActionPerformed);
        }
        return validateButton;
    }

    private JButton getSyncButton() {
        if (syncButton == null) {
            syncButton = new JButton();
            syncButton.setText("Re-sync");
            syncButton.setToolTipText(
                "Re-sync module list with latest version of the pipeline definition");
            syncButton.addActionListener(this::syncButtonActionPerformed);
        }
        return syncButton;
    }

    private JButton getReportButton() {
        if (reportButton == null) {
            reportButton = new JButton();
            reportButton.setText("report");
            reportButton.addActionListener(this::reportButtonActionPerformed);
        }
        return reportButton;
    }

    private JButton getExportButton() {
        if (exportButton == null) {
            exportButton = new JButton();
            exportButton.setText("export params");
            exportButton.addActionListener(this::exportButtonActionPerformed);
        }
        return exportButton;
    }
}
