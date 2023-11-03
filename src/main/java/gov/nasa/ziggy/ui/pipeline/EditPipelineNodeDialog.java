package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.proxy.PipelineModuleDefinitionCrudProxy;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Allows editing a new or existing {@link PipelineDefinitionNode}.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditPipelineNodeDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(EditPipelineNodeDialog.class);
    private JLabel moduleLabel;

    private JPanel dataPanel;
    private JTextArea errorTextArea;
    private JLabel uowTypeLabel;
    private JCheckBox startNewUowCheckBox;
    private JPanel uowPanel;
    private JPanel buttonPanel;
    private JComboBox<PipelineModuleDefinition> moduleComboBox;
    private JButton cancelButton;
    private JButton saveButton;
    private boolean savePressed = false;

    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud;

    private JLabel uowFullNameLabel;

    private final PipelineDefinition pipeline;
    private final PipelineDefinitionNode pipelineNode;

    public EditPipelineNodeDialog(Window owner, PipelineDefinition pipeline,
        PipelineDefinitionNode pipelineNode) throws Exception {

        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipeline = pipeline;
        this.pipelineNode = pipelineNode;
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void save(ActionEvent evt) {

        try {
            boolean startNewUowSelected = startNewUowCheckBox.isSelected();

            PipelineModuleDefinition selectedModule = (PipelineModuleDefinition) moduleComboBox
                .getSelectedItem();
            pipelineNode.setPipelineModuleDefinition(selectedModule);
            pipelineNode.setStartNewUow(startNewUowSelected);

            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        savePressed = true;
    }

    private void cancel(ActionEvent evt) {
        setVisible(false);
    }

    /**
     * Make sure that the selected UOW task generator generates the {@link UnitOfWorkGenerator} type
     * that the selected {@link PipelineModule} expects.
     * <p>
     * If the startNewUowCheckBox is not checked, we actually want to validate against the previous
     * node (TBD)
     */
    private void validateUnitOfWork() {
        try {

            if (!startNewUowCheckBox.isSelected()) {
                PipelineDefinitionNode parentNode = pipelineNode.getParentNode();

                while (parentNode != null && !parentNode.isStartNewUow()) {
                    parentNode = parentNode.getParentNode();
                }

                if (parentNode != null) {
                    ClassWrapper<UnitOfWorkGenerator> nodeTg = pipelineNode
                        .getUnitOfWorkGenerator();
                    ClassWrapper<UnitOfWorkGenerator> parentTg = parentNode
                        .getUnitOfWorkGenerator();
                    if (parentTg != null && nodeTg != null) {

                        if (!nodeTg.toString().equals(parentTg.toString())) {
                            setError(pipelineNode.getModuleName() + " expects " + nodeTg.toString()
                                + ", but the previous node (" + parentNode.getModuleName()
                                + ") uses " + parentTg.toString()
                                + ". Select 'Start new unit of work' or select a different module");
                        } else {
                            setError("");
                        }
                    }
                } else {
                    setError("Unit of Work type must be set for the first node in a pipeline");
                }
            }
        } catch (PipelineException e) {
            log.warn("Failed to validate UOW type", e);
            MessageUtil.showError(this, e);
        }
    }

    /**
     * @param evt
     */
    private void startNewUow(ActionEvent evt) {
        boolean selected = startNewUowCheckBox.isSelected();
        // grey out the uow tg settings if the checkbox is not checked
        uowTypeLabel.setEnabled(selected);
        uowFullNameLabel.setEnabled(selected);

        validateUnitOfWork();
    }

    private void buildComponent() throws Exception {

        setTitle("Edit pipeline node");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);

        validateUnitOfWork();
        pack();
    }

    private JComboBox<PipelineModuleDefinition> getModuleComboBox() {

        if (moduleComboBox == null) {
            String moduleName = pipelineNode.getModuleName();
            String currentModuleName = null;

            if (moduleName != null) {
                currentModuleName = moduleName;
            }

            int currentIndex = 0;
            int initialIndex = 0;
            DefaultComboBoxModel<PipelineModuleDefinition> moduleComboBoxModel = new DefaultComboBoxModel<>();
            List<PipelineModuleDefinition> modules = pipelineModuleDefinitionCrud.retrieveAll();
            for (PipelineModuleDefinition module : modules) {
                moduleComboBoxModel.addElement(module);
                if (currentModuleName != null && module.getName().equals(currentModuleName)) {
                    initialIndex = currentIndex;
                }
                currentIndex++;
            }
            moduleComboBox = new JComboBox<>();
            moduleComboBox.setModel(moduleComboBoxModel);
            moduleComboBox.setMaximumRowCount(15);
            moduleComboBox.setSelectedIndex(initialIndex);

            moduleComboBox.setEnabled(!pipeline.isLocked());
        }

        return moduleComboBox;
    }

    private JPanel createDataPanel() {

        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getModuleComboBox(),
                new GridBagConstraints(2, 0, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getModuleLabel(),
                new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getUowPanel(), new GridBagConstraints(1, 2, 4, 3, 0.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getErrorTextArea(), new GridBagConstraints(1, 1, 4, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }

        return dataPanel;
    }

    private JPanel getButtonPanel() {

        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }

        return buttonPanel;
    }

    private JButton getSaveButton() {

        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText(SAVE);
            saveButton.addActionListener(this::save);
            saveButton.setEnabled(!pipeline.isLocked());
        }

        return saveButton;
    }

    private JButton getCancelButton() {

        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText(CANCEL);
            cancelButton.addActionListener(this::cancel);
        }

        return cancelButton;
    }

    private JTextArea getErrorTextArea() {
        if (errorTextArea == null) {
            errorTextArea = new JTextArea();
            errorTextArea.setEditable(false);
            errorTextArea.setForeground(new java.awt.Color(255, 0, 0));
            errorTextArea.setOpaque(false);
            errorTextArea.setLineWrap(true);
            errorTextArea.setWrapStyleWord(true);
        }
        return errorTextArea;
    }

    private JLabel getModuleLabel() {
        if (moduleLabel == null) {
            moduleLabel = new JLabel();
            moduleLabel.setText("Module");
        }
        return moduleLabel;
    }

    /**
     * @return Returns the savePressed.
     */
    public boolean wasSavePressed() {
        return savePressed;
    }

    private JPanel getUowPanel() {
        if (uowPanel == null) {
            uowPanel = new JPanel();
            GridBagLayout uowPanelLayout = new GridBagLayout();
            uowPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            uowPanelLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            uowPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            uowPanelLayout.columnWidths = new int[] { 7, 7, 7, 7 };
            uowPanel.setLayout(uowPanelLayout);
            uowPanel.setBorder(BorderFactory.createTitledBorder("Unit of Work"));
            uowPanel.add(getStartNewUowCheckBox(),
                new GridBagConstraints(0, 0, 4, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            uowPanel.add(getUowTypeLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            uowPanel.add(getUowFullNameLabel(),
                new GridBagConstraints(0, 3, 4, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }
        return uowPanel;
    }

    private JCheckBox getStartNewUowCheckBox() {
        if (startNewUowCheckBox == null) {
            startNewUowCheckBox = new JCheckBox();
            startNewUowCheckBox.setText("Start new unit of work");
            startNewUowCheckBox.setSelected(pipelineNode.isStartNewUow());
            startNewUowCheckBox.addActionListener(this::startNewUow);
            startNewUowCheckBox.setEnabled(!pipeline.isLocked());
        }
        return startNewUowCheckBox;
    }

    private JLabel getUowTypeLabel() {
        if (uowTypeLabel == null) {
            uowTypeLabel = new JLabel();
            ClassWrapper<UnitOfWorkGenerator> uowWrapper = pipelineNode.getUnitOfWorkGenerator();
            String uowName = uowWrapper.getClassName();
            String uowLabel = "Unit of Work Class: " + uowName;
            uowTypeLabel.setText(uowLabel);
        }
        return uowTypeLabel;
    }

    /**
     * If the combination of module/uowtype/checkbox is in an invalid state, display error text and
     * disable the save button until it's fixed
     *
     * @param message
     */
    private void setError(String message) {
        if (saveButton != null) {
            saveButton.setEnabled(message.isEmpty());
        }

        if (errorTextArea != null) {
            errorTextArea.setText(message);
        }
    }

    private JLabel getUowFullNameLabel() {
        if (uowFullNameLabel == null) {
            uowFullNameLabel = new JLabel();
            uowFullNameLabel.setFont(new java.awt.Font("Dialog", 2, 10));
        }
        return uowFullNameLabel;
    }
}
