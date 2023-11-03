package gov.nasa.ziggy.ui.parameters;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.IMPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.RESTORE_DEFAULTS;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.GroupLayout;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.LayoutStyle.ComponentPlacement;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersUtils;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.PropertySheetHelper;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class EditParameterSetDialog extends javax.swing.JDialog {
    private final ParameterSet parameterSet;
    private Parameters currentParams;
    private PropertySheetPanel paramsPropPanel;
    private JTextArea descriptionTextArea;

    private boolean cancelled;
    private boolean isNew;

    public EditParameterSetDialog(Window owner, ParameterSet parameterSet, boolean isNew) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.parameterSet = parameterSet;
        this.isNew = isNew;

        initializeCurrentParamsfromDefinition();

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void initializeCurrentParamsfromDefinition() {
        currentParams = parameterSet.parametersInstance();

        // Make a copy of currentParams; otherwise the property sheet panel will overwrite the
        // original parameters and the updateParameterSet() call in save() won't notice that
        // anything has changed.
        currentParams.setParameters(currentParams.getParametersCopy());
    }

    private void buildComponent() {
        setTitle("Edit parameter set");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(SAVE, this::save), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        populateParamsPropertySheet();
        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Name");
        JLabel nameTextField = new JLabel(parameterSet.getName());

        JLabel version = boldLabel("Version");
        JLabel versionTextField = new JLabel(Integer.toString(parameterSet.getVersion()));

        JLabel description = boldLabel("Description");
        descriptionTextArea = new JTextArea(parameterSet.getDescription());
        JScrollPane descriptionScrollPane = new JScrollPane(descriptionTextArea);

        JPanel buttonPanel = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR,
            ZiggySwingUtils.createButton(RESTORE_DEFAULTS, this::defaults),
            ZiggySwingUtils.createButton(IMPORT, this::importParameters),
            ZiggySwingUtils.createButton(EXPORT, this::exportParameters));

        JLabel parameters = boldLabel("Parameters - " + parameterSet.clazz().getSimpleName());
        paramsPropPanel = new PropertySheetPanel();

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameTextField)
            .addComponent(version)
            .addComponent(versionTextField)
            .addComponent(description)
            .addComponent(descriptionScrollPane)
            .addComponent(parameters)
            .addComponent(buttonPanel)
            .addComponent(paramsPropPanel));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(version)
            .addComponent(versionTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(description)
            .addComponent(descriptionScrollPane)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(parameters)
            .addComponent(buttonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(paramsPropPanel));

        return dataPanel;
    }

    private void defaults(ActionEvent evt) {
        paramsPropPanel.readFromObject(currentParams);
    }

    private void importParameters(ActionEvent evt) {
        try {
            JFileChooser fc = new JFileChooser();
            int returnVal = fc.showOpenDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();
                currentParams = ParametersUtils.importParameters(file, currentParams.getClass());
                paramsPropPanel.readFromObject(currentParams);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void exportParameters(ActionEvent evt) {
        try {
            JFileChooser fc = new JFileChooser();
            int returnVal = fc.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();
                paramsPropPanel.writeToObject(currentParams);
                ParametersUtils.exportParameters(file, currentParams);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void save(ActionEvent evt) {
        try {
            paramsPropPanel.writeToObject(currentParams);
            PipelineOperationsProxy pipelineOperations = new PipelineOperationsProxy();
            pipelineOperations.updateParameterSet(parameterSet, currentParams,
                descriptionTextArea.getText(), isNew);
            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    private void populateParamsPropertySheet() {
        if (currentParams != null) {
            try {
                PropertySheetHelper.populatePropertySheet(currentParams, paramsPropPanel);
            } catch (Exception e) {
                throw new PipelineException("Failed to populate property sheet from parameters", e);
            }
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public static void main(String[] args) {
        ParameterSet parameters = new ParameterSet("test");
        parameters.setTypedParameters(new Parameters().getParameters());
        ZiggySwingUtils.displayTestDialog(new EditParameterSetDialog(null, parameters, false));
    }
}
