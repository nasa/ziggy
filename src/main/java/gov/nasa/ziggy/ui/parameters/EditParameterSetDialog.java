package gov.nasa.ziggy.ui.parameters;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.RESTORE_DEFAULTS;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.PropertySheetHelper;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class EditParameterSetDialog extends javax.swing.JDialog {
    private final ParameterSet parameterSet;
    private Set<Parameter> currentParams;
    private PropertySheetPanel paramsPropPanel;
    private JTextArea descriptionTextArea;

    private boolean cancelled;
    private boolean isNew;

    private final ParametersOperations parametersOperations = new ParametersOperations();

    public EditParameterSetDialog(Window owner, ParameterSet parameterSet, boolean isNew) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.parameterSet = parameterSet;
        this.isNew = isNew;

        currentParams = parameterSet.copyOfParameters();

        buildComponent();
        setLocationRelativeTo(owner);
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
            ZiggySwingUtils.createButton(RESTORE_DEFAULTS, this::defaults));

        JLabel parameters = boldLabel("Parameters");
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

    private void save(ActionEvent evt) {
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                paramsPropPanel.writeToObject(currentParams);
                parametersOperations().updateParameterSet(parameterSet, currentParams,
                    descriptionTextArea.getText(), isNew);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    setVisible(false);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(getRootPane(), e);
                }
            }
        }.execute();
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

    private ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    public static void main(String[] args) {
        ParameterSet parameters = new ParameterSet("test");
        parameters.setParameters(new HashSet<>());
        ZiggySwingUtils.displayTestDialog(new EditParameterSetDialog(null, parameters, false));
    }
}
