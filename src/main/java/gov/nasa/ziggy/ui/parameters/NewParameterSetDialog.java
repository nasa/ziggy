package gov.nasa.ziggy.ui.parameters;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.OK;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.lang.reflect.InvocationTargetException;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class NewParameterSetDialog extends javax.swing.JDialog {

    private ParameterClassSelectorPanel parameterClassSelectorPanel;
    private JTextArea descriptionTextArea;
    private JTextField nameTextField;

    private boolean cancelled;
    private ParameterSet newParamSet;

    public NewParameterSetDialog(Window owner) {
        super(owner, DEFAULT_MODALITY_TYPE);
        buildComponent();
        setLocationRelativeTo(owner);
    }

    public static ParameterSet createParameterSet(Component owner) {
        NewParameterSetDialog dialog = new NewParameterSetDialog(
            SwingUtilities.getWindowAncestor(owner));
        dialog.setVisible(true);

        if (!dialog.cancelled) {
            return dialog.newParamSet;
        }
        return null;
    }

    private void buildComponent() {
        setTitle("New parameter set");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(OK, this::ok), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Name");
        nameTextField = new JTextField();

        JLabel description = boldLabel("Description");
        descriptionTextArea = new JTextArea();
        JScrollPane descriptionScrollPane = new JScrollPane(descriptionTextArea);

        JLabel type = boldLabel("Type");
        parameterClassSelectorPanel = new ParameterClassSelectorPanel();

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameTextField)
            .addComponent(description)
            .addComponent(descriptionScrollPane)
            .addComponent(type)
            .addComponent(parameterClassSelectorPanel));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(description)
            .addComponent(descriptionScrollPane, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, 100)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(type)
            .addComponent(parameterClassSelectorPanel));

        return dataPanel;
    }

    private void ok(ActionEvent evt) {

        String paramSetName = nameTextField.getText();

        if (paramSetName.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                "Please enter a unique name for the new parameter set", "Error",
                JOptionPane.ERROR_MESSAGE);
            return;
        }

        String paramSetDesc = descriptionTextArea.getText();

        ClassWrapper<Parameters> paramSetClassWrapper = parameterClassSelectorPanel
            .getSelectedElement();

        if (paramSetClassWrapper == null) {
            JOptionPane.showMessageDialog(this, "Please select a class for the new parameter set",
                "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        newParamSet = new ParameterSet(paramSetName);
        newParamSet.setDescription(paramSetDesc);
        Class<? extends Parameters> paramSetClass = paramSetClassWrapper.getClazz();
        try {
            newParamSet.setTypedParameters(
                paramSetClass.getDeclaredConstructor().newInstance().getParameters());
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            JOptionPane.showMessageDialog(this,
                "Unable to create instance of " + paramSetClass.getName(), "Error",
                JOptionPane.ERROR_MESSAGE);
            return;
        }

        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new NewParameterSetDialog(null));
    }
}
