package gov.nasa.ziggy.ui.config.parameters;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetNewDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetNewDialog.class);

    private JPanel dataPanel;
    private JPanel namePanel;
    private JPanel typePanel;
    private JPanel descPanel;
    private ParameterClassSelectorPanel parameterClassSelectorPanel;
    private JScrollPane descriptionScrollPane;
    private JTextArea descriptionTextArea;
    private JTextField nameTextField;
    private JButton cancelButton;
    private JButton okButton;
    private JPanel actionPanel;

    private boolean cancelled = false;

    private ParameterSet newParamSet = null;

    public ParameterSetNewDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public static ParameterSet createParameterSet() {
        ParameterSetNewDialog dialog = ZiggyGuiConsole.newParameterSetNewDialog();
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.newParamSet;
        }
        return null;
    }

    private void okButtonActionPerformed(ActionEvent evt) {
        log.debug("okButton.actionPerformed, event=" + evt);

        String paramSetName = getNameTextField().getText();

        if (paramSetName.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                "Please enter a unique name for the new Parameter Set", "Error",
                JOptionPane.ERROR_MESSAGE);
            return;
        }

        String paramSetDesc = getDescriptionTextArea().getText();

        ClassWrapper<Parameters> paramSetClassWrapper = getParameterClassSelectorPanel()
            .getSelectedElement();

        if (paramSetClassWrapper == null) {
            JOptionPane.showMessageDialog(this, "Please select a class for the new Parameter Set",
                "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        Class<? extends Parameters> paramSetClass = paramSetClassWrapper.getClazz();

        newParamSet = new ParameterSet(paramSetName);
        newParamSet.setDescription(paramSetDesc);
        newParamSet.setParameters(new BeanWrapper<Parameters>(paramSetClass));

        setVisible(false);
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;

        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("New Parameter Set");
            }
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            setSize(400, 500);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getNamePanel(), new GridBagConstraints(0, 0, 6, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getDescPanel(), new GridBagConstraints(0, 1, 6, 2, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getTypePanel(), new GridBagConstraints(0, 4, 6, 4, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(30);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getOkButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getOkButton() {
        if (okButton == null) {
            okButton = new JButton();
            okButton.setText("ok");
            okButton.addActionListener(this::okButtonActionPerformed);
        }
        return okButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }

    private JTextField getNameTextField() {
        if (nameTextField == null) {
            nameTextField = new JTextField();
        }
        return nameTextField;
    }

    private JTextArea getDescriptionTextArea() {
        if (descriptionTextArea == null) {
            descriptionTextArea = new JTextArea();
        }
        return descriptionTextArea;
    }

    private JScrollPane getDescriptionScrollPane() {
        if (descriptionScrollPane == null) {
            descriptionScrollPane = new JScrollPane();
            descriptionScrollPane.setViewportView(getDescriptionTextArea());
        }
        return descriptionScrollPane;
    }

    private ParameterClassSelectorPanel getParameterClassSelectorPanel() {
        if (parameterClassSelectorPanel == null) {
            parameterClassSelectorPanel = new ParameterClassSelectorPanel();
        }
        return parameterClassSelectorPanel;
    }

    private JPanel getNamePanel() {
        if (namePanel == null) {
            namePanel = new JPanel();
            GridBagLayout namePanelLayout = new GridBagLayout();
            namePanelLayout.rowWeights = new double[] { 0.1 };
            namePanelLayout.rowHeights = new int[] { 7 };
            namePanelLayout.columnWeights = new double[] { 0.1 };
            namePanelLayout.columnWidths = new int[] { 7 };
            namePanel.setLayout(namePanelLayout);
            namePanel.setBorder(BorderFactory.createTitledBorder("Name"));
            namePanel.add(getNameTextField(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }
        return namePanel;
    }

    private JPanel getDescPanel() {
        if (descPanel == null) {
            descPanel = new JPanel();
            GridBagLayout descPanelLayout = new GridBagLayout();
            descPanelLayout.rowWeights = new double[] { 0.1 };
            descPanelLayout.rowHeights = new int[] { 7 };
            descPanelLayout.columnWeights = new double[] { 0.1 };
            descPanelLayout.columnWidths = new int[] { 7 };
            descPanel.setLayout(descPanelLayout);
            descPanel.setBorder(BorderFactory.createTitledBorder("Description"));
            descPanel.add(getDescriptionScrollPane(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return descPanel;
    }

    private JPanel getTypePanel() {
        if (typePanel == null) {
            typePanel = new JPanel();
            BorderLayout typePanelLayout = new BorderLayout();
            typePanel.setLayout(typePanelLayout);
            typePanel.setBorder(BorderFactory.createTitledBorder("Type"));
            typePanel.add(getParameterClassSelectorPanel(), BorderLayout.CENTER);
        }
        return typePanel;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            ParameterSetNewDialog inst = new ParameterSetNewDialog(frame);
            inst.setVisible(true);
        });
    }
}
