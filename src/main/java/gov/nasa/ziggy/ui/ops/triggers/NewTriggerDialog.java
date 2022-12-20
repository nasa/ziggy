package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.config.pipeline.PipelineDefinitionListModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class NewTriggerDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(NewTriggerDialog.class);

    private JPanel dataPanel;
    private JButton createButton;
    private JPanel namePanel;
    private JComboBox<PipelineDefinition> pipelineComboBox;
    private JTextField nameTextField;
    private JPanel pipelinePanel;
    private JButton cancelButton;
    private JPanel actionPanel;

    private boolean cancelled = false;

    PipelineDefinition pipelineDefinition;
    private PipelineDefinitionListModel pipelineComboBoxModel;

    public NewTriggerDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public String getTriggerName() {
        return nameTextField.getText();
    }

    public PipelineDefinition getPipelineDefinition() {
        return (PipelineDefinition) pipelineComboBox.getSelectedItem();
    }

    private void initGUI() {
        try {
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            setSize(300, 200);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createButtonActionPerformed(ActionEvent evt) {
        log.debug("createButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;

        setVisible(false);
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getNamePanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getPipelinePanel(), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(35);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getCreateButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getCreateButton() {
        if (createButton == null) {
            createButton = new JButton();
            createButton.setText("create");
            createButton.addActionListener(this::createButtonActionPerformed);
        }
        return createButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }

    private JPanel getNamePanel() {
        if (namePanel == null) {
            namePanel = new JPanel();
            GridBagLayout namePanelLayout = new GridBagLayout();
            namePanelLayout.columnWidths = new int[] { 7 };
            namePanelLayout.rowHeights = new int[] { 7 };
            namePanelLayout.columnWeights = new double[] { 0.1 };
            namePanelLayout.rowWeights = new double[] { 0.1 };
            namePanel.setLayout(namePanelLayout);
            namePanel.setBorder(BorderFactory.createTitledBorder("Trigger Name"));
            namePanel.add(getNameTextField(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }
        return namePanel;
    }

    private JPanel getPipelinePanel() {
        if (pipelinePanel == null) {
            pipelinePanel = new JPanel();
            GridBagLayout pipelinePanelLayout = new GridBagLayout();
            pipelinePanelLayout.columnWidths = new int[] { 7 };
            pipelinePanelLayout.rowHeights = new int[] { 7 };
            pipelinePanelLayout.columnWeights = new double[] { 0.1 };
            pipelinePanelLayout.rowWeights = new double[] { 0.1 };
            pipelinePanel.setLayout(pipelinePanelLayout);
            pipelinePanel.setBorder(BorderFactory.createTitledBorder("Pipeline Definition"));
            pipelinePanel.add(getPipelineComboBox(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }
        return pipelinePanel;
    }

    private JTextField getNameTextField() {
        if (nameTextField == null) {
            nameTextField = new JTextField();
        }
        return nameTextField;
    }

    private JComboBox<PipelineDefinition> getPipelineComboBox() {
        if (pipelineComboBox == null) {
            pipelineComboBoxModel = new PipelineDefinitionListModel();
            pipelineComboBox = new JComboBox<>();
            pipelineComboBox.setModel(pipelineComboBoxModel);
        }
        return pipelineComboBox;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            NewTriggerDialog inst = new NewTriggerDialog(frame);
            inst.setVisible(true);
        });
    }

    /**
     * @return the cancelled
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * @param cancelled the cancelled to set
     */
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
