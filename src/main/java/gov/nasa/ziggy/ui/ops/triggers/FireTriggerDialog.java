package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.ui.common.IntTextField;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class FireTriggerDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(FireTriggerDialog.class);

    private JPanel dataPanel;
    private JPanel namePanel;
    private JCheckBox overrideEndCheckBox;
    private JComboBox<String> endNodeComboBox;
    private JCheckBox overrideStartCheckBox;
    private JComboBox<String> startNodeComboBox;
    private JLabel endNodeLabel;
    private JLabel startNodeLabel;
    private JPanel startEndPanel;
    private JTextField instanceNameTextField;
    private JButton cancelButton;
    private JButton fireButton;
    private JPanel actionPanel;

    private JPanel repetitionPanel;
    private JComboBox<String> delayUnitsComboBox;
    private JLabel delayUnitsLabel;
    private JTextField delayTextField;
    private JLabel delayTextLabel;
    private JTextField repetitionsTextField;
    private JLabel repetitionsTextLabel;
    private JCheckBox unlimitedRepsCheckBox;

    private TriggerModulesListModel startNodeComboBoxModel;
    private TriggerModulesListModel endNodeComboBoxModel;

    private PipelineDefinition trigger;

    private enum DelayUnits {
        minutes, hours, days;
    }

    /** for Jigloo use only **/
    public FireTriggerDialog(JFrame frame) {
        super(frame, true);

        initGUI();
    }

    public FireTriggerDialog(JFrame frame, PipelineDefinition trigger) {
        super(frame, true);
        this.trigger = trigger;

        initGUI();
    }

    private void initGUI() {
        try {
            {
                setTitle("Launch Pipeline");
                getContentPane().add(getDataPanel(), BorderLayout.CENTER);
                getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            }
            setSize(350, 400);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void fireButtonActionPerformed(ActionEvent evt) {
        log.debug("fireButton.actionPerformed, event=" + evt);

        try {
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

            PipelineDefinitionNode startNode = null;
            PipelineDefinitionNode endNode = null;

            if (overrideStartCheckBox.isSelected()) {
                startNode = startNodeComboBoxModel.getSelectedPipelineNode();
            }

            if (overrideEndCheckBox.isSelected()) {
                endNode = endNodeComboBoxModel.getSelectedPipelineNode();
            }

            RepetitionsAndDelay repsAndDelay = getRepetitionAndDelay();
            int repetitions = repsAndDelay.getRepetitions();
            int delayMinutes = repsAndDelay.getDelayMinutes();
            if (repetitions == 1) {
                log.info("Trigger fired with 1 repetition");
            } else if (repetitions < 0) {
                log.info("Trigger fired with unlimited repetitions at " + delayMinutes
                    + " minutes interval");
            } else {
                log.info("Trigger fired with " + repetitions + " repetitions at " + delayMinutes
                    + " minutes interval");
            }

            if (startNode == null) {
                log.info("Trigger start at pipeline start nodes");
            } else {
                log.info("Trigger start at node: " + startNode.getModuleName().getName());
            }

            if (endNode == null) {
                log.info("Trigger end at pipeline end nodes");
            } else {
                log.info("Trigger end at node: " + endNode.getModuleName().getName());
            }

            PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
            pipelineOps.sendTriggerMessage(new WorkerFireTriggerRequest(trigger.getName().getName(),
                instanceNameTextField.getText(), startNode, endNode, repetitions,
                delayMinutes * 60));

            setCursor(null);
        } catch (Exception e) {
            log.error("fireButtonActionPerformed(ActionEvent)", e);

            MessageUtil.showError(this, e);
        }

        setVisible(false);
    }

    private void overrideStartCheckBoxActionPerformed(ActionEvent evt) {
        log.debug("overrideStartCheckBox.actionPerformed, event=" + evt);

        startNodeLabel.setEnabled(overrideStartCheckBox.isSelected());
        startNodeComboBox.setEnabled(overrideStartCheckBox.isSelected());
    }

    private void overrideEndCheckBoxActionPerformed(ActionEvent evt) {
        log.debug("overrideEndCheckBox.actionPerformed, event=" + evt);

        endNodeLabel.setEnabled(overrideEndCheckBox.isSelected());
        endNodeComboBox.setEnabled(overrideEndCheckBox.isSelected());
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getNamePanel(), new GridBagConstraints(0, 0, 6, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getRepetitionPanel(), new GridBagConstraints(0, 1, 6, 2, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getStartEndPanel(), new GridBagConstraints(0, 3, 6, 2, 0.0, 0.0,
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
            actionPanel.add(getFireButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getFireButton() {
        if (fireButton == null) {
            fireButton = new JButton();
            fireButton.setText("Fire!");
            fireButton.setFont(new java.awt.Font("Dialog", 1, 16));
            fireButton.setForeground(new java.awt.Color(255, 0, 0));
            fireButton.addActionListener(this::fireButtonActionPerformed);
        }
        return fireButton;
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
            namePanel.setBorder(BorderFactory.createTitledBorder("Pipeline Instance Name"));
            namePanel.add(getInstanceNameTextField(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }
        return namePanel;
    }

    private JPanel getRepetitionPanel() {
        if (repetitionPanel == null) {
            repetitionPanel = new JPanel();
            GridBagLayout repetitionPanelLayout = new GridBagLayout();
            repetitionPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7 };
            repetitionPanelLayout.rowHeights = new int[] { 5, 5 };
            repetitionPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1 };
            repetitionPanelLayout.rowWeights = new double[] { 0.1, 0.1 };
            repetitionPanel.setLayout(repetitionPanelLayout);
            repetitionPanel
                .setBorder(BorderFactory.createTitledBorder("Pipeline Instance Repetitions"));

            repetitionPanel.add(getUnlimitedRepsCheckBox(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

            repetitionPanel.add(getRepetitionsTextLabel(),
                new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.SOUTH,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            repetitionPanel.add(getRepetitionsTextField(),
                new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            repetitionPanel.add(getDelayTextLabel(), new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.SOUTH, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            repetitionPanel.add(getDelayTextField(),
                new GridBagConstraints(3, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            repetitionPanel.add(getDelayUnitsLabel(), new GridBagConstraints(4, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.SOUTH, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            repetitionPanel.add(getDelayUnitsComboBox(),
                new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return repetitionPanel;
    }

    private JCheckBox getUnlimitedRepsCheckBox() {
        if (unlimitedRepsCheckBox == null) {
            unlimitedRepsCheckBox = new JCheckBox();
            unlimitedRepsCheckBox.setText("Unlimited");
            unlimitedRepsCheckBox.addActionListener(this::unlimitedRepsCheckBoxActionPerformed);
        }
        return unlimitedRepsCheckBox;
    }

    private void unlimitedRepsCheckBoxActionPerformed(ActionEvent evt) {

        if (unlimitedRepsCheckBox.isSelected()) {
            repetitionsTextLabel.setEnabled(false);
            repetitionsTextField.setEnabled(false);
            repetitionsTextField.setText("");
        } else {
            repetitionsTextLabel.setEnabled(true);
            repetitionsTextField.setEnabled(true);
            repetitionsTextField.setText("1");
            delayTextField.setText("");
        }

    }

    private JTextField getRepetitionsTextField() {
        if (repetitionsTextField == null) {
            repetitionsTextField = new IntTextField("1");
        }
        return repetitionsTextField;
    }

    private JLabel getRepetitionsTextLabel() {
        if (repetitionsTextLabel == null) {
            repetitionsTextLabel = new JLabel();
            repetitionsTextLabel.setText("Repetitions");
        }
        return repetitionsTextLabel;
    }

    private JTextField getDelayTextField() {
        if (delayTextField == null) {
            delayTextField = new IntTextField();
        }
        return delayTextField;
    }

    private JLabel getDelayTextLabel() {
        if (delayTextLabel == null) {
            delayTextLabel = new JLabel();
            delayTextLabel.setText("Delay");
        }
        return delayTextLabel;
    }

    private JComboBox<String> getDelayUnitsComboBox() {
        if (delayUnitsComboBox == null) {
            String[] delayUnitsStrings = new String[DelayUnits.values().length];
            for (int i = 0; i < delayUnitsStrings.length; i++) {
                delayUnitsStrings[i] = DelayUnits.values()[i].toString();
            }
            delayUnitsComboBox = new JComboBox<>(delayUnitsStrings);
            delayUnitsComboBox.setSelectedIndex(0);
        }
        return delayUnitsComboBox;
    }

    private JLabel getDelayUnitsLabel() {
        if (delayUnitsLabel == null) {
            delayUnitsLabel = new JLabel();
            delayUnitsLabel.setText("Units");
        }
        return delayUnitsLabel;
    }

    private RepetitionsAndDelay getRepetitionAndDelay() {

        int repetitions;
        int delayMinutes;

        if (getUnlimitedRepsCheckBox().isSelected()) {
            repetitions = -1;
        } else {
            String repsText = getRepetitionsTextField().getText();
            if (repsText.isEmpty()) {
                throw new IllegalStateException("Number of repetitions is empty");
            }
            repetitions = Integer.parseInt(repsText);
            if (repetitions == 0) {
                throw new IllegalStateException("Number of repetitions is zero");
            }
        }

        if (repetitions != 1) {
            String delayText = getDelayTextField().getText();
            if (delayText.isEmpty()) {
                throw new IllegalStateException("Repetitions delay is empty");
            }
            int delay = Integer.parseInt(delayText);
            if (delay == 0) {
                throw new IllegalStateException("Delay is set to zero");
            }
            String delayUnitsString = (String) getDelayUnitsComboBox().getSelectedItem();
            DelayUnits delayUnit = DelayUnits.valueOf(delayUnitsString);
            switch (delayUnit) {
                case minutes:
                    delayMinutes = delay;
                    break;
                case hours:
                    delayMinutes = 60 * delay;
                    break;
                case days:
                    delayMinutes = 60 * 24 * delay;
                    break;
                default:
                    throw new IllegalStateException(
                        "Delay unit " + delayUnitsString + " not supported");
            }
        } else {
            delayMinutes = 0;
        }

        return new RepetitionsAndDelay(repetitions, delayMinutes);
    }

    private JTextField getInstanceNameTextField() {
        if (instanceNameTextField == null) {
            instanceNameTextField = new JTextField();
        }
        return instanceNameTextField;
    }

    private JPanel getStartEndPanel() {
        if (startEndPanel == null) {
            startEndPanel = new JPanel();
            GridBagLayout startEndPanelLayout = new GridBagLayout();
            startEndPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7 };
            startEndPanelLayout.rowHeights = new int[] { 7, 7, 7 };
            startEndPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            startEndPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1 };
            startEndPanel.setLayout(startEndPanelLayout);
            startEndPanel.setBorder(BorderFactory.createTitledBorder("Start & End Node Override"));
            startEndPanel.add(getOverrideStartCheckBox(),
                new GridBagConstraints(0, 0, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            startEndPanel.add(getStartNodeLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            startEndPanel.add(getEndNodeLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            startEndPanel.add(getStartNodeComboBox(),
                new GridBagConstraints(1, 1, 5, 1, 1.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            startEndPanel.add(getEndNodeComboBox(),
                new GridBagConstraints(1, 3, 5, 1, 1.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            startEndPanel.add(getOverrideEndCheckBox(),
                new GridBagConstraints(0, 2, 6, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return startEndPanel;
    }

    private JCheckBox getOverrideStartCheckBox() {
        if (overrideStartCheckBox == null) {
            overrideStartCheckBox = new JCheckBox();
            overrideStartCheckBox.setText("Override Start");
            overrideStartCheckBox.addActionListener(this::overrideStartCheckBoxActionPerformed);
        }
        return overrideStartCheckBox;
    }

    private JLabel getStartNodeLabel() {
        if (startNodeLabel == null) {
            startNodeLabel = new JLabel();
            startNodeLabel.setText("Start Node:");
            startNodeLabel.setEnabled(false);
        }
        return startNodeLabel;
    }

    private JLabel getEndNodeLabel() {
        if (endNodeLabel == null) {
            endNodeLabel = new JLabel();
            endNodeLabel.setText("End Node:");
            endNodeLabel.setEnabled(false);
        }
        return endNodeLabel;
    }

    private JComboBox<String> getStartNodeComboBox() {
        if (startNodeComboBox == null) {
            startNodeComboBoxModel = new TriggerModulesListModel(trigger);
            startNodeComboBox = new JComboBox<>();
            startNodeComboBox.setModel(startNodeComboBoxModel);
            startNodeComboBox.setEnabled(false);
        }
        return startNodeComboBox;
    }

    private JComboBox<String> getEndNodeComboBox() {
        if (endNodeComboBox == null) {
            endNodeComboBoxModel = new TriggerModulesListModel(trigger);
            endNodeComboBox = new JComboBox<>();
            endNodeComboBox.setModel(endNodeComboBoxModel);
            endNodeComboBox.setEnabled(false);
        }
        return endNodeComboBox;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            PipelineDefinition dummyPipeline = new PipelineDefinition("dummy");
            PipelineModuleDefinition dummyModule = new PipelineModuleDefinition("dmy");
            PipelineDefinitionNode dummyNode = new PipelineDefinitionNode(dummyModule.getName(),
                dummyPipeline.getName().getName());
            List<PipelineDefinitionNode> rootNode = new ArrayList<>();
            rootNode.add(dummyNode);
            dummyPipeline.setRootNodes(rootNode);
            FireTriggerDialog inst = new FireTriggerDialog(frame, dummyPipeline);
            inst.setVisible(true);
        });
    }

    private JCheckBox getOverrideEndCheckBox() {
        if (overrideEndCheckBox == null) {
            overrideEndCheckBox = new JCheckBox();
            overrideEndCheckBox.setText("Override Stop");
            overrideEndCheckBox.addActionListener(this::overrideEndCheckBoxActionPerformed);
        }
        return overrideEndCheckBox;
    }

    private static class RepetitionsAndDelay {

        private final int repetitions;
        private final int delayMinutes;

        public RepetitionsAndDelay(int repetitions, int delayMinutes) {
            this.repetitions = repetitions;
            this.delayMinutes = delayMinutes;
        }

        public int getRepetitions() {
            return repetitions;
        }

        public int getDelayMinutes() {
            return delayMinutes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(delayMinutes, repetitions);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            RepetitionsAndDelay other = (RepetitionsAndDelay) obj;
            return delayMinutes == other.delayMinutes && repetitions == other.repetitions;
        }
    }

}
