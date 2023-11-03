package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.START;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.messages.FireTriggerRequest;
import gov.nasa.ziggy.ui.util.IntTextField;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class StartPipelineDialog extends javax.swing.JDialog {
    private static final long serialVersionUID = 20230810L;

    private static final Logger log = LoggerFactory.getLogger(StartPipelineDialog.class);

    private enum DelayUnits {
        MINUTES, HOURS, DAYS;

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    private PipelineDefinition pipeline;

    private JTextField instanceNameTextField;
    private JCheckBox unlimitedCheckBox;
    private JLabel repetitions;
    private JTextField repetitionsTextField;
    private JTextField delayTextField;
    private JComboBox<DelayUnits> delayUnitsComboBox;
    private JCheckBox overrideStartCheckBox;
    private JComboBox<String> startNodeComboBox;
    private PipelineModulesListModel startNodeComboBoxModel;
    private JCheckBox overrideEndCheckBox;
    private JComboBox<String> endNodeComboBox;
    private PipelineModulesListModel endNodeComboBoxModel;

    public StartPipelineDialog(Window owner, PipelineDefinition pipeline) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipeline = pipeline;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Start pipeline");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(START, this::start), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {

        JLabel instanceNameGroup = boldLabel("Pipeline instance name", LabelType.HEADING1);
        instanceNameTextField = new JTextField();

        JLabel repetitionsGroup = boldLabel("Pipeline instance repetitions", LabelType.HEADING1);

        unlimitedCheckBox = new JCheckBox("Unlimited");
        unlimitedCheckBox.addActionListener(this::unlimitedReps);

        repetitions = new JLabel("Repetitions");
        repetitionsTextField = new IntTextField("1");
        repetitionsTextField.setColumns(6);

        JLabel delay = new JLabel("Delay");
        delayTextField = new IntTextField();
        delayTextField.setColumns(6);

        JLabel delayUnits = new JLabel("Units");
        delayUnitsComboBox = new JComboBox<>(DelayUnits.values());
        delayUnitsComboBox.setSelectedIndex(0);

        JLabel overrideGroup = boldLabel("Start & end node override", LabelType.HEADING1);

        overrideStartCheckBox = new JCheckBox("Override start");
        overrideStartCheckBox.addActionListener(this::overrideStart);

        startNodeComboBoxModel = new PipelineModulesListModel(pipeline);
        startNodeComboBox = new JComboBox<>(startNodeComboBoxModel);
        startNodeComboBox.setEnabled(false);

        overrideEndCheckBox = new JCheckBox("Override stop");
        overrideEndCheckBox.addActionListener(this::overrideEnd);

        endNodeComboBoxModel = new PipelineModulesListModel(pipeline);
        endNodeComboBox = new JComboBox<>(endNodeComboBoxModel);
        endNodeComboBox.setEnabled(false);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(instanceNameGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggySwingUtils.INDENT)
                .addComponent(instanceNameTextField))
            .addComponent(repetitionsGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggySwingUtils.INDENT)
                .addComponent(unlimitedCheckBox)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(repetitions)
                    .addComponent(repetitionsTextField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(delay)
                    .addComponent(delayTextField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(delayUnits)
                    .addComponent(delayUnitsComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
            .addComponent(overrideGroup)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(ZiggySwingUtils.INDENT)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addComponent(overrideStartCheckBox)
                        .addPreferredGap(ComponentPlacement.RELATED)
                        .addComponent(startNodeComboBox, GroupLayout.PREFERRED_SIZE,
                            GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                    .addGroup(dataPanelLayout.createSequentialGroup()
                        .addComponent(overrideEndCheckBox)
                        .addPreferredGap(ComponentPlacement.RELATED)
                        .addComponent(endNodeComboBox, GroupLayout.PREFERRED_SIZE,
                            GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(instanceNameGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(instanceNameTextField, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(repetitionsGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(unlimitedCheckBox, Alignment.TRAILING)
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(repetitions)
                    .addComponent(repetitionsTextField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(delay)
                    .addComponent(delayTextField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addComponent(delayUnits)
                    .addComponent(delayUnitsComboBox, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
            .addGap(ZiggySwingUtils.GROUP_GAP)
            .addComponent(overrideGroup)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(overrideStartCheckBox)
                .addComponent(startNodeComboBox, GroupLayout.PREFERRED_SIZE,
                    GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(overrideEndCheckBox)
                .addComponent(endNodeComboBox, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE)));

        return dataPanel;
    }

    private void unlimitedReps(ActionEvent evt) {

        if (unlimitedCheckBox.isSelected()) {
            repetitions.setEnabled(false);
            repetitionsTextField.setEnabled(false);
            repetitionsTextField.setText("");
        } else {
            repetitions.setEnabled(true);
            repetitionsTextField.setEnabled(true);
            repetitionsTextField.setText("1");
            delayTextField.setText("");
        }
    }

    private void overrideStart(ActionEvent evt) {
        startNodeComboBox.setEnabled(overrideStartCheckBox.isSelected());
    }

    private void overrideEnd(ActionEvent evt) {
        endNodeComboBox.setEnabled(overrideEndCheckBox.isSelected());
    }

    private void start(ActionEvent evt) {

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
                log.info("Pipeline started with 1 repetition");
            } else if (repetitions < 0) {
                log.info("Pipeline started with unlimited repetitions at " + delayMinutes
                    + " minutes interval");
            } else {
                log.info("Pipeline started with " + repetitions + " repetitions at " + delayMinutes
                    + " minutes interval");
            }

            if (startNode == null) {
                log.info("Pipeline start at pipeline start nodes");
            } else {
                log.info("Pipeline start at node: " + startNode.getModuleName());
            }

            if (endNode == null) {
                log.info("Pipeline end at pipeline end nodes");
            } else {
                log.info("Pipeline end at node: " + endNode.getModuleName());
            }

            PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
            pipelineOps.sendPipelineMessage(
                new FireTriggerRequest(pipeline.getName(), instanceNameTextField.getText(),
                    startNode, endNode, repetitions, delayMinutes * 60));

            setCursor(null);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        setVisible(false);
    }

    private RepetitionsAndDelay getRepetitionAndDelay() {

        int repetitions;
        int delayMinutes;

        if (unlimitedCheckBox.isSelected()) {
            repetitions = -1;
        } else {
            String repsText = repetitionsTextField.getText();
            if (repsText.isEmpty()) {
                throw new IllegalStateException("Number of repetitions is empty");
            }
            repetitions = Integer.parseInt(repsText);
            if (repetitions == 0) {
                throw new IllegalStateException("Number of repetitions is zero");
            }
        }

        if (repetitions != 1) {
            String delayText = delayTextField.getText();
            if (delayText.isEmpty()) {
                throw new IllegalStateException("Repetitions delay is empty");
            }
            int delay = Integer.parseInt(delayText);
            if (delay == 0) {
                throw new IllegalStateException("Delay is set to zero");
            }
            DelayUnits delayUnits = (DelayUnits) delayUnitsComboBox.getSelectedItem();
            delayMinutes = switch (delayUnits) {
                case MINUTES -> delay;
                case HOURS -> 60 * delay;
                case DAYS -> 60 * 24 * delay;
                default -> throw new IllegalStateException(
                    "Delay unit " + delayUnits + " not supported");
            };
        } else {
            delayMinutes = 0;
        }

        return new RepetitionsAndDelay(repetitions, delayMinutes);
    }

    private void cancel(ActionEvent evt) {
        setVisible(false);
    }

    public static void main(String[] args) {
        PipelineDefinition dummyPipeline = new PipelineDefinition("dummy");
        PipelineModuleDefinition dummyModule = new PipelineModuleDefinition("dmy");
        PipelineDefinitionNode dummyNode = new PipelineDefinitionNode(dummyModule.getName(),
            dummyPipeline.getName());
        List<PipelineDefinitionNode> rootNode = new ArrayList<>();
        rootNode.add(dummyNode);
        dummyPipeline.setRootNodes(rootNode);
        ZiggySwingUtils.displayTestDialog(new StartPipelineDialog(null, dummyPipeline));
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
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RepetitionsAndDelay other = (RepetitionsAndDelay) obj;
            return delayMinutes == other.delayMinutes && repetitions == other.repetitions;
        }
    }
}
