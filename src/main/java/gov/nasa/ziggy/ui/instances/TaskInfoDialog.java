package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.DisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.ModuleTaskMetrics;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class TaskInfoDialog extends javax.swing.JDialog {
    private PipelineTaskDisplayData pipelineTask;

    public TaskInfoDialog(Window owner, PipelineTaskDisplayData pipelineTask) {
        super(owner);
        this.pipelineTask = pipelineTask;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Pipeline task details");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        pack();
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private JPanel createDataPanel() {
        JLabel idLabel = boldLabel("ID:");
        JLabel idTextField = new JLabel(Long.toString(pipelineTask.getPipelineTaskId()));

        JLabel stateLabel = boldLabel("Processing step:");
        JLabel stateTextField = new JLabel(HtmlBuilder.htmlBuilder()
            .appendBoldColor(pipelineTask.getDisplayProcessingStep(),
                pipelineTask.isError() ? "red" : "green")
            .toString());

        JLabel moduleLabel = boldLabel("Module:");
        JLabel moduleTextField = new JLabel(pipelineTask.getModuleName());

        JLabel uowLabel = boldLabel("Unit of work:");
        JLabel uowTextField = new JLabel(pipelineTask.getBriefState());

        JLabel workerLabel = boldLabel("Worker:");
        JLabel workerTextField = new JLabel(pipelineTask.getWorkerName());
        JLabel workerHelpLabel = new JLabel("(host:thread)");

        JLabel createdLabel = boldLabel("Created:");
        JLabel createdTextField = new JLabel(DisplayModel.formatDate(pipelineTask.getCreated()));

        JLabel durationLabel = boldLabel("Duration:");
        JLabel durationTextField = new JLabel(pipelineTask.getExecutionClock().toString());

        JLabel ziggyRevisionLabel = boldLabel("Ziggy software revision:");
        JLabel ziggyRevisionTextField = new JLabel(pipelineTask.getZiggySoftwareRevision());

        JLabel pipelineRevisionLabel = boldLabel("Pipeline software revision:");
        JLabel pipelineRevisionTextField = new JLabel(pipelineTask.getPipelineSoftwareRevision());

        JLabel failureCountLabel = boldLabel("Failure count:");
        JLabel failureCountTextField = new JLabel(Integer.toString(pipelineTask.getFailureCount()));

        ZiggyTable<ModuleTaskMetrics> processingBreakdownTable = createProcessingBreakdownTable();
        JScrollPane processingBreakdownTableScrollPane = new JScrollPane(
            processingBreakdownTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanelLayout.setAutoCreateGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup()
                .addGroup(dataPanelLayout.createSequentialGroup()
                    .addGroup(dataPanelLayout.createParallelGroup()
                        .addComponent(idLabel)
                        .addComponent(stateLabel)
                        .addComponent(moduleLabel)
                        .addComponent(uowLabel)
                        .addComponent(workerLabel)
                        .addComponent(durationLabel)
                        .addComponent(createdLabel)
                        .addComponent(ziggyRevisionLabel)
                        .addComponent(pipelineRevisionLabel)
                        .addComponent(failureCountLabel))
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addGroup(dataPanelLayout.createParallelGroup()
                        .addComponent(idTextField)
                        .addComponent(stateTextField)
                        .addComponent(moduleTextField)
                        .addComponent(uowTextField)
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addComponent(workerTextField)
                            .addComponent(workerHelpLabel))
                        .addComponent(durationTextField)
                        .addComponent(createdTextField)
                        .addComponent(ziggyRevisionTextField)
                        .addComponent(pipelineRevisionTextField)
                        .addComponent(failureCountTextField)))
                .addComponent(processingBreakdownTableScrollPane)));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(idLabel)
                .addComponent(idTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(stateLabel)
                .addComponent(stateTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(moduleLabel)
                .addComponent(moduleTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(uowLabel)
                .addComponent(uowTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(workerLabel)
                .addComponent(workerTextField)
                .addComponent(workerHelpLabel))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(durationLabel)
                .addComponent(durationTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(createdLabel)
                .addComponent(createdTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(ziggyRevisionLabel)
                .addComponent(ziggyRevisionTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(pipelineRevisionLabel)
                .addComponent(pipelineRevisionTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(failureCountLabel)
                .addComponent(failureCountTextField))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(processingBreakdownTableScrollPane));

        return dataPanel;
    }

    private ZiggyTable<ModuleTaskMetrics> createProcessingBreakdownTable() {
        TaskMetricsTableModel processingBreakdownTableModel = new TaskMetricsTableModel(
            List.of(pipelineTask), List.of(pipelineTask.getModuleName()), false);
        return new ZiggyTable<>(processingBreakdownTableModel);
    }

    public static void showTaskInfoDialog(Window owner, PipelineTaskDisplayData pipelineTask) {
        new TaskInfoDialog(owner, pipelineTask).setVisible(true);
    }
}
