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
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.PipelineStepTaskMetrics;

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
        JLabel id = boldLabel("ID:");
        JLabel idTextField = new JLabel(Long.toString(pipelineTask.getPipelineTaskId()));

        JLabel state = boldLabel("Processing step:");
        JLabel stateTextField = new JLabel(HtmlBuilder.htmlBuilder()
            .appendBoldColor(pipelineTask.getDisplayProcessingStep(),
                pipelineTask.isError() ? "red" : "green")
            .toString());

        JLabel node = boldLabel("Node:");
        JLabel nodeTextField = new JLabel(pipelineTask.getPipelineStepName());

        JLabel uow = boldLabel("Unit of work:");
        JLabel uowTextField = new JLabel(pipelineTask.getBriefState());

        JLabel worker = boldLabel("Worker:");
        JLabel workerTextField = new JLabel(pipelineTask.getWorkerName());
        JLabel workerHelp = new JLabel("(host:thread)");

        JLabel created = boldLabel("Created:");
        JLabel createdTextField = new JLabel(DisplayModel.formatDate(pipelineTask.getCreated()));

        JLabel duration = boldLabel("Duration:");
        JLabel durationTextField = new JLabel(pipelineTask.getExecutionClock().toString());

        JLabel ziggyRevision = boldLabel("Ziggy software revision:");
        JLabel ziggyRevisionTextField = new JLabel(pipelineTask.getZiggySoftwareRevision());

        JLabel pipelineRevision = boldLabel("Pipeline software revision:");
        JLabel pipelineRevisionTextField = new JLabel(pipelineTask.getPipelineSoftwareRevision());

        JLabel failureCount = boldLabel("Failure count:");
        JLabel failureCountTextField = new JLabel(Integer.toString(pipelineTask.getFailureCount()));

        ZiggyTable<PipelineStepTaskMetrics> processingBreakdownTable = createProcessingBreakdownTable();
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
                        .addComponent(id)
                        .addComponent(state)
                        .addComponent(node)
                        .addComponent(uow)
                        .addComponent(worker)
                        .addComponent(duration)
                        .addComponent(created)
                        .addComponent(ziggyRevision)
                        .addComponent(pipelineRevision)
                        .addComponent(failureCount))
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addGroup(dataPanelLayout.createParallelGroup()
                        .addComponent(idTextField)
                        .addComponent(stateTextField)
                        .addComponent(nodeTextField)
                        .addComponent(uowTextField)
                        .addGroup(dataPanelLayout.createSequentialGroup()
                            .addComponent(workerTextField)
                            .addComponent(workerHelp))
                        .addComponent(durationTextField)
                        .addComponent(createdTextField)
                        .addComponent(ziggyRevisionTextField)
                        .addComponent(pipelineRevisionTextField)
                        .addComponent(failureCountTextField)))
                .addComponent(processingBreakdownTableScrollPane)));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(id).addComponent(idTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(state)
                .addComponent(stateTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(node)
                .addComponent(nodeTextField))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(uow).addComponent(uowTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(worker)
                .addComponent(workerTextField)
                .addComponent(workerHelp))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(duration)
                .addComponent(durationTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(created)
                .addComponent(createdTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(ziggyRevision)
                .addComponent(ziggyRevisionTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(pipelineRevision)
                .addComponent(pipelineRevisionTextField))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(failureCount)
                .addComponent(failureCountTextField))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(processingBreakdownTableScrollPane));

        return dataPanel;
    }

    private ZiggyTable<PipelineStepTaskMetrics> createProcessingBreakdownTable() {
        TaskMetricsTableModel processingBreakdownTableModel = new TaskMetricsTableModel(
            List.of(pipelineTask), List.of(pipelineTask.getPipelineStepName()), false);
        return new ZiggyTable<>(processingBreakdownTableModel);
    }

    public static void showTaskInfoDialog(Window owner, PipelineTaskDisplayData pipelineTask) {
        new TaskInfoDialog(owner, pipelineTask).setVisible(true);
    }
}
