package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.RESTART;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellEditor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class RestartDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(RestartDialog.class);

    private RestartTableModel restartTableModel;
    private boolean cancelled;

    public RestartDialog(Window owner,
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {

        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(supportedRunModesByPipelineTask);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {

        setTitle("Restart failed tasks");

        getContentPane().add(createDataPanel(supportedRunModesByPipelineTask), BorderLayout.CENTER);
        getContentPane().add(ZiggySwingUtils.createButtonPanel(createButton(RESTART, this::restart),
            createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        pack();
    }

    private JScrollPane createDataPanel(
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {
        return new JScrollPane(createRestartTable(supportedRunModesByPipelineTask));
    }

    private JTable createRestartTable(
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {

        restartTableModel = new RestartTableModel(supportedRunModesByPipelineTask);

        List<RestartAttributes> allRestartAttributes = restartTableModel.getAllRestartAttributes();
        final List<TableCellEditor> editors = new ArrayList<>();

        for (RestartAttributes restartAttriutes : allRestartAttributes) {
            editors.add(new DefaultCellEditor(
                new JComboBox<>(new Vector<>(restartAttriutes.getRestartModes()))));
        }

        JTable table = new JTable(restartTableModel) {
            // Determine editor to be used by row
            @Override
            public TableCellEditor getCellEditor(int row, int column) {
                int modelColumn = convertColumnIndexToModel(column);

                if (modelColumn == 3) {
                    return editors.get(row);
                }
                return super.getCellEditor(row, column);
            }
        };

        table.setPreferredScrollableViewportSize(new Dimension(750, 350));

        return table;
    }

    private void restart(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    // TODO Return Map<PipelineTaskDisplayData, RunMode> and delete this comment
    // Note that the restartAttrs variable is clobbered during the loop and the last task wins.
    // Given that what eventually happens is that each task gets its own restart message, and
    // the restart message includes the restart mode, the right thing to do is probably to
    // change the logic such that a Map<PipelineTaskDisplayData, RunMode> is returned and then
    // passed to the PipelineExecutorProxy. That way we can be sure that we’re doing the right
    // thing.
    public static RunMode restartTasks(Window owner,
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {

        RestartDialog dialog = new RestartDialog(owner, supportedRunModesByPipelineTask);
        dialog.cancelled = false;
        dialog.setVisible(true);
        if (dialog.cancelled) {
            log.info("Restart cancelled by user");
            return null;
        }

        RestartAttributes restartAttributes = null;
        Map<String, RestartAttributes> restartAttributesByPipelineProcessingSteps = dialog.restartTableModel
            .getRestartAttributesByPipelineProcessingSteps();

        for (PipelineTaskDisplayData failedTask : supportedRunModesByPipelineTask.keySet()) {
            String key = RestartAttributes.key(failedTask.getPipelineStepName(),
                failedTask.getProcessingStep());

            restartAttributes = restartAttributesByPipelineProcessingSteps.get(key);

            log.info("Set task {} restartMode to {}", failedTask.getPipelineTaskId(),
                restartAttributes.getSelectedRestartMode());
        }
        return restartAttributes.getSelectedRestartMode();
    }

    private static class RestartTableModel extends AbstractTableModel {
        private static final Logger log = LoggerFactory.getLogger(RestartTableModel.class);

        private final List<RestartAttributes> allRestartAttributes;
        private final Map<String, RestartAttributes> restartAttributesByPipelineProcessingSteps;

        public RestartTableModel(
            Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {
            restartAttributesByPipelineProcessingSteps = new HashMap<>();

            for (PipelineTaskDisplayData task : supportedRunModesByPipelineTask.keySet()) {
                String pipelineStepName = task.getPipelineStepName();
                ProcessingStep processingStep = task.getProcessingStep();
                String key = RestartAttributes.key(pipelineStepName, processingStep);
                RestartAttributes restartAttributes = restartAttributesByPipelineProcessingSteps
                    .get(key);

                if (restartAttributes == null) {
                    List<RunMode> supportedModes = supportedRunModesByPipelineTask.get(task);
                    RunMode selectedMode = supportedModes.get(0);

                    restartAttributes = new RestartAttributes(pipelineStepName, processingStep, 1,
                        supportedModes, selectedMode);

                    restartAttributesByPipelineProcessingSteps.put(key, restartAttributes);
                } else {
                    restartAttributes.incrementCount();
                }
            }

            allRestartAttributes = new LinkedList<>(
                restartAttributesByPipelineProcessingSteps.values());

            log.debug("allRestartAttributes.size()={}", allRestartAttributes.size());
        }

        @Override
        public int getRowCount() {
            return allRestartAttributes.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            log.debug("rowIndex={}, columnIndex={}", rowIndex, columnIndex);

            RestartAttributes restartGroup = allRestartAttributes.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> restartGroup.getPipelineStepName();
                case 1 -> restartGroup.getProcessingStep();
                case 2 -> restartGroup.getCount();
                case 3 -> restartGroup.getSelectedRestartMode();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public int getColumnCount() {
            return 4;
        }

        @Override
        public String getColumnName(int column) {
            return switch (column) {
                case 0 -> "Node";
                case 1 -> "Status";
                case 2 -> "Count";
                case 3 -> "Restart mode";
                default -> throw new IllegalArgumentException("Unexpected value: " + column);
            };
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            if (columnIndex == 3) {
                return true;
            }
            return super.isCellEditable(rowIndex, columnIndex);
        }

        @Override
        public void setValueAt(Object value, int rowIndex, int columnIndex) {
            log.debug("rowIndex={}, columnIndex={}, value={}", rowIndex, columnIndex, value);

            if (columnIndex != 3) {
                throw new IllegalArgumentException("read-only columnIndex = " + columnIndex);
            }
            RestartAttributes restartAttributes = allRestartAttributes.get(rowIndex);
            restartAttributes.setSelectedRestartMode((RunMode) value);
        }

        public List<RestartAttributes> getAllRestartAttributes() {
            return allRestartAttributes;
        }

        public Map<String, RestartAttributes> getRestartAttributesByPipelineProcessingSteps() {
            return restartAttributesByPipelineProcessingSteps;
        }
    }

    /**
     * @author Todd Klaus
     */
    private static class RestartAttributes {
        private final String pipelineStepName;
        private final ProcessingStep processingStep;
        private int count;
        private final List<RunMode> restartModes;
        private RunMode selectedRestartMode;

        public RestartAttributes(String pipelineStepName, ProcessingStep processingStep, int count,
            List<RunMode> restartModes, RunMode selectedRestartMode) {
            this.pipelineStepName = pipelineStepName;
            this.processingStep = processingStep;
            this.count = count;
            this.restartModes = restartModes;
            this.selectedRestartMode = selectedRestartMode;
        }

        public static String key(String pipelineStepName, ProcessingStep processingStep) {
            return pipelineStepName + ":" + processingStep;
        }

        public void incrementCount() {
            count++;
        }

        public String getPipelineStepName() {
            return pipelineStepName;
        }

        public ProcessingStep getProcessingStep() {
            return processingStep;
        }

        public int getCount() {
            return count;
        }

        public List<RunMode> getRestartModes() {
            return restartModes;
        }

        public RunMode getSelectedRestartMode() {
            return selectedRestartMode;
        }

        public void setSelectedRestartMode(RunMode selectedRestartMode) {
            this.selectedRestartMode = selectedRestartMode;
        }
    }
}
