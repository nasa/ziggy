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
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.table.SpreadsheetCellRenderer;

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
            // Determine editor to be used by cell.
            @Override
            public TableCellEditor getCellEditor(int row, int column) {
                int modelColumn = convertColumnIndexToModel(column);

                if (modelColumn == 3) {
                    return editors.get(row);
                }
                return super.getCellEditor(row, column);
            }

            @Override
            public TableCellRenderer getCellRenderer(int row, int column) {
                return new SpreadsheetCellRenderer();
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

    public static Map<RunMode, List<PipelineTask>> tasksByRunMode(Window owner,
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTask) {

        RestartDialog dialog = new RestartDialog(owner, supportedRunModesByPipelineTask);
        dialog.cancelled = false;
        dialog.setVisible(true);
        if (dialog.cancelled) {
            log.info("Restart cancelled by user");
            return null;
        }

        Map<RunMode, List<PipelineTask>> tasksByRunMode = new HashMap<>();
        Map<String, RestartAttributes> restartAttributesByPipelineProcessingSteps = dialog.restartTableModel
            .getRestartAttributesByPipelineProcessingSteps();

        for (PipelineTaskDisplayData failedTask : supportedRunModesByPipelineTask.keySet()) {
            String key = RestartAttributes.key(failedTask.getPipelineStepName(),
                failedTask.getProcessingStep());
            RunMode runMode = restartAttributesByPipelineProcessingSteps.get(key)
                .getSelectedRestartMode();
            log.info("Set task {} restartMode to {}", failedTask.getPipelineTaskId(), runMode);
            List<PipelineTask> tasks = tasksByRunMode.get(runMode);
            if (tasks == null) {
                tasks = new ArrayList<>();
                tasksByRunMode.put(runMode, tasks);
            }
            tasks.add(failedTask.getPipelineTask());
        }

        return tasksByRunMode;
    }

    private static class RestartTableModel extends AbstractTableModel {
        private static final Logger log = LoggerFactory.getLogger(RestartTableModel.class);

        private static final String[] COLUMN_NAMES = { "Node", "Status", "Count", "Restart mode" };
        private static final Class<?>[] COLUMN_CLASSES = { String.class, String.class,
            Integer.class, String.class };

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

            allRestartAttributes = new ArrayList<>(
                restartAttributesByPipelineProcessingSteps.values());

            log.debug("allRestartAttributes.size()={}", allRestartAttributes.size());
        }

        @Override
        public int getRowCount() {
            return allRestartAttributes.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return COLUMN_CLASSES[columnIndex];
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            if (columnIndex == 3) {
                return true;
            }
            return super.isCellEditable(rowIndex, columnIndex);
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
