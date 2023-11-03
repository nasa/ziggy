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

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
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

    public RestartDialog(Window owner, List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {

        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(failedTasks, taskAttrs);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {

        setTitle("Restart failed tasks");

        getContentPane().add(createDataPanel(failedTasks, taskAttrs), BorderLayout.CENTER);
        getContentPane().add(ZiggySwingUtils.createButtonPanel(createButton(RESTART, this::restart),
            createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        pack();
    }

    private JScrollPane createDataPanel(List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {
        return new JScrollPane(createRestartTable(failedTasks, taskAttrs));
    }

    private JTable createRestartTable(List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {

        restartTableModel = new RestartTableModel(failedTasks, taskAttrs);

        List<RestartAttributes> modules = restartTableModel.getModuleList();
        final List<TableCellEditor> editors = new ArrayList<>();

        for (RestartAttributes module : modules) {
            editors.add(
                new DefaultCellEditor(new JComboBox<>(new Vector<>(module.getRestartModes()))));
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

    // TODO Return Map<PipelineTask, RunMode> and delete this comment
    // Note that the restartAttrs variable is clobbered during the loop and the last task wins.
    // Given that what eventually happens is that each task gets its own restart message, and
    // the restart message includes the restart mode, the right thing to do is probably to
    // change the logic such that a Map<PipelineTask, RunMode> is returned and then passed to
    // the PipelineExecutorProxy. That way we can be sure that we’re doing the right thing.
    public static RunMode restartTasks(Window owner, List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {

        RestartDialog dialog = new RestartDialog(owner, failedTasks, taskAttrs);
        dialog.cancelled = false;
        dialog.setVisible(true);
        if (dialog.cancelled) {
            log.info("Restart cancelled by user");
            return null;
        }

        RestartAttributes restartAttrs = null;
        Map<String, RestartAttributes> moduleMap = dialog.restartTableModel.getModuleMap();

        for (PipelineTask failedTask : failedTasks) {
            String moduleName = failedTask.getModuleName();

            ProcessingSummary attrs = taskAttrs.get(failedTask.getId());
            String pState = ProcessingState.INITIALIZING.toString();

            if (attrs != null) {
                pState = attrs.getProcessingState().shortName();
            }

            String key = RestartAttributes.key(moduleName, pState);

            restartAttrs = moduleMap.get(key);

            log.info("Set task " + failedTask.getId() + " restartMode to "
                + restartAttrs.getSelectedRestartMode());
        }
        return restartAttrs.getSelectedRestartMode();
    }

    private static class RestartTableModel extends AbstractTableModel {
        private static final Logger log = LoggerFactory.getLogger(RestartTableModel.class);

        private final List<RestartAttributes> moduleList;
        private final Map<String, RestartAttributes> moduleMap;

        public RestartTableModel(List<PipelineTask> failedTasks,
            Map<Long, ProcessingSummary> taskAttrMap) {
            moduleMap = new HashMap<>();

            for (PipelineTask task : failedTasks) {
                String moduleName = task.getModuleName();
                String pState = ProcessingState.INITIALIZING.toString();

                ProcessingSummary taskAttrs = taskAttrMap.get(task.getId());
                if (taskAttrs != null) {
                    pState = taskAttrs.getProcessingState().shortName();
                }

                String key = RestartAttributes.key(moduleName, pState);

                RestartAttributes module = moduleMap.get(key);

                if (module == null) {
                    List<RunMode> supportedModes = task.getModuleImplementation()
                        .supportedRestartModes();
                    RunMode selectedMode = supportedModes.get(0);

                    module = new RestartAttributes(moduleName, pState, 1, supportedModes,
                        selectedMode);

                    moduleMap.put(key, module);
                } else {
                    module.incrementCount();
                }
            }

            moduleList = new LinkedList<>(moduleMap.values());

            log.debug("moduleList.size() = " + moduleList.size());
        }

        @Override
        public int getRowCount() {
            return moduleList.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            log.debug("getValueAt(r=" + rowIndex + ", c=" + columnIndex + ")");

            RestartAttributes restartGroup = moduleList.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> restartGroup.getModuleName();
                case 1 -> restartGroup.getProcessingState();
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
                case 0 -> "Module";
                case 1 -> "P-state";
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
            log.debug(
                "setValueAt(r=" + rowIndex + ", c=" + columnIndex + ", value-=" + value + ")");

            if (columnIndex != 3) {
                throw new IllegalArgumentException("read-only columnIndex = " + columnIndex);
            }
            RestartAttributes module = moduleList.get(rowIndex);
            module.setSelectedRestartMode((RunMode) value);
        }

        public List<RestartAttributes> getModuleList() {
            return moduleList;
        }

        public Map<String, RestartAttributes> getModuleMap() {
            return moduleMap;
        }
    }

    /**
     * @author Todd Klaus
     */
    private static class RestartAttributes {
        private final String moduleName;
        private final String processingState;
        private int count;
        private final List<RunMode> restartModes;
        private RunMode selectedRestartMode;

        public RestartAttributes(String moduleName, String processingState, int count,
            List<RunMode> restartModes, RunMode selectedRestartMode) {
            this.moduleName = moduleName;
            this.processingState = processingState;
            this.count = count;
            this.restartModes = restartModes;
            this.selectedRestartMode = selectedRestartMode;
        }

        public static String key(String moduleName, String pState) {
            return moduleName + ":" + pState;
        }

        public void incrementCount() {
            count++;
        }

        public String getModuleName() {
            return moduleName;
        }

        public String getProcessingState() {
            return processingState;
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
