package gov.nasa.ziggy.ui.ops.instances;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class RestartTableModel extends AbstractTableModel {
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

                module = new RestartAttributes(moduleName, pState, 1, supportedModes, selectedMode);

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

        switch (columnIndex) {
            case 0:
                return restartGroup.getModuleName();
            case 1:
                return restartGroup.getProcessingState();
            case 2:
                return restartGroup.getCount();
            case 3:
                return restartGroup.getSelectedRestartMode();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public int getColumnCount() {
        return 4;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Module";
            case 1:
                return "P-state";
            case 2:
                return "Count";
            case 3:
                return "Restart Mode";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
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
        log.debug("setValueAt(r=" + rowIndex + ", c=" + columnIndex + ", value-=" + value + ")");

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
