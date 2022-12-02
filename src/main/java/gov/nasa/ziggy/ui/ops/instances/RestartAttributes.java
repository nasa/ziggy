package gov.nasa.ziggy.ui.ops.instances;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;

/**
 * @author Todd Klaus
 */
class RestartAttributes {
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
