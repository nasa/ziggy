package gov.nasa.ziggy.ui.pipeline;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;

/**
 * Hold the ParameterSetName assigned for a specified type. Used by the ParameterSetNamesTableModel.
 *
 * @author Todd Klaus
 */
public class ParameterSetAssignment {
    private ClassWrapper<ParametersInterface> type = null;
    private String assignedName = null;
    private boolean assignedAtPipelineLevel = false;
    private boolean assignedAtBothLevels = false;

    public ParameterSetAssignment(ClassWrapper<ParametersInterface> type) {
        this.type = type;
    }

    public ParameterSetAssignment(ClassWrapper<ParametersInterface> type, String assignedName,
        boolean assignedAtPipelineLevel, boolean assignedAtBothLevels) {
        this.type = type;
        this.assignedName = assignedName;
        this.assignedAtPipelineLevel = assignedAtPipelineLevel;
        this.assignedAtBothLevels = assignedAtBothLevels;
    }

    public ClassWrapper<ParametersInterface> getType() {
        return type;
    }

    public void setType(ClassWrapper<ParametersInterface> type) {
        this.type = type;
    }

    public String getAssignedName() {
        return assignedName;
    }

    public void setAssignedName(String assignedName) {
        this.assignedName = assignedName;
    }

    public boolean isAssignedAtPipelineLevel() {
        return assignedAtPipelineLevel;
    }

    public void setAssignedAtPipelineLevel(boolean assignedAtPipelineLevel) {
        this.assignedAtPipelineLevel = assignedAtPipelineLevel;
    }

    public boolean isAssignedAtBothLevels() {
        return assignedAtBothLevels;
    }

    public void setAssignedAtBothLevels(boolean assignedAtBothLevels) {
        this.assignedAtBothLevels = assignedAtBothLevels;
    }
}
