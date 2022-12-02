package gov.nasa.ziggy.ui.ops.parameters;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;

/**
 * Hold the ParameterSetName assigned for a specified type. Used by the ParameterSetNamesTableModel.
 *
 * @author Todd Klaus
 */
public class ParameterSetAssignment {
    private ClassWrapper<Parameters> type = null;
    private ParameterSetName assignedName = null;
    private boolean assignedAtPipelineLevel = false;
    private boolean assignedAtBothLevels = false;

    public ParameterSetAssignment(ClassWrapper<Parameters> type) {
        this.type = type;
    }

    public ParameterSetAssignment(ClassWrapper<Parameters> type, ParameterSetName assignedName,
        boolean assignedAtPipelineLevel, boolean assignedAtBothLevels) {
        this.type = type;
        this.assignedName = assignedName;
        this.assignedAtPipelineLevel = assignedAtPipelineLevel;
        this.assignedAtBothLevels = assignedAtBothLevels;
    }

//    public ParameterSetAssignment(ClassWrapper<Parameters> type, ParameterSetName assignedName,
//        boolean assignedAtPipelineLevel) {
//        this.type = type;
//        this.assignedName = assignedName;
//        this.assignedAtPipelineLevel = assignedAtPipelineLevel;
//    }

    public ClassWrapper<Parameters> getType() {
        return type;
    }

    public void setType(ClassWrapper<Parameters> type) {
        this.type = type;
    }

    public ParameterSetName getAssignedName() {
        return assignedName;
    }

    public void setAssignedName(ParameterSetName assignedName) {
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
