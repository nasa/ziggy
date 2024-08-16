package gov.nasa.ziggy.ui.pipeline;

/**
 * Hold the ParameterSetName assigned for a specified type. Used by the ParameterSetNamesTableModel.
 *
 * @author Todd Klaus
 */
public class ParameterSetAssignment {
    private String assignedName = null;
    private boolean assignedAtPipelineLevel = false;
    private boolean assignedAtBothLevels = false;

    public ParameterSetAssignment(String assignedName, boolean assignedAtPipelineLevel,
        boolean assignedAtBothLevels) {
        this.assignedName = assignedName;
        this.assignedAtPipelineLevel = assignedAtPipelineLevel;
        this.assignedAtBothLevels = assignedAtBothLevels;
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
