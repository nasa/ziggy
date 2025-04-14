package gov.nasa.ziggy.pipeline.step.subtask;

/**
 * Simple container for information on the subtasks within a given task / unit of work.
 *
 * @author PT
 */
public class SubtaskInformation {

    private final String pipelineStepName;
    private final String uowBriefState;
    private final int subtaskCount;

    public SubtaskInformation(String pipelineStepName, String uowBriefState, int subtaskCount) {
        this.pipelineStepName = pipelineStepName;
        this.uowBriefState = uowBriefState;
        this.subtaskCount = subtaskCount;
    }

    public String getPipelineStepName() {
        return pipelineStepName;
    }

    public String getUowBriefState() {
        return uowBriefState;
    }

    public int getSubtaskCount() {
        return subtaskCount;
    }
}
