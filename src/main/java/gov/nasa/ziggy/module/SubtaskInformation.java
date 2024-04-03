package gov.nasa.ziggy.module;

/**
 * Simple container for information on the subtasks within a given task / unit of work.
 *
 * @author PT
 */
public class SubtaskInformation {

    private final String moduleName;
    private final String uowBriefState;
    private final int subtaskCount;

    public SubtaskInformation(String moduleName, String uowBriefState, int subtaskCount) {
        this.moduleName = moduleName;
        this.uowBriefState = uowBriefState;
        this.subtaskCount = subtaskCount;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getUowBriefState() {
        return uowBriefState;
    }

    public int getSubtaskCount() {
        return subtaskCount;
    }
}
