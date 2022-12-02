package gov.nasa.ziggy.uow;

import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.Parameters;

/**
 * Defines the task and subtask generation for pipeline modules that use the DefaultUnitOfWork.
 * <p>
 * The taskDirectoryRegex parameter defines a regular expression for the directories below the
 * datastore root that are to be made into units of work. For example, "sector-([0-9]{4})/cal" would
 * make a unit of work for directories under the datastore root such as sector-0001/cal,
 * sector-0002/cal, etc. The singleSubtask parameter indicates whether each task should have a
 * single subtask rather than generating 1 subtask per file based on the pipeline module's
 * DataFileType instances.
 *
 * @author PT
 */
public class TaskConfigurationParameters implements Parameters {

    private String taskDirectoryRegex;
    private boolean singleSubtask;
    private int maxFailedSubtaskCount;
    private boolean reprocess;
    private int maxAutoResubmits;

    @ProxyIgnore
    private long[] reprocessingTasksExclude = new long[0];

    public String getTaskDirectoryRegex() {
        return taskDirectoryRegex;
    }

    public void setTaskDirectoryRegex(String taskDirectoryRegex) {
        this.taskDirectoryRegex = taskDirectoryRegex;
    }

    public boolean isSingleSubtask() {
        return singleSubtask;
    }

    public void setSingleSubtask(boolean singleSubtask) {
        this.singleSubtask = singleSubtask;
    }

    public int getMaxFailedSubtaskCount() {
        return maxFailedSubtaskCount;
    }

    public void setMaxFailedSubtaskCount(int maxFailedSubtaskCount) {
        this.maxFailedSubtaskCount = maxFailedSubtaskCount;
    }

    public boolean isReprocess() {
        return reprocess;
    }

    public void setReprocess(boolean reprocess) {
        this.reprocess = reprocess;
    }

    public long[] getReprocessingTasksExclude() {
        return reprocessingTasksExclude;
    }

    public void setReprocessingTasksExclude(long[] reprocessingTasksExclude) {
        this.reprocessingTasksExclude = reprocessingTasksExclude;
    }

    public int getMaxAutoResubmits() {
        return maxAutoResubmits;
    }

    public void setMaxAutoResubmits(int maxAutoResubmits) {
        this.maxAutoResubmits = maxAutoResubmits;
    }

}
