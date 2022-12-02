package gov.nasa.ziggy.module;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;

/**
 * Provides summary information on failed sub-tasks.
 *
 * @author PT
 */
public class ProcessingFailureSummary {

    private List<String> failedSubTaskDirs = new ArrayList<>();
    private boolean allTasksFailed = false;

    public ProcessingFailureSummary(String moduleName, File taskDirectory) {

        SubtaskDirectoryIterator taskDirectoryIterator = new SubtaskDirectoryIterator(
            taskDirectory);
        int numSubTasks = taskDirectoryIterator.numSubTasks();

        // loop over sub-task directories and look for stack trace files

        while (taskDirectoryIterator.hasNext()) {
            File subTaskDir = taskDirectoryIterator.next().getSubtaskDir();
            if (ModuleInterfaceUtils.errorFile(subTaskDir, moduleName).exists()) {
                failedSubTaskDirs.add(subTaskDir.getName());
            }
        }
        allTasksFailed = failedSubTaskDirs.size() == numSubTasks;
    }

    boolean isAllTasksFailed() {
        return allTasksFailed;
    }

    List<String> getFailedSubTaskDirs() {
        return failedSubTaskDirs;
    }

    boolean isAllTasksSucceeded() {
        return failedSubTaskDirs.size() == 0;
    }
}
