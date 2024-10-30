package gov.nasa.ziggy.module;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;

/**
 * Provides summary information on failed subtasks.
 *
 * @author PT
 */
public class ProcessingFailureSummary {

    private List<String> failedSubtaskDirs = new ArrayList<>();
    private boolean allTasksFailed = false;

    public ProcessingFailureSummary(String moduleName, File taskDirectory) {

        SubtaskDirectoryIterator taskDirectoryIterator = new SubtaskDirectoryIterator(
            taskDirectory);
        int numSubtasks = taskDirectoryIterator.numSubtasks();

        // loop over subtask directories and look for stack trace files

        while (taskDirectoryIterator.hasNext()) {
            File subtaskDir = taskDirectoryIterator.next().getSubtaskDir();
            if (ModuleInterfaceUtils.errorFile(subtaskDir, moduleName).exists()) {
                failedSubtaskDirs.add(subtaskDir.getName());
            }
        }
        allTasksFailed = failedSubtaskDirs.size() == numSubtasks;
    }

    public boolean isAllTasksFailed() {
        return allTasksFailed;
    }

    public List<String> getFailedSubtaskDirs() {
        return failedSubtaskDirs;
    }

    public boolean isAllTasksSucceeded() {
        return failedSubtaskDirs.size() == 0;
    }
}
