package gov.nasa.ziggy.module;

import java.nio.file.Path;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Provides utility functions for PipelineInputs and PipelineOutputs classes.
 *
 * @author PT
 */
public abstract class PipelineInputsOutputsUtils implements Persistable {

    /**
     * Returns the task directory. Assumes that the working directory is the sub-task directory.
     */
    public static Path taskDir() {
        return DirectoryProperties.workingDir().getParent();
    }

    /**
     * Returns the module executable name. Assumes that the working directory is the sub-task
     * directory.
     */
    public static String moduleName() {
        String taskDirString = taskDir().getFileName().toString();
        PipelineTask.TaskBaseNameMatcher m = new PipelineTask.TaskBaseNameMatcher(taskDirString);
        return m.moduleName();
    }

    /**
     * Applies the log stream identifier to the current thread, so that log messages will contain
     * information on which subtask generated them.
     */
    public static void putLogStreamIdentifier() {
        String subtaskName = DirectoryProperties.workingDir().getFileName().toString();
        SubtaskUtils.putLogStreamIdentifier(subtaskName);
    }

}
