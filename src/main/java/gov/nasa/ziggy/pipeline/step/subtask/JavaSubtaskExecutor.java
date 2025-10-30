package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.File;

import gov.nasa.ziggy.pipeline.step.FatalAlgorithmProcessingException;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;

/**
 * Subclass of {@link SubtaskExecutor} that provides additional support for Java algorithms.
 *
 * @author PT
 */
public class JavaSubtaskExecutor extends SubtaskExecutor {

    JavaSubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs,
        float heapSizeGigabytes, TaskConfiguration taskConfiguration) {
        super(taskDir, subtaskIndex, binaryName, timeoutSecs, heapSizeGigabytes, taskConfiguration);
    }

    @Override
    protected void initialize() {
        throw new FatalAlgorithmProcessingException("JavaSubtaskExecutor not yet supported");
    }
}
