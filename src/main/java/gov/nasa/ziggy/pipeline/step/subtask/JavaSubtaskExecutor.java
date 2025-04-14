package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.File;

import gov.nasa.ziggy.pipeline.step.FatalAlgorithmProcessingException;

/**
 * Subclass of {@link SubtaskExecutor} that provides additional support for Java algorithms.
 *
 * @author PT
 */
public class JavaSubtaskExecutor extends SubtaskExecutor {

    JavaSubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs) {
        super(taskDir, subtaskIndex, binaryName, timeoutSecs);
    }

    @Override
    protected void initialize() {
        throw new FatalAlgorithmProcessingException("JavaSubtaskExecutor not yet supported");
    }
}
