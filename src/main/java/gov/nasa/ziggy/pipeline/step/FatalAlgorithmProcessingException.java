package gov.nasa.ziggy.pipeline.step;

import gov.nasa.ziggy.util.PipelineException;

/**
 * PipelineStepExecutors should throw this exception for cases where the pipeline should not attempt
 * to re-try this task.
 * <p>
 * The difference between a FatalAlgorithmProcessingException and any other kind of exception is
 * whether the pipeline will attempt to automatically re-try the task by rolling back the messaging
 * transaction. This puts the message back on the queue for delivery to another (or possibly the
 * same) worker. For FatalAlgorithmProcessingException, the pipeline will just mark the task as
 * failed and will not retry (the operator can manually retry using the console once they fix the
 * problem). For any other exception, it will retry. So, if you know for sure that the problem is
 * 'permanent', like the requisite inputs not being available, incorrect configuration, or anything
 * else where the error will continue to occur no matter how many times you try, you should throw
 * FatalAlgorithmProcessingException.
 *
 * @author Todd Klaus
 */
public class FatalAlgorithmProcessingException extends PipelineException {
    private static final long serialVersionUID = 20250310L;

    public FatalAlgorithmProcessingException(String errorText, Throwable t) {
        super(errorText, t);
    }

    public FatalAlgorithmProcessingException(String errorText) {
        super(errorText);
    }
}
