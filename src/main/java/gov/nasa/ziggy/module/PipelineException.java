package gov.nasa.ziggy.module;

/**
 * PipelineExceptions are thrown by the infrastructure in response to resource or configuration
 * problems. They are generally not handled by client code and should be allowed to propagate back
 * up to the top of the thread where they are handled (for example in the worker or filestore
 * processes)
 *
 * @author Todd Klaus
 */
public class PipelineException extends RuntimeException {
    private static final long serialVersionUID = -5067765463060718311L;

    /**
     * Determines whether a given exception has an exception of a particular class in its "Caused
     * by" chain. It's a little unbelievable to me that Java doesn't have this functionality built
     * into either Exception or Throwable, but okay.
     *
     * @param exception Thrown exception
     * @param expected Class of exception that is tested for
     * @return True if there is an exception of the expected class in the sequence of throws that
     * resulted in the thrown exception, false otherwise.
     */
    public static boolean isCause(Throwable exception, Class<? extends Throwable> expected) {
        return expected.isInstance(exception)
            || exception != null && isCause(exception.getCause(), expected);
    }

    public PipelineException() {
    }

    /**
     * @param message
     */
    public PipelineException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public PipelineException(Throwable cause) {
        super(cause);
    }
}
