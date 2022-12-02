package gov.nasa.ziggy.ui;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineUIException extends Exception {
    public PipelineUIException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public PipelineUIException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public PipelineUIException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public PipelineUIException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }
}
