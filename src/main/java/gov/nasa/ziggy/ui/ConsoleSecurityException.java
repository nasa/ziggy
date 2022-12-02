package gov.nasa.ziggy.ui;

import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ConsoleSecurityException extends PipelineException {
    public ConsoleSecurityException() {
        super();
    }

    public ConsoleSecurityException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsoleSecurityException(String message) {
        super(message);
    }

    public ConsoleSecurityException(Throwable cause) {
        super(cause);
    }
}
