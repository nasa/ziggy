package gov.nasa.ziggy.services.messages;

/**
 * Carries the contents of a single task log file to a requestor.
 *
 * @author PT
 */
public class SingleTaskLogMessage extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;
    private final String taskLogContents;

    public SingleTaskLogMessage(SingleTaskLogRequest originalMessage, String taskLogContents) {
        super(originalMessage);
        this.taskLogContents = taskLogContents;
    }

    public String taskLogContents() {
        return taskLogContents;
    }
}
