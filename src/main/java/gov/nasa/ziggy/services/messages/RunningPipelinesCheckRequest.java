package gov.nasa.ziggy.services.messages;

/**
 * Message sent from the client to the supervisor requesting information on whether there are any
 * running pipeline instances or any instances that are waiting to run. There's no message content
 * other than the request itself.
 *
 * @author PT
 */
public class RunningPipelinesCheckRequest extends PipelineMessage {

    private static final long serialVersionUID = 20230511L;
}
