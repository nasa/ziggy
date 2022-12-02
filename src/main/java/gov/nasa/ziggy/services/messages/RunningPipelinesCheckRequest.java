package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Message sent from the client to the worker requesting information on whether there are any
 * running pipeline instances or any instances that are waiting to run. There's no message content
 * other than the request itself.
 *
 * @author PT
 */
public class RunningPipelinesCheckRequest extends PipelineMessage {

    private static final long serialVersionUID = 20210318L;

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.handleRunningPipelinesCheckRequest(this);
        return null;
    }
}
