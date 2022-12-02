package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Tells clients that the worker is shutting down, and therefore they should do so as well. The
 * shutdown is executed by invoking the client's shutdown method.
 *
 * @author PT
 */
public class WorkerShutdownMessage extends PipelineMessage {

    private static final long serialVersionUID = 20210318L;

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.handleShutdownMessage(this);
        return null;
    }
}
