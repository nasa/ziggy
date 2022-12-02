package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Requests that the worker send the UI the full list of {@link ZiggyEventHandler} instances.
 *
 * @author PT
 */
public class EventHandlerRequest extends PipelineMessage {

    private static final long serialVersionUID = 20220707L;

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return messageHandler.handleEventHandlerRequest();
    }

}
