package gov.nasa.ziggy.services.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.proxy.CrudProxy;

/**
 * Sends a request to the worker process to toggle the state of a single {@link ZiggyEventHandler}
 * instance.
 *
 * @author PT
 */
public class EventHandlerToggleStateRequest extends PipelineMessage {

    private static final long serialVersionUID = 20220707L;

    private static final Logger log = LoggerFactory.getLogger(EventHandlerToggleStateRequest.class);

    private final String handlerName;

    /**
     * Constructs and sends the request. A static method is used in order to make the constructor
     * private, which in turn forces the user to verify privileges before the request is sent.
     */
    public static void requestEventHandlerToggle(String handlerName) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        log.debug("Sending toggle request for event handler \"" + handlerName + "\"");
        UiCommunicator.send(new EventHandlerToggleStateRequest(handlerName));
    }

    private EventHandlerToggleStateRequest(String handlerName) {
        this.handlerName = handlerName;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.handleEventHandlerToggleRequest(handlerName);
        return null;
    }

}
