package gov.nasa.ziggy.services.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;

/**
 * Sends a request to the supervisor process to toggle the state of a single
 * {@link ZiggyEventHandler} instance.
 *
 * @author PT
 */
public class EventHandlerToggleStateRequest extends PipelineMessage {

    private static final long serialVersionUID = 20230511L;

    private static final Logger log = LoggerFactory.getLogger(EventHandlerToggleStateRequest.class);

    private final String handlerName;

    /**
     * Constructs and sends the request. A static method is used in order to make the constructor
     * private, which in turn forces the user to verify privileges before the request is sent.
     */
    public static void requestEventHandlerToggle(String handlerName) {
        log.debug("Sending toggle request for event handler \"" + handlerName + "\"");
        ZiggyMessenger.publish(new EventHandlerToggleStateRequest(handlerName));
    }

    private EventHandlerToggleStateRequest(String handlerName) {
        this.handlerName = handlerName;
    }

    public String getHandlerName() {
        return handlerName;
    }
}
