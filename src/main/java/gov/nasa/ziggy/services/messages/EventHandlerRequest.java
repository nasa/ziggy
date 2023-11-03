package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.util.Requestor;

/**
 * Requests that the supervisor send the UI the full list of {@link ZiggyEventHandler} instances.
 *
 * @author PT
 */
public class EventHandlerRequest extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;

    public EventHandlerRequest(Requestor sender) {
        super(sender);
    }
}
