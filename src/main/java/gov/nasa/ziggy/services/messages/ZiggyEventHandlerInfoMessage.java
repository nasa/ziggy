package gov.nasa.ziggy.services.messages;

import java.util.Set;

import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;

/**
 * Transmits a {@link Set} of {@link ZiggyEventHandlerInfoForDisplay} instances. This message is
 * sent in response to the {@link EventHandlerRequest} message.
 *
 * @author PT
 */
public class ZiggyEventHandlerInfoMessage extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;

    private final Set<ZiggyEventHandlerInfoForDisplay> eventHandlerInfo;

    public ZiggyEventHandlerInfoMessage(EventHandlerRequest originalMessage,
        Set<ZiggyEventHandlerInfoForDisplay> eventHandlerInfo) {
        super(originalMessage);
        this.eventHandlerInfo = eventHandlerInfo;
    }

    public Set<ZiggyEventHandlerInfoForDisplay> getEventHandlerInfo() {
        return eventHandlerInfo;
    }

    @Override
    public String toString() {
        return super.toString() + ", " + eventHandlerInfo.size() + " event handlers";
    }
}
