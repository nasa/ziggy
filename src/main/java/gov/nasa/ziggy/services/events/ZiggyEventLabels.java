package gov.nasa.ziggy.services.events;

import gov.nasa.ziggy.parameters.InternalParameters;

/**
 * Contains the event labels associated with a particular event that has been managed by the
 * {@link ZiggyEventHandler}.
 *
 * @author PT
 */
public class ZiggyEventLabels implements InternalParameters {

    private String eventHandlerName;
    private String eventName;
    private String[] eventLabels;

    public String getEventHandlerName() {
        return eventHandlerName;
    }

    public void setEventHandlerName(String eventHandlerName) {
        this.eventHandlerName = eventHandlerName;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String[] getEventLabels() {
        return eventLabels;
    }

    public void setEventLabels(String[] eventLabels) {
        this.eventLabels = eventLabels;
    }

}
