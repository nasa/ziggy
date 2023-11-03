package gov.nasa.ziggy.services.events;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.parameters.InternalParameters;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * Contains the event labels associated with a particular event that has been managed by the
 * {@link ZiggyEventHandler}.
 *
 * @author PT
 */
public class ZiggyEventLabels extends InternalParameters {

    // If a field is renamed, update the parameter string in its setter.

    private String eventHandlerName;
    private String eventName;
    private String[] eventLabels;

    public String getEventHandlerName() {
        return eventHandlerName;
    }

    public void setEventHandlerName(String eventHandlerName) {
        this.eventHandlerName = eventHandlerName;
        addParameter(new TypedParameter("eventHandlerName", this.eventHandlerName));
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
        addParameter(new TypedParameter("eventName", this.eventName));
    }

    public String[] getEventLabels() {
        return eventLabels;
    }

    public void setEventLabels(String[] eventLabels) {
        this.eventLabels = eventLabels;
        addParameter(new TypedParameter("eventLabels",
            ZiggyArrayUtils.arrayToString(this.eventLabels), ZiggyDataType.ZIGGY_STRING, false));
    }
}
