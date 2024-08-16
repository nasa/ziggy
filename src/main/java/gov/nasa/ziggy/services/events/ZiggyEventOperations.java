package gov.nasa.ziggy.services.events;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for methods that primarily concern {@link ZiggyEvent}s.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ZiggyEventOperations extends DatabaseOperations {

    private ZiggyEventCrud ziggyEventCrud = new ZiggyEventCrud();

    public void newZiggyEvent(String eventHandlerName, String pipelineName, long pipelineInstanceId,
        Set<String> eventLabels) {
        performTransaction(() -> ziggyEventCrud().persist(
            new ZiggyEvent(eventHandlerName, pipelineName, pipelineInstanceId, eventLabels)));
    }

    public List<ZiggyEvent> events(Collection<PipelineInstance> instances) {
        return performTransaction(() -> ziggyEventCrud().retrieve(instances));
    }

    public ZiggyEventHandler mergeEventHandler(ZiggyEventHandler eventHandler) {
        return performTransaction(() -> ziggyEventCrud().merge(eventHandler));
    }

    public List<ZiggyEventHandler> eventHandlers() {
        return performTransaction(() -> ziggyEventCrud().retrieveAllEventHandlers());
    }

    ZiggyEventCrud ziggyEventCrud() {
        return ziggyEventCrud;
    }
}
