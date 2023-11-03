package gov.nasa.ziggy.services.events;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * CRUD (Create, Retrieve, Update, Delete) class for {@link ZiggyEventHandler} and
 * {@link ZiggyEvent} classes.
 *
 * @author PT
 */
public class ZiggyEventCrud extends AbstractCrud<ZiggyEvent> {

    /**
     * Returns all events. Used by the GUI event display.
     */
    public List<ZiggyEvent> retrieveAllEvents() {
        return list(createZiggyQuery(ZiggyEvent.class));
    }

    /** Returns events associated with the given instances. */
    public List<ZiggyEvent> retrieve(Collection<PipelineInstance> instances) {
        Set<Long> instanceIds = instances.stream()
            .map(PipelineInstance::getId)
            .collect(Collectors.toSet());

        ZiggyQuery<ZiggyEvent, ZiggyEvent> query = createZiggyQuery(ZiggyEvent.class)
            .column(ZiggyEvent_.pipelineInstanceId)
            .in(instanceIds)
            .distinct(true)
            .ascendingOrder();

        return list(query);
    }

    /**
     * Returns all event handlers. Used by the supervisor during initialization.
     */
    public List<ZiggyEventHandler> retrieveAllEventHandlers() {
        return list(createZiggyQuery(ZiggyEventHandler.class));
    }

    @Override
    public Class<ZiggyEvent> componentClass() {
        return ZiggyEvent.class;
    }
}
