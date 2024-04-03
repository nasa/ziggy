package gov.nasa.ziggy.ui.util.proxy;

import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.events.ZiggyEvent;
import gov.nasa.ziggy.services.events.ZiggyEventCrud;

public class ZiggyEventCrudProxy {

    public List<ZiggyEvent> retrieveAllEvents() {
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new ZiggyEventCrud().retrieveAllEvents());
    }

    public List<ZiggyEvent> retrieve(Collection<PipelineInstance> instances) {
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new ZiggyEventCrud().retrieve(instances));
    }
}
