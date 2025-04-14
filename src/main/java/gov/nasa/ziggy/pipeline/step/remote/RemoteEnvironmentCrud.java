package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;

public class RemoteEnvironmentCrud extends AbstractCrud<RemoteEnvironment> {

    public List<String> retrieveRemoteEnvironmentNames() {
        ZiggyQuery<RemoteEnvironment, String> query = createZiggyQuery(RemoteEnvironment.class,
            String.class);
        query.column(RemoteEnvironment_.name).select();
        return list(query);
    }

    public RemoteEnvironment retrieveRemoteEnvironment(String name) {
        ZiggyQuery<RemoteEnvironment, RemoteEnvironment> query = createZiggyQuery(
            RemoteEnvironment.class);
        query.column(RemoteEnvironment_.name).in(name);
        return uniqueResult(query);
    }

    public List<RemoteEnvironment> retrieveAllEnvironments() {
        return list(createZiggyQuery(RemoteEnvironment.class));
    }

    @Override
    public Class<RemoteEnvironment> componentClass() {
        return RemoteEnvironment.class;
    }
}
