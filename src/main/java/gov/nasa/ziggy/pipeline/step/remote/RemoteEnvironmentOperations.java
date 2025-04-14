package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Database operations methods for {@link RemoteEnvironment}. */
public class RemoteEnvironmentOperations extends DatabaseOperations {

    private RemoteEnvironmentCrud remoteEnvironmentCrud = new RemoteEnvironmentCrud();

    public void persist(RemoteEnvironment remoteEnvironment) {
        performTransaction(() -> remoteEnvironmentCrud().persist(remoteEnvironment));
    }

    public Architecture merge(Architecture architecture) {
        return performTransaction(() -> remoteEnvironmentCrud().merge(architecture));
    }

    public BatchQueue merge(BatchQueue batchQueue) {
        return performTransaction(() -> remoteEnvironmentCrud.merge(batchQueue));
    }

    public RemoteEnvironment merge(RemoteEnvironment remoteEnvironment) {
        return performTransaction(() -> remoteEnvironmentCrud().merge(remoteEnvironment));
    }

    public RemoteEnvironment remoteEnvironment(String name) {
        return performTransaction(() -> remoteEnvironmentCrud().retrieveRemoteEnvironment(name));
    }

    public List<String> remoteEnvironmentNames() {
        return performTransaction(() -> remoteEnvironmentCrud().retrieveRemoteEnvironmentNames());
    }

    public Map<String, RemoteEnvironment> remoteEnvironmentByName() {
        Map<String, RemoteEnvironment> remoteEnvironmentByName = new TreeMap<>();
        List<RemoteEnvironment> remoteEnvironments = performTransaction(
            () -> remoteEnvironmentCrud().retrieveAllEnvironments());
        for (RemoteEnvironment remoteEnvironment : remoteEnvironments) {
            remoteEnvironmentByName.put(remoteEnvironment.getName(), remoteEnvironment);
        }
        return remoteEnvironmentByName;
    }

    RemoteEnvironmentCrud remoteEnvironmentCrud() {
        return remoteEnvironmentCrud;
    }
}
