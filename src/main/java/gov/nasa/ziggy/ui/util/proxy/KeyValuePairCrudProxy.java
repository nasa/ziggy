package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * @author Todd Klaus
 */
public class KeyValuePairCrudProxy {

    public KeyValuePairCrudProxy() {
    }

    public void save(final KeyValuePair keyValuePair) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.create(keyValuePair);
            return null;
        });
    }

    public void delete(final KeyValuePair keyValuePair) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.delete(keyValuePair);
            return null;
        });
    }

    public KeyValuePair retrieve(final String key) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            return crud.retrieve(key);
        });
    }

    public List<KeyValuePair> retrieveAll() {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            return crud.retrieveAll();
        });
    }
}
