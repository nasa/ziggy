package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class KeyValuePairCrudProxy extends CrudProxy {

    public KeyValuePairCrudProxy() {
    }

    public void save(final KeyValuePair keyValuePair) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.create(keyValuePair);
            return null;
        });
    }

    public void delete(final KeyValuePair keyValuePair) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.delete(keyValuePair);
            return null;
        });
    }

    public KeyValuePair retrieve(final String key) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            KeyValuePair result1 = crud.retrieve(key);
            return result1;
        });
    }

    public List<KeyValuePair> retrieveAll() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            List<KeyValuePair> r = crud.retrieveAll();
            return r;
        });
    }
}
