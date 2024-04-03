package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;

/**
 * @author Todd Klaus
 */
public class KeyValuePairCrudProxy {

    public KeyValuePairCrudProxy() {
    }

    public void save(final KeyValuePair keyValuePair) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.create(keyValuePair);
            return null;
        });
    }

    public void delete(final KeyValuePair keyValuePair) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            crud.delete(keyValuePair);
            return null;
        });
    }

    public KeyValuePair retrieve(final String key) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            return crud.retrieve(key);
        });
    }

    public List<KeyValuePair> retrieveAll() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            KeyValuePairCrud crud = new KeyValuePairCrud();
            return crud.retrieveAll();
        });
    }
}
