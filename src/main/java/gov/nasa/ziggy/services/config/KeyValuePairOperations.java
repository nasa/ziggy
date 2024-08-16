package gov.nasa.ziggy.services.config;

import java.util.List;

import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Provides access to the {@link KeyValuePair} entity.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class KeyValuePairOperations extends DatabaseOperations {
    private KeyValuePairCrud keyValuePairCrud = new KeyValuePairCrud();

    public void persist(KeyValuePair keyValuePair) {
        performTransaction(() -> keyValuePairCrud.persist(keyValuePair));
    }

    public KeyValuePair keyValuePair(String key) {
        return performTransaction(() -> keyValuePairCrud.retrieve(key));
    }

    public List<KeyValuePair> keyValuePairs() {
        return performTransaction(() -> keyValuePairCrud.retrieveAll());
    }

    public String keyValuePairValue(String key) {
        return performTransaction(() -> keyValuePairCrud.retrieve(key).getValue());
    }

    public KeyValuePair updateKeyValuePair(String key, String newValue) {
        return performTransaction(() -> {
            KeyValuePair keyValuePair = new KeyValuePairCrud().retrieve(key);
            keyValuePair.setValue(newValue);
            return new KeyValuePairCrud().merge(keyValuePair);
        });
    }

    public void delete(KeyValuePair keyValuePair) {
        performTransaction(() -> new KeyValuePairCrud().delete(keyValuePair));
    }
}
