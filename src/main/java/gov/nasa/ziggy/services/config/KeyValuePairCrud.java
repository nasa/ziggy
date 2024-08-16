package gov.nasa.ziggy.services.config;

import java.util.List;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;

/**
 * Provides CRUD methods for the KeyValuePair entity
 * <p>
 * Note that an updateKeyValuePair() method is not needed! Changes to a persisted object will be
 * updated in the database automatically when the transaction is committed, unless the object is
 * detached.
 *
 * @author Todd Klaus
 */
public class KeyValuePairCrud extends AbstractCrud<KeyValuePair> {
    public List<KeyValuePair> retrieveAll() {
        return list(createZiggyQuery(KeyValuePair.class));
    }

    public String retrieveValue(String key) {
        ZiggyQuery<KeyValuePair, String> query = createZiggyQuery(KeyValuePair.class, String.class);
        query.column(KeyValuePair_.key).in(key);
        query.column(KeyValuePair_.value).select();
        return uniqueResult(query);
    }

    public KeyValuePair retrieve(String key) {
        ZiggyQuery<KeyValuePair, KeyValuePair> query = createZiggyQuery(KeyValuePair.class);
        query.column(KeyValuePair_.key).in(key);
        return uniqueResult(query);
    }

    public void create(KeyValuePair keyValuePair) {
        super.persist(keyValuePair);
    }

    public void delete(KeyValuePair keyValuePair) {
        super.remove(keyValuePair);
    }

    @Override
    public Class<KeyValuePair> componentClass() {
        return KeyValuePair.class;
    }
}
