package gov.nasa.ziggy.services.config;

import java.util.List;

import org.hibernate.Query;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for the KeyValuePair entity
 * <p>
 * Note that an updateKeyValuePair() method is not needed! Changes to a persisted object will be
 * updated in the database automatically when the transaction is commited, unless the object is
 * detached.
 *
 * @author Todd Klaus
 */
public class KeyValuePairCrud extends AbstractCrud {
    protected DatabaseService databaseService = null;

    /**
     * Used only for mocking.
     */
    public KeyValuePairCrud() {
        this(DatabaseService.getInstance());
    }

    public KeyValuePairCrud(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    public List<KeyValuePair> retrieveAll() {
        List<KeyValuePair> results = list(
            databaseService.getSession().createQuery("from KeyValuePair"));
        return results;
    }

    public String retrieveValue(String key) {
        Query query = databaseService.getSession()
            .createQuery("from KeyValuePair where key = :key");
        query.setString("key", key);
        KeyValuePair keyValuePair = uniqueResult(query);
        return keyValuePair.getValue();
    }

    public KeyValuePair retrieve(String key) {
        Query query = databaseService.getSession()
            .createQuery("from KeyValuePair where key = :key");
        query.setString("key", key);
        KeyValuePair keyValuePair = uniqueResult(query);
        return keyValuePair;
    }

    public void create(KeyValuePair keyValuePair) {
        super.create(keyValuePair);
    }

    public void delete(KeyValuePair keyValuePair) {
        super.delete(keyValuePair);
    }
}
