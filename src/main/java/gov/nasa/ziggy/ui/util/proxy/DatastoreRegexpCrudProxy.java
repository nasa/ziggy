package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreRegexpCrud;

/**
 * Proxy class for {@link DatastoreRegexpCrud}, used to perform operations of same in the context of
 * the pipeline console.
 *
 * @author Bill Wohler
 */
public class DatastoreRegexpCrudProxy {

    public List<DatastoreRegexp> retrieveAll() {
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new DatastoreRegexpCrud().retrieveAll());
    }
}
