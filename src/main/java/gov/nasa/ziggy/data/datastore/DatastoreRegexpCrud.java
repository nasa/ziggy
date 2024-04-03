package gov.nasa.ziggy.data.datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.crud.AbstractCrud;

public class DatastoreRegexpCrud extends AbstractCrud<DatastoreRegexp> {

    public DatastoreRegexp retrieve(String name) {
        return uniqueResult(
            createZiggyQuery(DatastoreRegexp.class).column(DatastoreRegexp_.name).in(name));
    }

    public List<DatastoreRegexp> retrieveAll() {
        return list(createZiggyQuery(DatastoreRegexp.class));
    }

    public Map<String, DatastoreRegexp> retrieveRegexpsByName() {
        Map<String, DatastoreRegexp> regexpByName = new HashMap<>();
        List<DatastoreRegexp> regexps = list(createZiggyQuery(DatastoreRegexp.class));
        for (DatastoreRegexp regexp : regexps) {
            regexpByName.put(regexp.getName(), regexp);
        }
        return regexpByName;
    }

    public List<String> retrieveRegexpNames() {
        return list(
            createZiggyQuery(DatastoreRegexp.class, String.class).column(DatastoreRegexp_.name)
                .select());
    }

    @Override
    public Class<DatastoreRegexp> componentClass() {
        return DatastoreRegexp.class;
    }
}
