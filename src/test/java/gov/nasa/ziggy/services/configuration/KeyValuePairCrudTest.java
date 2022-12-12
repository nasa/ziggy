package gov.nasa.ziggy.services.configuration;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class KeyValuePairCrudTest {
    private KeyValuePairCrud keyValuePairCrud;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        keyValuePairCrud = new KeyValuePairCrud();
    }

    @Test
    public void testCreate() throws Exception {
        String value = null;

        // store
        DatabaseTransactionFactory.performTransaction(() -> {
            keyValuePairCrud.create(new KeyValuePair("key1", "value1"));
            return null;
        });

        // retrieve
        value = (String) DatabaseTransactionFactory.performTransaction(() -> {
            return keyValuePairCrud.retrieveValue("key1");
        });

        assertEquals("value", "value1", value);
    }

    @Test
    public void testUpdate() throws Exception {
        String value = null;

        // store
        DatabaseTransactionFactory.performTransaction(() -> {
            keyValuePairCrud.create(new KeyValuePair("key1", "value1"));
            return null;
        });

        // retrieve & update
        DatabaseTransactionFactory.performTransaction(() -> {
            KeyValuePair kvp = keyValuePairCrud.retrieve("key1");
            kvp.setValue("value2");
            return null;
        });

        // retrieve
        value = (String) DatabaseTransactionFactory.performTransaction(() -> {
            return keyValuePairCrud.retrieveValue("key1");
        });

        assertEquals("value", "value2", value);
    }
}
