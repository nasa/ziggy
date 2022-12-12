package gov.nasa.ziggy.services.configuration;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class KeyValuePairCrudTest {
    private KeyValuePairCrud keyValuePairCrud;

    @Before
    public void setUp() {
        ZiggyUnitTestUtils.setUpDatabase();
        keyValuePairCrud = new KeyValuePairCrud();
    }

    @After
    public void tearDown() {
        ZiggyUnitTestUtils.tearDownDatabase();
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
