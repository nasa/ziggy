package gov.nasa.ziggy.services.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;

public class KeyValuePairOperationsTest {
    private KeyValuePairOperations keyValuePairOperations = new KeyValuePairOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testPersist() throws Exception {
        keyValuePairOperations.persist(new KeyValuePair("key1", "value1"));

        assertEquals("value1", keyValuePairOperations.keyValuePairValue("key1"));
    }

    @Test
    public void testUpdate() throws Exception {
        keyValuePairOperations.persist(new KeyValuePair("key1", "value1"));
        assertEquals("value1", keyValuePairOperations.keyValuePairValue("key1"));

        keyValuePairOperations.updateKeyValuePair("key1", "value2");
        assertEquals("value2", keyValuePairOperations.keyValuePairValue("key1"));
    }

    @Test
    public void testkeyValuePairs() throws Exception {
        KeyValuePair keyValuePair1 = new KeyValuePair("key1", "value1");
        keyValuePairOperations.persist(keyValuePair1);
        KeyValuePair keyValuePair2 = new KeyValuePair("key2", "value2");
        keyValuePairOperations.persist(keyValuePair2);
        KeyValuePair keyValuePair3 = new KeyValuePair("key3", "value3");
        keyValuePairOperations.persist(keyValuePair3);

        List<KeyValuePair> keyValuePairs = keyValuePairOperations.keyValuePairs();
        assertEquals(3, keyValuePairs.size());
        assertTrue(keyValuePairs.contains(keyValuePair1));
        assertTrue(keyValuePairs.contains(keyValuePair2));
        assertTrue(keyValuePairs.contains(keyValuePair3));
    }
}
