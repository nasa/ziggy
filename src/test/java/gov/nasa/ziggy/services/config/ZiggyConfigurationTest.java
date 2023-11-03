package gov.nasa.ziggy.services.config;

import static gov.nasa.ziggy.services.config.PropertyName.ALLOW_PARTIAL_TASKS;
import static gov.nasa.ziggy.services.config.PropertyName.DATABASE_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;

public class ZiggyConfigurationTest {

    /**
     * The value of the {@link PropertyName#TEST_FILE} property in $PIPELINE_CONFIG_PATH.
     */
    private static final String PIPELINE_CONFIG_PATH_VALUE = "from.envvar.location";

    /** The value of the {@link PropertyName#TEST_FILE} property in ziggy.properties */
    private static final String ZIGGY_PROPERTIES_VALUE = "from.default.location";

    /** A property that should exist in the properties file. */
    private static final String EXISTING_PROPERTY = "some.existent.property";

    /** A property that should not exist in the properties file. */
    private static final String NONEXISTENT_PROPERTY = "some.non-existent.property";

    /** The number of threads accessing a property that exists. */
    private static final int NUM_EXISTING_PROPERTY_READERS = 4;

    /** The number of threads accessing a property that does not exist. */
    private static final int NUM_NONEXISTENT_PROPERTY_READERS = 4;

    // DATABASE_PORT is a random property that normally takes numbers.
    @Rule
    public ZiggyPropertyRule fooPropertyRule = new ZiggyPropertyRule(DATABASE_PORT, "1");

    private static final String NUMERIC_PROPERTY = DATABASE_PORT.property();

    // ALLOW_PARTIAL_TASKS is a random property that normally takes booleans.
    @Rule
    public ZiggyPropertyRule barPropertyRule = new ZiggyPropertyRule(ALLOW_PARTIAL_TASKS, "true");

    private static final String BOOLEAN_PROPERTY = ALLOW_PARTIAL_TASKS.property();

    @Test
    public void testGetInstance() {
        ImmutableConfiguration configuration1 = ZiggyConfiguration.getInstance();
        ImmutableConfiguration configuration2 = ZiggyConfiguration.getInstance();
        assertNotNull(configuration1);
        assertTrue(configuration1 == configuration2);

        assertEquals(BigDecimal.ONE, configuration1.getBigDecimal(NUMERIC_PROPERTY));
        assertEquals(BigInteger.ONE, configuration1.getBigInteger(NUMERIC_PROPERTY));
        assertEquals(Byte.parseByte("1"), configuration1.getByte(NUMERIC_PROPERTY));
        assertEquals(1.0, configuration1.getDouble(NUMERIC_PROPERTY), 0.0001);
        assertEquals(1.0F, configuration1.getFloat(NUMERIC_PROPERTY), 0.0001);
        assertEquals(1, configuration1.getInt(NUMERIC_PROPERTY));
        assertEquals(1L, configuration1.getLong(NUMERIC_PROPERTY));

        assertTrue(configuration1.getBoolean(BOOLEAN_PROPERTY));
    }

    @Test
    public void testGetMutableInstance() {
        ImmutableConfiguration configuration1 = ZiggyConfiguration.getMutableInstance();
        ImmutableConfiguration configuration2 = ZiggyConfiguration.getMutableInstance();
        assertNotNull(configuration1);
        assertTrue(configuration1 == configuration2);
    }

    @Test
    public void testInterpolate() {
        assertEquals("foo 1 bar",
            ZiggyConfiguration.interpolate("foo ${" + NUMERIC_PROPERTY + "} bar"));
    }

    @Test
    public void testReset() {
        ImmutableConfiguration configuration1 = ZiggyConfiguration.getInstance();
        ImmutableConfiguration configuration2 = ZiggyConfiguration.getMutableInstance();
        assertTrue(configuration1 != configuration2);

        ZiggyConfiguration.reset();

        ImmutableConfiguration configuration3 = ZiggyConfiguration.getInstance();
        ImmutableConfiguration configuration4 = ZiggyConfiguration.getMutableInstance();
        assertTrue(configuration1 != configuration3);
        assertTrue(configuration2 != configuration4);
        assertTrue(configuration3 != configuration4);
    }

    @Test
    public void testConfigurationThrowIfMissing() {
        ImmutableConfiguration configuration = ZiggyConfiguration.getInstance();

        try {
            configuration.getBigDecimal("does-not-exist");
            fail("Expected a NoSuchElementException");
        } catch (NoSuchElementException expected) {
        }
    }

    // The following tests were previously in ConfigurationServiceFactoryTest.

    @Test
    public void testSystemProperty() {
        System.setProperty("my.string.property", "foo");
        System.setProperty("my.boolean.property", "true");
        System.setProperty("my.int.property", "42");
        System.setProperty("my.double.property", "42.42");

        // Force getInstance() to read from system properties.
        ZiggyConfiguration.reset();
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        assertEquals("foo", config.getString("my.string.property"));
        assertEquals(true, config.getBoolean("my.boolean.property"));
        assertEquals(42, config.getInt("my.int.property"));
        assertEquals(42.42, config.getDouble("my.double.property"), 0);

        System.clearProperty("my.string.property");
        System.clearProperty("my.boolean.property");
        System.clearProperty("my.int.property");
        System.clearProperty("my.double.property");
    }

    @Test
    public void testFilePropertyOverride() {
    }

    @Test
    public void testSystemPropertyOverride() {
    }

    @Test
    public void testDefaultFileProperty() {
        // Force getInstance() to read from ziggy.properties.
        ZiggyConfiguration.reset();
        ZiggyConfiguration.getMutableInstance();

        assertEquals(ZIGGY_PROPERTIES_VALUE,
            ZiggyConfiguration.getInstance().getString(PropertyName.TEST_FILE.property()));
    }

    // TODO: can't enable this test because there's no way to set an env var for the current
    // process. Maybe launch a sub-process?
    // @Test
    public void testEnvVarFileProperty() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        assertEquals(PIPELINE_CONFIG_PATH_VALUE,
            config.getString(PropertyName.TEST_FILE.property()));
    }

    /**
     * Implements tests of concurrent access to configuration properties.
     */
    @Test
    public void testConcurrentAccess() throws InterruptedException {
        List<PropertyAccessThread> threads = new ArrayList<>();

        for (int i = 0; i < NUM_EXISTING_PROPERTY_READERS; ++i) {
            threads.add(new PropertyAccessThread(EXISTING_PROPERTY, 12, 150));
        }
        for (int i = 0; i < NUM_NONEXISTENT_PROPERTY_READERS; ++i) {
            threads.add(new PropertyAccessThread(NONEXISTENT_PROPERTY, 42, 250));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (PropertyAccessThread thread : threads) {
            if (thread.isPropertyDiffered()) {
                fail("Property differed in at least one thread");
            }
        }
    }

    @Test
    public void testBuildProperty() {
        assertNotNull(
            ZiggyConfiguration.getInstance().getString(PropertyName.ZIGGY_VERSION.property()));
    }

    @Test
    public void testLogJvmProperties() {
        // Just ensure that the code is covered and doesn't blow up. If the log output can be easily
        // checked, do it.
        ZiggyConfiguration.logJvmProperties();
    }

    private static class PropertyAccessThread extends Thread {

        private String propertyName;
        private int defaultValue;
        private long duration;
        private boolean propertyDiffered;

        /**
         * Creates a new instance of a thread that continually accesses a property from the
         * configuration until a time duration has elapsed. Also records whether the property
         * differed during the test.
         *
         * @param propertyName the property name to access
         * @param defaultValue a default value for the property if not present
         * @param duration the length of the access test in milliseconds
         */
        public PropertyAccessThread(String propertyName, int defaultValue, long duration) {
            this.propertyName = propertyName;
            this.defaultValue = defaultValue;
            this.duration = duration;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            propertyDiffered = false;

            Configuration config = ZiggyConfiguration.getMutableInstance();
            int lastValue = config.getInt(propertyName, defaultValue);
            long accesses = 1;

            for (;;) {
                long now = System.currentTimeMillis();
                if (now - start > duration) {
                    break;
                }

                try {
                    int newValue = config.getInt(propertyName, defaultValue);
                    config.setProperty(propertyName, newValue);

                    propertyDiffered |= newValue != lastValue;
                    lastValue = newValue;
                } catch (Throwable ex) {
                    System.err.println("Error accessing property: " + ex.getMessage());
                }

                ++accesses;
            }

            System.out.println(String.format("Thread=%s property=%s accesses=%d", getName(),
                propertyName, accesses));
        }

        /**
         * Tests whether the property differed in value during the test.
         *
         * @return true, if the property value differed at any time during the test
         */
        public boolean isPropertyDiffered() {
            return propertyDiffered;
        }
    }
}
