package gov.nasa.ziggy.services.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

public class ConfigurationServiceFactoryTest {

    /** A property that should exist in the properties file. */
    private static final String EXISTING_PROPERTY = "pi.worker.numTaskThreads";

    /** A property that should not exist in the properties file. */
    private static final String NONEXISTENT_PROPERTY = "pi.non.existent.property";

    /** The number of threads accessing a property that exists. */
    private static final int NUM_EXISTING_PROPERTY_READERS = 4;

    /** The number of threads accessing a property that does not exist. */
    private static final int NUM_NONEXISTENT_PROPERTY_READERS = 4;

    @Before
    public void before() throws PipelineException {
        ZiggyConfiguration.reset();
    }

    @Test
    public void testSystemProperty() throws PipelineException {
        System.setProperty("my.string.property", "foo");
        System.setProperty("my.boolean.property", "true");
        System.setProperty("my.int.property", "42");
        System.setProperty("my.double.property", "42.42");

        Configuration config = ZiggyConfiguration.getInstance();

        assertEquals("foo", config.getString("my.string.property"));
        assertEquals(true, config.getBoolean("my.boolean.property"));
        assertEquals(42, config.getInt("my.int.property"));
        assertEquals(42.42, config.getDouble("my.double.property"), 0);
    }

    @Test
    public void testFilePropertyOverride() throws PipelineException {
    }

    @Test
    public void testSystemPropertyOverride() throws PipelineException {
    }

    @Test
    public void testDefaultFileProperty() throws PipelineException {
        Configuration config = ZiggyConfiguration.getInstance();

        assertEquals("from.default.location", config.getString("test.file.property"));
    }

    // TODO: can't enable this test because there's no way to set an env var for the current
    // process. Maybe launch a sub-process?
    // @Test
    public void testEnvVarFileProperty() throws PipelineException {
        Configuration config = ZiggyConfiguration.getInstance();

        assertEquals("from.envvar.location", config.getString("test.file.property"));
    }

    /**
     * Implements tests of concurrent access to configuration properties.
     */
    @Test
    public void testConcurrentAccess() throws InterruptedException {
        List<PropertyAccessThread> threads = new ArrayList<>();

        for (int i = 0; i < NUM_EXISTING_PROPERTY_READERS; ++i) {
            threads.add(new PropertyAccessThread(EXISTING_PROPERTY, 12, 4000));
        }
        for (int i = 0; i < NUM_NONEXISTENT_PROPERTY_READERS; ++i) {
            threads.add(new PropertyAccessThread(NONEXISTENT_PROPERTY, 42, 4000));
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

            Configuration config = ZiggyConfiguration.getInstance();
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
