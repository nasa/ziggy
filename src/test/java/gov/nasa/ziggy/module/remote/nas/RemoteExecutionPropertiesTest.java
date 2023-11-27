package gov.nasa.ziggy.module.remote.nas;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_GROUP;
import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_HOST;
import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_USER;
import static gov.nasa.ziggy.services.config.PropertyName.TEST_ENVIRONMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.RemoteExecutionProperties;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Tests the {@link RemoteExecutionProperties} and {@link NasProperties} classes.
 *
 * @author PT
 */
public class RemoteExecutionPropertiesTest {

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule(REMOTE_GROUP, "g1");

    @Rule
    public ZiggyPropertyRule hostPropertyRule = new ZiggyPropertyRule(REMOTE_HOST, "h1;h2");

    @Rule
    public ZiggyPropertyRule userPropertyRule = new ZiggyPropertyRule(REMOTE_USER, "u1");

    @Rule
    public ZiggyPropertyRule testEnvRule = new ZiggyPropertyRule(TEST_ENVIRONMENT, "true");

    @Test
    public void testPropertiesRetrieval() {

        assertTrue(RemoteExecutionProperties.getUser().equals("u1"));
        assertTrue(RemoteExecutionProperties.getGroup().equals("g1"));
        String[] hosts = RemoteExecutionProperties.getHost();
        assertEquals(2, hosts.length);
        assertTrue(hosts[0].equals("h1"));
        assertTrue(hosts[1].equals("h2"));
    }

    @Test
    public void testEmptyPropertiesRetrieval() {

        // This clears properties set by rules, and ensures that ZiggyConfiguration doesn't read the
        // user's property file.
        ZiggyConfiguration.reset();
        ZiggyConfiguration.getMutableInstance();

        assertTrue(RemoteExecutionProperties.getUser().isEmpty());
        assertTrue(RemoteExecutionProperties.getGroup().isEmpty());
        assertEquals(0, RemoteExecutionProperties.getHost().length);
    }
}
