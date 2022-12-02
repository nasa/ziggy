package gov.nasa.ziggy.module.remote.nas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import gov.nasa.ziggy.module.remote.RemoteExecutionProperties;

/**
 * Tests the {@link RemoteExecutionProperties} and {@link NasProperties} classes.
 *
 * @author PT
 */
public class RemoteExecutionPropertiesTest {

    @After
    public void teardown() {
        System.clearProperty(RemoteExecutionProperties.GROUP_PROPERTY);
        System.clearProperty(RemoteExecutionProperties.HOST_PROPERTY);
        System.clearProperty(RemoteExecutionProperties.USER_PROPERTY);
    }

    @Test
    public void testPropertiesRetrieval() {

        // Start with tests that involve setting the value and retrieving it
        System.setProperty(RemoteExecutionProperties.HOST_PROPERTY, "h1;h2");
        System.setProperty(RemoteExecutionProperties.USER_PROPERTY, "u1");
        System.setProperty(RemoteExecutionProperties.GROUP_PROPERTY, "g1");

        assertTrue(RemoteExecutionProperties.getUser().equals("u1"));
        assertTrue(RemoteExecutionProperties.getGroup().equals("g1"));
        String[] hosts = RemoteExecutionProperties.getHost();
        assertEquals(2, hosts.length);
        assertTrue(hosts[0].equals("h1"));
        assertTrue(hosts[1].equals("h2"));
        System.clearProperty(RemoteExecutionProperties.GROUP_PROPERTY);
        System.clearProperty(RemoteExecutionProperties.HOST_PROPERTY);
        System.clearProperty(RemoteExecutionProperties.USER_PROPERTY);

        assertTrue(RemoteExecutionProperties.getUser().isEmpty());
        assertTrue(RemoteExecutionProperties.getGroup().isEmpty());
        assertEquals(0, RemoteExecutionProperties.getHost().length);
    }
}
