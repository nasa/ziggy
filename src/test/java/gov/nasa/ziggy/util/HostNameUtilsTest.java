package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class HostNameUtilsTest {

    @Test
    public void testHostName() {
        String hostName = HostNameUtils.hostName();
        assertNotNull(hostName);
        assertFalse(hostName.isEmpty());
    }

    @Test
    public void testShortHostName() {
        String hostName = HostNameUtils.shortHostName();
        assertNotNull(hostName);
        assertFalse(hostName.isEmpty());
        assertFalse(hostName.contains("."));
    }

    @Test
    public void testShortHostNameFromHostName() {
        String hostName = HostNameUtils.shortHostNameFromHostName("foo");
        assertNotNull(hostName);
        assertEquals("foo", hostName);

        hostName = HostNameUtils.shortHostNameFromHostName("foo.bar.baz");
        assertNotNull(hostName);
        assertEquals("foo", hostName);
    }

    @Test
    public void testCallerHostNameOrLocalhost() {
        String hostName = HostNameUtils.callerHostNameOrLocalhost("foo");
        assertNotNull(hostName);
        assertEquals("foo", hostName);

        hostName = HostNameUtils.callerHostNameOrLocalhost("foo.bar.baz");
        assertNotNull(hostName);
        assertEquals("foo", hostName);
    }
}
