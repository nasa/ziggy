package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Implements unit tests for {@link ZiggyRmiServer}.
 */
public class ZiggyRmiServerTest {

    @Rule
    public ZiggyPropertyRule rmiServerHostnameProperty = new ZiggyPropertyRule(
        PropertyName.JAVA_RMI_SERVER_HOSTNAME, "localhost");

    @Test
    public void testServerStartStop() {
        ZiggyRmiServer.start();
        TestEventDetector.detectTestEvent(1000L, () -> ZiggyRmiServer.isInitialized());
        assertNotNull(ZiggyRmiServer.serverInstance().getBroadcastThread());

        ZiggyRmiServer.shutdown();
        TestEventDetector.detectTestEvent(1000L, () -> !ZiggyRmiServer.isInitialized());
    }

    /**
     * Tests that if the RMI server is started twice, the same instance is used.
     */
    @Test
    public void testServerStartTwice() {
        ZiggyRmiServer.start();
        TestEventDetector.detectTestEvent(1000L, () -> ZiggyRmiServer.isInitialized());
        ZiggyRmiServer instance = ZiggyRmiServer.serverInstance();

        ZiggyRmiServer.start();
        assertSame(instance, ZiggyRmiServer.serverInstance());

        ZiggyRmiServer.shutdown();
        TestEventDetector.detectTestEvent(1000L, () -> !ZiggyRmiServer.isInitialized());
    }
}
