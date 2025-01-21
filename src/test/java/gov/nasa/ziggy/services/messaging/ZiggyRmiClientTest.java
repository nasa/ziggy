package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Implements unit tests of {@link ZiggyRmiClient}.
 */
public class ZiggyRmiClientTest {

    private static final String CLIENT_TYPE = "the_client_type";

    @Rule
    public ZiggyPropertyRule rmiServerHostnameProperty = new ZiggyPropertyRule(
        PropertyName.JAVA_RMI_SERVER_HOSTNAME, "localhost");

    @Test
    public void testClientStartStop() {
        try {
            ZiggyRmiServer.start();
            TestEventDetector.detectTestEvent(1000L, () -> ZiggyRmiServer.isInitialized());

            ZiggyRmiClient.start(CLIENT_TYPE);
            TestEventDetector.detectTestEvent(1000L, () -> ZiggyRmiClient.isInitialized());
            assertEquals(CLIENT_TYPE, ZiggyRmiClient.getClientType());

            ZiggyRmiClient.reset();
            assertFalse(ZiggyRmiClient.isInitialized());
        } finally {
            ZiggyRmiServer.shutdown();
        }
    }
}
