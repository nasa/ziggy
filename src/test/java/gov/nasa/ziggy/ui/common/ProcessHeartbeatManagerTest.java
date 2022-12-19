package gov.nasa.ziggy.ui.common;

import static gov.nasa.ziggy.services.config.PropertyNames.DATABASE_SOFTWARE_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.HeartbeatManagerExternalMethods;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.NoHeartbeatException;
import gov.nasa.ziggy.ui.messaging.ConsoleMessageDispatcher;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.util.SystemTime;

/**
 * Unit tests for {@link ProcessHeartbeatManager} class.
 *
 * @author PT
 */
public class ProcessHeartbeatManagerTest {

    private ProcessHeartbeatManager manager;
    private MessageHandler messageHandler;
    private ScheduledThreadPoolExecutor heartbeatGenerator;
    private HeartbeatManagerExternalMethods externalMethods;
    private ClusterController clusterController;

    @Rule
    public ZiggyPropertyRule heartbeatIntervalPropertyRule = new ZiggyPropertyRule(
        HEARTBEAT_INTERVAL_PROP_NAME, Long.toString(0));

    @Rule
    public ZiggyPropertyRule databaseSoftwarePropertyRule = new ZiggyPropertyRule(
        DATABASE_SOFTWARE_PROP_NAME, "postgresql");

    @Before
    public void setup() {
        messageHandler = new MessageHandler(new ConsoleMessageDispatcher(null, null, false));
        externalMethods = mock(HeartbeatManagerExternalMethods.class);
        clusterController = mock(ClusterController.class);
        when(clusterController.isDatabaseRunning()).thenReturn(true);
        when(clusterController.isWorkerRunning()).thenReturn(true);
    }

    @After
    public void teardown() throws InterruptedException {
        ScheduledThreadPoolExecutor s = manager.getHeartbeatListener();
        if (s != null) {
            s.shutdownNow();
        }
        if (heartbeatGenerator != null) {
            heartbeatGenerator.shutdownNow();
            heartbeatGenerator = null;
        }
    }

    /**
     * Tests a "good start", in which heartbeat messages are coming in and getting handled.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testGoodStart() throws InterruptedException, NoHeartbeatException {
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods, clusterController);
        messageHandler.setLastHeartbeatTimeMillis(1L);
        SystemTime.setUserTime(5L);
        manager.initialize();
        assertNotNull(manager.getHeartbeatListener());
        assertFalse(manager.getHeartbeatListener().isShutdown());
        verify(externalMethods).setRmiIndicator(Indicator.State.GREEN);
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.RED);
    }

    /**
     * Tests good running, in which the heartbeats are regularly detected and responded to.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testGoodRunning() throws InterruptedException, NoHeartbeatException {
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods, clusterController);

        // Set conditions such that initialization believes that a heartbeat has been detected.
        messageHandler.setLastHeartbeatTimeMillis(1L);
        SystemTime.setUserTime(5L);
        manager.initialize();

        // Send 2 additional heartbeats at later times.
        SystemTime.setUserTime(105L);
        sendHeartbeat();
        SystemTime.setUserTime(205L);
        sendHeartbeat();
        manager.checkForHeartbeat();
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.AMBER);

        // Send 2 more heartbeats at even later times.
        SystemTime.setUserTime(305L);
        sendHeartbeat();
        SystemTime.setUserTime(405L);
        sendHeartbeat();
        manager.checkForHeartbeat();
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.AMBER);
        assertFalse(manager.getHeartbeatListener().isShutdown());
    }

    /**
     * Tests a "bad start," in which there are no detected heartbeat messages (achieved here by
     * preventing the heartbeat message generator from starting).
     */
    @Test
    public void testBadStart() {
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods, clusterController);
        try {
            manager.initialize();
            assertFalse(true);
        } catch (NoHeartbeatException e) {
            assertNull(manager.getHeartbeatListener());
            verify(externalMethods).setRmiIndicator(Indicator.State.RED);
            verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.GREEN);
        }
    }

    /**
     * Tests the case in which the heartbeat detector hears nothing. To achieve this, the heartbeat
     * generator is deliberately left switched off, but the message handler's "last heartbeat" time
     * is set to something non-zero.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testHeartbeatDetectorHearsNothing()
        throws InterruptedException, NoHeartbeatException {
        when(externalMethods.getProcessIdiotLightState()).thenReturn(Indicator.State.RED);

        // Set conditions such that initialization believes that a heartbeat has been detected.
        messageHandler.setLastHeartbeatTimeMillis(1L);
        SystemTime.setUserTime(5L);
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods, clusterController);
        manager.initialize();
        try {
            manager.checkForHeartbeat();
            assertFalse("NoHeartbeatException not detected", true);
        } catch (NoHeartbeatException e) {
        }
        assertNotNull(manager.getHeartbeatListener());
        verify(externalMethods).restartUiCommunicator();
        verify(externalMethods).setRmiIndicator(Indicator.State.GREEN);
        verify(externalMethods).setRmiIndicator(Indicator.State.AMBER);
        verify(externalMethods).setRmiIndicator(Indicator.State.RED);
        assertTrue(manager.getHeartbeatListener().isShutdown());
    }

    /**
     * Tests the case in which the heartbeat messages are interrupted but then return before the
     * restart attempt is completed.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testHeartbeatDetectorSuccessfulRestart()
        throws InterruptedException, NoHeartbeatException {
        when(externalMethods.getProcessIdiotLightState()).thenReturn(Indicator.State.GREEN);
        messageHandler.setLastHeartbeatTimeMillis(1L);
        SystemTime.setUserTime(5L);
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods, clusterController);
        manager.initialize();

        // Note that we don't want to automatically go to reinitialization. Instead we want to
        // simulate waiting in the initializer for a new heartbeat.
        manager.setReinitializeOnMissedHeartbeat(false);
        assertFalse(manager.getHeartbeatListener().isShutdown());
        SystemTime.setUserTime(205L);
        manager.checkForHeartbeat();
        assertEquals(0L, messageHandler.getLastHeartbeatTimeMillis());
        assertFalse(manager.getHeartbeatListener().isShutdown());
        SystemTime.setUserTime(305L);
        messageHandler.setLastHeartbeatTimeMillis(300L);

        // Here is where we simulate detecting the new heartbeat in the initializer.
        manager.initialize();
        assertNotNull(manager.getHeartbeatListener());
        verify(externalMethods).setRmiIndicator(Indicator.State.AMBER);
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.RED);
        verify(externalMethods, times(2)).setRmiIndicator(Indicator.State.GREEN);
        assertFalse(manager.getHeartbeatListener().isShutdown());
    }

    private void sendHeartbeat() {
        new WorkerHeartbeatMessage().handleMessage(messageHandler);
    }
}
