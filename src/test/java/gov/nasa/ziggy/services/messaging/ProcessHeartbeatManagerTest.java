package gov.nasa.ziggy.services.messaging;

import static gov.nasa.ziggy.services.config.PropertyName.DATABASE_SOFTWARE;
import static gov.nasa.ziggy.services.config.PropertyName.HEARTBEAT_INTERVAL;
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
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.InstrumentedHeartbeatMessage;
import gov.nasa.ziggy.services.messaging.ProcessHeartbeatManager.HeartbeatManagerAssistant;
import gov.nasa.ziggy.services.messaging.ProcessHeartbeatManager.NoHeartbeatException;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.ui.status.Indicator;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Unit tests for {@link ProcessHeartbeatManager} class.
 *
 * @author PT
 */
public class ProcessHeartbeatManagerTest {

    private ProcessHeartbeatManager manager;
    private HeartbeatManagerAssistant assistant;
    private ClusterController clusterController;
    private InstrumentedHeartbeatMessage heartbeatMessage = new InstrumentedHeartbeatMessage();

    @Rule
    public ZiggyPropertyRule heartbeatIntervalPropertyRule = new ZiggyPropertyRule(
        HEARTBEAT_INTERVAL, Long.toString(0));

    @Rule
    public ZiggyPropertyRule databaseSoftwarePropertyRule = new ZiggyPropertyRule(DATABASE_SOFTWARE,
        "postgresql");

    @Before
    public void setup() {
        assistant = Mockito.mock(HeartbeatManagerAssistant.class);
        clusterController = mock(ClusterController.class);
        when(clusterController.isDatabaseAvailable()).thenReturn(true);
        when(clusterController.isSupervisorRunning()).thenReturn(true);
        ProcessHeartbeatManager.setInitializeInThread(false);
    }

    @After
    public void teardown() throws InterruptedException {
        ScheduledThreadPoolExecutor s = manager.getHeartbeatListener();
        if (s != null) {
            s.shutdownNow();
        }
    }

    /**
     * Tests a "good start", in which heartbeat messages are coming in and getting handled.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testGoodStart() throws InterruptedException, NoHeartbeatException {
        manager = new ProcessHeartbeatManager(assistant, clusterController);
        manager.setHeartbeatTime(1L);
        SystemProxy.setUserTime(5L);
        manager.initializeHeartbeatManager();
        assertNotNull(manager.getHeartbeatListener());
        assertFalse(manager.getHeartbeatListener().isShutdown());
        verify(assistant).setRmiIndicator(Indicator.State.NORMAL);
        verify(assistant, times(0)).setRmiIndicator(Indicator.State.ERROR);
    }

    /**
     * Tests good running, in which the heartbeats are regularly detected and responded to.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testGoodRunning() throws InterruptedException, NoHeartbeatException {
        manager = new ProcessHeartbeatManager(assistant, clusterController);

        // Set conditions such that initialization believes that a heartbeat has been detected.
        manager.setHeartbeatTime(1L);
        SystemProxy.setUserTime(5L);
        manager.initializeHeartbeatManager();

        // Send 2 additional heartbeats at later times.
        SystemProxy.setUserTime(105L);
        manager.setHeartbeatTime(105L);
        SystemProxy.setUserTime(205L);
        manager.setHeartbeatTime(205L);
        manager.checkForHeartbeat();
        verify(assistant, times(0)).setRmiIndicator(Indicator.State.WARNING);

        // Send 2 more heartbeats at even later times.
        SystemProxy.setUserTime(305L);
        manager.setHeartbeatTime(305L);
        SystemProxy.setUserTime(405L);
        manager.setHeartbeatTime(405L);
        ZiggyMessenger.publish(heartbeatMessage);
        manager.checkForHeartbeat();
        verify(assistant, times(0)).setRmiIndicator(Indicator.State.WARNING);
        assertFalse(manager.getHeartbeatListener().isShutdown());
    }

    /**
     * Tests a "bad start," in which there are no detected heartbeat messages (achieved here by
     * preventing the heartbeat message generator from starting).
     */
    @Test
    public void testBadStart() {
        manager = new ProcessHeartbeatManager(assistant, clusterController);
        try {
            manager.initializeHeartbeatManager();
        } catch (NoHeartbeatException e) {
            assertNull(manager.getHeartbeatListener());
            verify(assistant).setRmiIndicator(Indicator.State.ERROR);
            verify(assistant, times(0)).setRmiIndicator(Indicator.State.NORMAL);
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

        // Set conditions such that initialization believes that a heartbeat has been detected.
        SystemProxy.setUserTime(5L);
        manager = new ProcessHeartbeatManager(assistant, clusterController);
        manager.setHeartbeatTime(1L);
        manager.initializeHeartbeatManager();
        verify(assistant).setRmiIndicator(Indicator.State.NORMAL);
        manager.checkForHeartbeat();
        assertNotNull(manager.getHeartbeatListener());
        verify(assistant).restartClientCommunicator();
        verify(assistant).setRmiIndicator(Indicator.State.NORMAL);
        verify(assistant).setRmiIndicator(Indicator.State.WARNING);
        verify(assistant).setRmiIndicator(Indicator.State.ERROR);
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
        SystemProxy.setUserTime(5L);
        manager = new ProcessHeartbeatManager(assistant, clusterController);
        manager.setHeartbeatTime(5L);
        manager.initializeHeartbeatManager();

        // Note that we don't want to automatically go to reinitialization. Instead we want to
        // simulate waiting in the initializer for a new heartbeat.
        manager.setReinitializeOnMissedHeartbeat(false);
        assertFalse(manager.getHeartbeatListener().isShutdown());
        SystemProxy.setUserTime(205L);
        manager.checkForHeartbeat();
        assertEquals(0L, manager.getHeartbeatTime());
        assertFalse(manager.getHeartbeatListener().isShutdown());
        SystemProxy.setUserTime(305L);
        manager.setHeartbeatTime(300L);

        // Here is where we simulate detecting the new heartbeat in the initializer.
        manager.initializeHeartbeatManager();
        assertNotNull(manager.getHeartbeatListener());
        verify(assistant).setRmiIndicator(Indicator.State.WARNING);
        verify(assistant, times(0)).setRmiIndicator(Indicator.State.ERROR);
        verify(assistant, times(2)).setRmiIndicator(Indicator.State.NORMAL);
        assertFalse(manager.getHeartbeatListener().isShutdown());
    }
}
