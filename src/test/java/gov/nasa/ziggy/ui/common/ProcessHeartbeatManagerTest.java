package gov.nasa.ziggy.ui.common;

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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.HeartbeatManagerExternalMethods;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.NoHeartbeatException;
import gov.nasa.ziggy.ui.messaging.ConsoleMessageDispatcher;
import gov.nasa.ziggy.ui.mon.master.Indicator;

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

    @Before
    public void setup() {
        System.setProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME, Long.toString(100));
        System.setProperty(PropertyNames.DATABASE_SOFTWARE_PROP_NAME, "postgresql");
        messageHandler = new MessageHandler(new ConsoleMessageDispatcher(null, null, false));
        externalMethods = mock(HeartbeatManagerExternalMethods.class);
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
        System.clearProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME);
        System.clearProperty(PropertyNames.DATABASE_SOFTWARE_PROP_NAME);
    }

    /**
     * Tests a "good start", in which heartbeat messages are coming in and getting handled.
     *
     * @throws NoHeartbeatException
     */
    @Test
    public void testGoodStart() throws InterruptedException, NoHeartbeatException {
        startHeartbeatGenerator();
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods);
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
        startHeartbeatGenerator();
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods);
        manager.initialize();
        Thread.sleep(200);
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.AMBER);
        Thread.sleep(200);
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.AMBER);
        assertFalse(manager.getHeartbeatListener().isShutdown());
    }

    /**
     * Tests a "bad start," in which there are no detected heartbeat messages (achieved here by
     * preventing the heartbeat message generator from starting).
     */
    @Test
    public void testBadStart() {
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods);
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
        messageHandler.setLastHeartbeatTimeMillis(1L);
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods);
        manager.initialize();
        Thread.sleep(305);
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
        manager = new ProcessHeartbeatManager(messageHandler, externalMethods);
        manager.initialize();
        assertFalse(manager.getHeartbeatListener().isShutdown());
        Thread.sleep(210);
        assertEquals(0L, messageHandler.getLastHeartbeatTimeMillis());
        assertFalse(manager.getHeartbeatListener().isShutdown());
        messageHandler.setLastHeartbeatTimeMillis(2L);
        Thread.sleep(110);
        assertNotNull(manager.getHeartbeatListener());
        verify(externalMethods).setRmiIndicator(Indicator.State.AMBER);
        verify(externalMethods, times(0)).setRmiIndicator(Indicator.State.RED);
        verify(externalMethods, times(2)).setRmiIndicator(Indicator.State.GREEN);
        assertFalse(manager.getHeartbeatListener().isShutdown());

    }

    /**
     * Starts the heartbeat generator. After it starts there is a 1 msec sleep because without it
     * the good start unit test errors; apparently there is some latency in starting the heartbeat
     * such that immediately going to tests means that a test can run before any heartbeats are
     * generated and managed, but a 1 msec delay in starting tests avoids this outcome.
     */
    private void startHeartbeatGenerator() throws InterruptedException {
        heartbeatGenerator = new ScheduledThreadPoolExecutor(1);
        heartbeatGenerator.scheduleAtFixedRate(
            () -> new WorkerHeartbeatMessage().handleMessage(messageHandler), 0,
            WorkerHeartbeatMessage.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
        Thread.sleep(1);
    }

}
