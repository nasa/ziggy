package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ClientSideMessageHandlerForTest;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ConsoleMessageDispatcherForTest;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.InstrumentedWorkerHeartbeatManager;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager;
import gov.nasa.ziggy.util.os.ProcessUtils;

/**
 * Tests the RMI communication classes for Ziggy in the context where the client and server sides
 * (or UI and worker, if you prefer) are running in the different processes and thus different JVMs.
 * The server is always run as the external process.
 * <P>
 * The inter- and intra-process tests are in separate classes because of failures that occur when an
 * inter-process test immediately follows an intra-process test.
 *
 * @author PT
 */
public class RmiInterProcessCommunicationTest {

    private int port = 4788;
    private Process serverProcess;
    String heartbeatIntervalMillis = "100";
    private ProcessHeartbeatManager heartbeatManager = mock(ProcessHeartbeatManager.class);

    @Before
    public void setup() {
        System.setProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME, heartbeatIntervalMillis);
        serverProcess = null;
    }

    @After
    public void teardown()
        throws AccessException, RemoteException, NotBoundException, InterruptedException {

        UiCommunicator.stopHeartbeatListener();
        if (WorkerCommunicator.isInitialized() && WorkerCommunicator.getRegistry() != null) {
            WorkerCommunicator.shutdown();
        }
        System.clearProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME);
        UiCommunicator.reset();
        if (serverProcess != null) {
            serverProcess.destroy();
            serverProcess = null;
        }
    }

    /**
     * Tests communication when the worker and UI are in separate JVMs.
     */
    @Test
    public void testInterProcessCommunication() throws IOException, InterruptedException {

        // Start the worker in a remote process
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(port));
        args.add(Integer.toString(2));
        args.add("false");
        args.add(heartbeatIntervalMillis);

        serverProcess = ProcessUtils.runJava(ServerTest.class, args);
        Thread.sleep(1000L);

        // now start the UiCommunicator with a ClientMessageHandler

        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(new ClientSideMessageHandlerForTest(), port);
        UiCommunicator.stopHeartbeatListener();
        Registry registry = UiCommunicator.getRegistry();
        assertNotNull(registry);
        assertNotNull(UiCommunicator.getServerMessageHandlerStub());
        assertNotNull(UiCommunicator.getWorkerService());

        // send two messages
        MessageFromClient m1 = new MessageFromClient("telecaster");
        UiCommunicator.send(m1);
        MessageFromClient m2 = new MessageFromClient("stratocaster");
        UiCommunicator.send(m2);

        Thread.sleep(50);

        ClientSideMessageHandlerForTest messageHandler = (ClientSideMessageHandlerForTest) UiCommunicator
            .getMessageHandler();
        Set<MessageFromServer> messagesFromServer = messageHandler.getMessagesFromServer();
        assertEquals(1, messagesFromServer.size());
    }

    /**
     * Tests heartbeat generation, detection, and handling for the inter-process use-case.
     */
    @Test
    public void testHeartbeatManagement() throws InterruptedException, IOException {

        // Start the worker in a remote process
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(port));
        args.add(Integer.toString(2));
        args.add("false");
        args.add(heartbeatIntervalMillis);

        serverProcess = ProcessUtils.runJava(ServerTest.class, args);
        Thread.sleep(1000L);

        // Start the heartbeat manager and communicator
        MessageHandler messageHandler = new MessageHandler(
            new ConsoleMessageDispatcherForTest(null, null, false));
        InstrumentedWorkerHeartbeatManager h = new InstrumentedWorkerHeartbeatManager(
            messageHandler);
        UiCommunicator.setHeartbeatManager(h);
        UiCommunicator.initializeInstance(messageHandler, port);

        // Pause to let 5 heartbeat-check intervals go by
        Thread.sleep(1100);
        UiCommunicator.stopHeartbeatListener();

        serverProcess.destroy();
        serverProcess = null;

        // Start checking to see what the heartbeat handler did:
        // there should be only 1 start (i.e., no restarts)
        List<Long> messageHandlerStartTimes = h.getMessageHandlerStartTimes();
        List<Long> localStartTimes = h.getLocalStartTimes();
        assertEquals(1, messageHandlerStartTimes.size());
        assertEquals(1, localStartTimes.size());
        assertTrue(messageHandlerStartTimes.get(0) > 0);
        assertEquals(messageHandlerStartTimes.get(0), localStartTimes.get(0));

        // There should be 5 checks after starting, all good
        List<Boolean> checkStatus = h.getCheckStatus();
        assertEquals(5, checkStatus.size());
        for (Boolean status : checkStatus) {
            assertTrue(status);
        }

        // The check times should be within errors of 200 msec apart, and the
        // manager's last detection time should be set to the message handler's
        // last detection time from the preceding detection
        //
        // Note: the 1st and 2nd local heartbeat times can be significantly
        // closer in time than 200 msec because the worker sends the UI a
        // heartbeat immediately when the UI starts up, which can be at any
        // time in the normal heartbeat cycle
        List<Long> messageHandlerHeartbeatTimesAtChecks = h
            .getMessageHandlerHeartbeatTimesAtChecks();
        List<Long> localTimesAtChecks = h.getlocalHeartbeatTimesAtChecks();
        assertEquals(5, messageHandlerHeartbeatTimesAtChecks.size());
        assertEquals(5, localTimesAtChecks.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(messageHandlerHeartbeatTimesAtChecks.get(i),
                localTimesAtChecks.get(i + 1));
            assertTrue(messageHandlerHeartbeatTimesAtChecks.get(i + 1)
                - messageHandlerHeartbeatTimesAtChecks.get(i) > 190L);
            assertTrue(messageHandlerHeartbeatTimesAtChecks.get(i + 1)
                - messageHandlerHeartbeatTimesAtChecks.get(i) < 210L);
            assertTrue(localTimesAtChecks.get(i + 1) - localTimesAtChecks.get(i) < 210L);
        }
    }

}
