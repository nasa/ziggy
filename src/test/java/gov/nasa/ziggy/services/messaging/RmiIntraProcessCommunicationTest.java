package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ClientSideMessageHandlerForTest;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.InstrumentedWorkerHeartbeatManager;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ServerSideMessageHandlerForTest;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager;
import gov.nasa.ziggy.util.SystemTime;

/**
 * Tests the RMI communication classes for Ziggy in the context where the client and server sides
 * (or UI and worker, if you prefer) are running in the same process and thus the same JVM.
 * <P>
 * Intra-process communication isn't actually a use-case for the production Ziggy systems, but it
 * simplifies some debugging tasks and thus these tests are provided. Also, there seem to be
 * problems when the intra- and inter-process tests are in the same class (specifically, an
 * inter-process test will always fail if immediately preceded by an intra-process test). Moving
 * them to separate classes seems to eliminate this problem, which suggests that the intra-process
 * tests are leaving some state configuration behind that is breaking the inter-process tests.
 *
 * @author PT
 */
public class RmiIntraProcessCommunicationTest {

    private ServerSideMessageHandlerForTest messageHandler1;
    private ClientSideMessageHandlerForTest messageHandler2;
    private int port = 4788;
    private Registry registry;
    private ProcessHeartbeatManager heartbeatManager = mock(ProcessHeartbeatManager.class);

    @Before
    public void setup() {
        messageHandler1 = new ServerSideMessageHandlerForTest();
        messageHandler2 = new ClientSideMessageHandlerForTest();
        registry = null;
    }

    @After
    public void teardown()
        throws AccessException, RemoteException, NotBoundException, InterruptedException {

        UiCommunicator.stopHeartbeatListener();
        if (WorkerCommunicator.isInitialized() && WorkerCommunicator.getRegistry() != null) {
            WorkerCommunicator.shutdown();
        }
        if (registry != null) {
            UnicastRemoteObject.unexportObject(registry, true);
        }
        System.clearProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME);
        UiCommunicator.reset();
        messageHandler1 = null;
        messageHandler2 = null;
    }

    /**
     * Tests basic initialization of the two classes.
     *
     * @throws InterruptedException
     */
    @Test
    public void testInitialize()
        throws AccessException, RemoteException, NotBoundException, InterruptedException {

        WorkerCommunicator.initializeInstance(messageHandler1, port);
        Set<MessageHandlerService> clientStubs = WorkerCommunicator.getClientMessageServiceStubs();
        assertTrue(clientStubs.isEmpty());
        assertEquals(messageHandler1, WorkerCommunicator.getMessageHandler());
        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(messageHandler2, port);

        clientStubs = WorkerCommunicator.getClientMessageServiceStubs();
        assertFalse(clientStubs.isEmpty());
        assertEquals(messageHandler2, UiCommunicator.getMessageHandler());
    }

    /**
     * Tests the case in which the worker crashes and a new WorkerCommunicator needs to be
     * instantiated.
     *
     * @throws InterruptedException
     */
    @Test
    public void testReinitializeWorker() throws InterruptedException {

        WorkerCommunicator.initializeInstance(messageHandler1, port);

        // Note that for this test we need to preserve a reference to the registry from
        // the WorkerCommunicator instance that started it; this will be used to shut down
        // the registry when the test completes.
        registry = WorkerCommunicator.getRegistry();
        WorkerCommunicator.stopHeartbeatExecutor();
        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(messageHandler2, port);
        WorkerCommunicator.broadcast(new MessageFromServer("first message"));
        ClientSideMessageHandlerForTest msg = (ClientSideMessageHandlerForTest) UiCommunicator
            .getMessageHandler();
        long startTime = System.currentTimeMillis();
        while (msg.getMessagesFromServer().size() == 0
            && System.currentTimeMillis() < startTime + 1000L) {
        }
        assertEquals(1, msg.getMessagesFromServer().size());

        // Emulate a worker crashing and coming back by resetting it and running the
        // initializer again
        WorkerCommunicator.reset();
        WorkerCommunicator.initializeInstance(messageHandler1, port);
        WorkerCommunicator.stopHeartbeatExecutor();

        // This instance should have no MessageHandler service references from clients
        assertEquals(0, WorkerCommunicator.getClientMessageServiceStubs().size());

        // IRL, the UiCommunicator will be restarted by the heartbeat monitor, but since
        // we're not using that here we have to manually restart it
        UiCommunicator.restart();
        startTime = System.currentTimeMillis();
        while (WorkerCommunicator.getClientMessageServiceStubs().size() == 0
            && System.currentTimeMillis() < startTime + 1000L) {
        }
        // Now the worker should have a MessageHandlerService from the UiCommunicator
        assertEquals(1, WorkerCommunicator.getClientMessageServiceStubs().size());

        // The new worker can send a message to the UI
        WorkerCommunicator.broadcast(new MessageFromServer("zing!"));
        startTime = System.currentTimeMillis();
        msg = (ClientSideMessageHandlerForTest) UiCommunicator.getMessageHandler();
        while (msg.getMessagesFromServer().size() < 2
            && System.currentTimeMillis() < startTime + 1000L) {
        }
        assertEquals(2, msg.getMessagesFromServer().size());
        ServerSideMessageHandlerForTest msg2 = (ServerSideMessageHandlerForTest) WorkerCommunicator
            .getMessageHandler();

        // The UI can send a message to the new worker
        assertEquals(0, msg2.getMessagesFromClient().size());
        UiCommunicator.send(new MessageFromClient("back at ya!"));
        msg2 = (ServerSideMessageHandlerForTest) WorkerCommunicator.getMessageHandler();
        assertEquals(1, msg2.getMessagesFromClient().size());
    }

    /**
     * Tests the use-case in which the UI exits and a different one starts up.
     */
    @Test
    public void testReinitializeUi() throws InterruptedException {
        WorkerCommunicator.initializeInstance(messageHandler1, port);
        WorkerCommunicator.stopHeartbeatExecutor();
        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(messageHandler2, port);
        WorkerCommunicator.broadcast(new MessageFromServer("first message"));
        ClientSideMessageHandlerForTest msg = (ClientSideMessageHandlerForTest) UiCommunicator
            .getMessageHandler();
        long startTime = System.currentTimeMillis();
        while (msg.getMessagesFromServer().size() == 0
            && System.currentTimeMillis() < startTime + 1000L) {
        }
        assertEquals(1, msg.getMessagesFromServer().size());

        // Emulate the shutdown of a UI by resetting the existing one
        UiCommunicator.reset();

        // Emulate the start of a new GUI
        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(messageHandler2, port);
        UiCommunicator.stopHeartbeatListener();
        startTime = System.currentTimeMillis();
        while (WorkerCommunicator.getClientMessageServiceStubs().size() < 2
            && System.currentTimeMillis() < startTime + 1000L) {
        }

        // there should now be 2 client services in the worker
        assertEquals(2, WorkerCommunicator.getClientMessageServiceStubs().size());

        // broadcast a message
        assertEquals(1, messageHandler2.getMessagesFromServer().size());
        WorkerCommunicator.broadcast(new MessageFromServer("zing!"));
        startTime = System.currentTimeMillis();
        while (messageHandler2.getMessagesFromServer().size() < 2
            && System.currentTimeMillis() < startTime + 1000L) {
        }
        assertEquals(2, messageHandler2.getMessagesFromServer().size());

        // the UI should be able to communicate with the worker as well
        ServerSideMessageHandlerForTest msg2 = (ServerSideMessageHandlerForTest) WorkerCommunicator
            .getMessageHandler();
        assertEquals(0, msg2.getMessagesFromClient().size());
        UiCommunicator.send(new MessageFromClient("back at ya!"));
        msg2 = (ServerSideMessageHandlerForTest) WorkerCommunicator.getMessageHandler();
        assertEquals(1, msg2.getMessagesFromClient().size());
    }

    /**
     * Tests communication when the worker and UI are running in the same JVM.
     */
    @Test
    public void testIntraProcessCommunication() {

		ServerTest serverTest = new ServerTest();
        serverTest.startServer(port, 2, true);
        messageHandler1 = (ServerSideMessageHandlerForTest) WorkerCommunicator.getMessageHandler();

        // broadcast a message before the UiCommunicator has been initialized
        MessageFromServer m1 = new MessageFromServer("telecaster");
        WorkerCommunicator.broadcast(m1);

        ClientSideMessageHandlerForTest messageHandler2 = new ClientSideMessageHandlerForTest();
        UiCommunicator.setHeartbeatManager(heartbeatManager);
        UiCommunicator.initializeInstance(messageHandler2, port);
        UiCommunicator.stopHeartbeatListener();
        assertEquals(0, messageHandler2.getMessagesFromServer().size());

        // broadcast two messages from the worker to the UI
        MessageFromServer m2 = new MessageFromServer("stratocaster");
        WorkerCommunicator.broadcast(m2);

        MessageFromServer m3 = new MessageFromServer("mustang");
        WorkerCommunicator.broadcast(m3);

        Set<MessageFromServer> clientMessages = messageHandler2.getMessagesFromServer();
        assertEquals(2, clientMessages.size());
        Set<String> payloads = new HashSet<>();
        for (MessageFromServer message : clientMessages) {
            payloads.add(message.getPayload());
        }
        assertTrue(payloads.contains("stratocaster"));
        assertTrue(payloads.contains("mustang"));

        // Now construct a MessageFromClient and send it back
        assertEquals(0, messageHandler1.getMessagesFromClient().size());
        MessageFromClient m4 = new MessageFromClient("reply");
        UiCommunicator.send(m4);

        assertEquals(1, messageHandler1.getMessagesFromClient().size());
        for (MessageFromClient message : messageHandler1.getMessagesFromClient()) {
            assertEquals("reply", message.getPayload());
        }
    }

    /**
     * Tests that heartbeats are properly sent, detected, and handled.
     */
    @Test
    public void testHeartbeatManagement() throws InterruptedException {

        // Start the server
        ServerTest serverTest = new ServerTest();
        serverTest.startServer(port, 2, true);

        // Start the heartbeat manager and communicator
        MessageHandler messageHandler = new MessageHandler(
				new PigMessageDispatcherForTest(null, null, false));
        InstrumentedWorkerHeartbeatManager h = new InstrumentedWorkerHeartbeatManager(
            messageHandler);
        UiCommunicator.setHeartbeatManager(h);
        UiCommunicator.initializeInstance(messageHandler, port);
        UiCommunicator.stopHeartbeatListener();

        // Simulate 10 heartbeats getting sent, and a detector that runs at 1/2 the rate
        // of the heartbeat generator.
        long startTime = SystemTime.currentTimeMillis();
        long currentTime = startTime;
        long heartbeatInterval = 100L;
        for (int heartbeatDetectionCount = 0; heartbeatDetectionCount < 5; heartbeatDetectionCount++) {
            currentTime += heartbeatInterval;
            SystemTime.setUserTime(currentTime);
            WorkerCommunicator.broadcast(new WorkerHeartbeatMessage());
            currentTime += heartbeatInterval;
            SystemTime.setUserTime(currentTime);
            WorkerCommunicator.broadcast(new WorkerHeartbeatMessage());
            h.checkForHeartbeat();
        }

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
                - messageHandlerHeartbeatTimesAtChecks.get(i) > 199L);
            assertTrue(messageHandlerHeartbeatTimesAtChecks.get(i + 1)
                - messageHandlerHeartbeatTimesAtChecks.get(i) < 201L);
        }
    }

	public static class PigMessageDispatcherForTest extends PigMessageDispatcher {

		public PigMessageDispatcherForTest(AlertMessageTableModel tableModel, WorkerStatusPanel statusPanel,
				boolean shutdownEnabled) {
			super(tableModel, statusPanel, shutdownEnabled);
		}

		@Override
		public void handleShutdownMessage(WorkerShutdownMessage message) {

		}

	}

}
