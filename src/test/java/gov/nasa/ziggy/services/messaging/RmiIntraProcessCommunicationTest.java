package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.RunByNameTestCategory;
import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.Message1;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer.RmiClientThread;

/**
 * Tests the RMI communication classes for Ziggy in the context where the client and server sides
 * are running in the same process and thus the same JVM. This is analogous to the use-case in which
 * the supervisor starts the {@link ZiggyRmiServer} and also has a {@link ZiggyRmiClient} of its
 * own.
 *
 * @author PT
 */
@Category(RunByNameTestCategory.class)
public class RmiIntraProcessCommunicationTest {

    private int port = 4788;
    private Registry registry;

    @Before
    public void setup() {
        registry = null;
        ZiggyRmiClient.clearDetectedMessages();
    }

    @After
    public void teardown() throws RemoteException, InterruptedException {

        if (ZiggyRmiServer.isInitialized() && ZiggyRmiServer.getRegistry() != null) {
            ZiggyRmiServer.shutdown();
        }
        if (registry != null) {
            UnicastRemoteObject.unexportObject(registry, true);
        }
        ZiggyRmiClient.reset();
    }

    /**
     * Tests basic initialization of the two classes.
     */
    @Test
    public void testInitialize() {

        ZiggyRmiServer.initializeInstance(port);
        Set<RmiClientThread> clientStubs = ZiggyRmiServer.getClientServiceStubs();
        assertTrue(clientStubs.isEmpty());
        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);

        clientStubs = ZiggyRmiServer.getClientServiceStubs();
        assertFalse(clientStubs.isEmpty());
        assertNotNull(ZiggyRmiClient.ziggyRmiServerService());
    }

    /**
     * Tests the case in which the server crashes and a new server needs to be instantiated.
     */
    @Test
    public void testReinitializeServer() {

        ZiggyRmiServer.initializeInstance(port);

        // Note that for this test we need to preserve a reference to the registry from
        // the ZiggyRmiServer instance that started it; this will be used to shut down
        // the registry when the test completes.
        registry = ZiggyRmiServer.getRegistry();
        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);
        ZiggyRmiServer.addToBroadcastQueue(new Message1("first message"));
        Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected = ZiggyRmiClient
            .messagesDetected();
        TestEventDetector.detectTestEvent(1000L, () -> messagesDetected.size() > 1);
        assertEquals(1, messagesDetected.get(Message1.class).size());

        // Emulate a server crashing and coming back by resetting it and running the
        // initializer again
        ZiggyRmiServer.reset();
        ZiggyRmiServer.initializeInstance(port);
        ZiggyRmiClient.setUseMessenger(false);

        // This instance should have no MessageHandler service references from clients
        assertEquals(0, ZiggyRmiServer.getClientServiceStubs().size());

        // IRL, the client will be restarted by the heartbeat monitor, but since
        // we're not using that here we have to manually restart it
        ZiggyRmiClient.restart();
        ZiggyRmiClient.setUseMessenger(false);
        TestEventDetector.detectTestEvent(1000L,
            () -> ZiggyRmiServer.getClientServiceStubs().size() > 0);

        // Now the worker should have a MessageHandlerService from the UiCommunicator
        assertEquals(1, ZiggyRmiServer.getClientServiceStubs().size());

        // The new server can send a message to the client
        ZiggyRmiServer.addToBroadcastQueue(new Message1("zing!"));
        TestEventDetector.detectTestEvent(1000L,
            () -> messagesDetected.get(Message1.class).size() > 1);
        assertEquals(2, messagesDetected.get(Message1.class).size());
        assertEquals("zing!",
            ((Message1) messagesDetected.get(Message1.class).get(1)).getPayload());

        // The client can send a message to the new server, which in turn gets rebroadcast
        // everywhere.
        assertEquals(0, ZiggyRmiServer.messagesReceived().size());
        ZiggyRmiClient.send(new Message1("back at ya!"), null);
        TestEventDetector.detectTestEvent(1000L,
            () -> ZiggyRmiServer.messagesReceived().size() > 0);
        assertEquals(1, ZiggyRmiServer.messagesReceived().size());
        Integer message1Count = ZiggyRmiServer.messagesReceived().get(Message1.class);
        assertNotNull(message1Count);
        assertEquals(1, message1Count.intValue());

        TestEventDetector.detectTestEvent(1000L,
            () -> messagesDetected.get(Message1.class).size() > 2);
        assertEquals(3, messagesDetected.get(Message1.class).size());
        assertEquals("back at ya!",
            ((Message1) messagesDetected.get(Message1.class).get(2)).getPayload());
    }

    /**
     * Tests the use-case in which the client exits and a different one starts up.
     */
    @Test
    public void testReinitializeClient() {
        ZiggyRmiServer.initializeInstance(port);
        ZiggyRmiClient.initializeInstance(port, "test client 1");
        ZiggyRmiClient.setUseMessenger(false);
        Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected = ZiggyRmiClient
            .messagesDetected();
        ZiggyRmiServer.addToBroadcastQueue(new Message1("first message"));
        TestEventDetector.detectTestEvent(1000L, () -> messagesDetected.size() > 1);
        assertEquals(2, messagesDetected.size());
        assertEquals(1, messagesDetected.get(Message1.class).size());

        // This is the heartbeat message that the server sends to a new client.
        assertEquals(1, messagesDetected.get(HeartbeatMessage.class).size());

        // Emulate the shutdown of a client by resetting the existing one
        ZiggyRmiClient.reset();

        // Emulate the start of a new client
        ZiggyRmiClient.initializeInstance(port, "test client 2");
        ZiggyRmiClient.setUseMessenger(false);

        // There should now be 2 client stubs -- one from the original client, one from the
        // new client.
        TestEventDetector.detectTestEvent(1000L,
            () -> ZiggyRmiServer.getClientServiceStubs().size() > 1);
        assertEquals(2, ZiggyRmiServer.getClientServiceStubs().size());

        // The client should have a stub from the server
        assertNotNull(ZiggyRmiClient.ziggyRmiServerService());

        // broadcast a message
        ZiggyRmiServer.addToBroadcastQueue(new Message1("zing!"));
        TestEventDetector.detectTestEvent(1000L,
            () -> messagesDetected.get(Message1.class).size() > 1);
        assertEquals(2, messagesDetected.get(Message1.class).size());

        // the client should be able to communicate with the server as well
        ZiggyRmiClient.send(new Message1("from client"), null);
        TestEventDetector.detectTestEvent(1000L,
            () -> messagesDetected.get(Message1.class).size() > 2);
        assertEquals(3, messagesDetected.get(Message1.class).size());
        TestEventDetector.detectTestEvent(1000L,
            () -> ZiggyRmiServer.messagesReceived().size() > 0);
        assertEquals(1, ZiggyRmiServer.messagesReceived().size());
        Integer message1Count = ZiggyRmiServer.messagesReceived().get(Message1.class);
        assertNotNull(message1Count);
        assertEquals(1, message1Count.intValue());
    }

    /**
     * Tests communication when the worker and UI are running in the same JVM.
     *
     * @throws IOException
     */
    @Test
    public void testIntraProcessCommunication() throws IOException {

        RmiServerInstantiator serverTest = new RmiServerInstantiator();
        serverTest.startServer(port, 2, null);

        // broadcast a message before the client has been initialized
        Message1 m1 = new Message1("telecaster");
        broadcastAndWait(m1);

        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);
        Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected = ZiggyRmiClient
            .messagesDetected();

        assertEquals(0, messagesDetected.size());

        // broadcast two messages from the server to the client
        Message1 m2 = new Message1("stratocaster");
        ZiggyRmiServer.addToBroadcastQueue(m2);

        Message1 m3 = new Message1("mustang");
        broadcastAndWait(m3);

        List<PipelineMessage> detectedMessage1Instances = messagesDetected.get(Message1.class);
        assertEquals(2, detectedMessage1Instances.size());
        assertTrue(detectedMessage1Instances.contains(m2));
        assertTrue(detectedMessage1Instances.contains(m3));

        // Now construct a message from the client and send it
        assertEquals(0, ZiggyRmiServer.messagesReceived().size());
        Message1 m4 = new Message1("reply");
        ZiggyRmiClient.send(m4, null);
        waitForBroadcastCompletion();

        assertEquals(3, detectedMessage1Instances.size());
        assertTrue(detectedMessage1Instances.contains(m2));
        assertTrue(detectedMessage1Instances.contains(m3));
        assertTrue(detectedMessage1Instances.contains(m4));
        assertEquals(1, ZiggyRmiServer.messagesReceived().size());
        assertTrue(ZiggyRmiServer.messagesReceived().containsKey(Message1.class));
    }

    /**
     * Tests that the RMI client decrements a {@link CountDownLatch} as part of its
     * {@link ZiggyRmiClient#send(PipelineMessage, CountDownLatch)} method.
     */
    @Test
    public void testSendWithCountdownLatch() throws IOException {
        RmiServerInstantiator serverTest = new RmiServerInstantiator();
        serverTest.startServer(port, 2, null);
        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);

        CountDownLatch countdownLatch = new CountDownLatch(1);
        Message1 m1 = new Message1("telecaster");
        ZiggyRmiClient.send(m1, countdownLatch);
        waitForBroadcastCompletion();
        assertEquals(0, countdownLatch.getCount());
    }

    private void broadcastAndWait(PipelineMessage pipelineMessage) {
        ZiggyRmiServer.addToBroadcastQueue(pipelineMessage);
        waitForBroadcastCompletion();
    }

    /**
     * Forces the test thread to wait until the {@link ZiggyRmiServer} message broadcasting thread
     * has completed its work. This corresponds to the message queue being empty and the broadcast
     * thread in state WAITING.
     */
    private void waitForBroadcastCompletion() {
        assertTrue(
            TestEventDetector.detectTestEvent(1000L, ZiggyRmiServer::isAllMessagingComplete));
    }
}
