package gov.nasa.ziggy.services.messaging;

import static gov.nasa.ziggy.services.config.PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ClientSideMessageHandlerForTest;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.InstrumentedWorkerHeartbeatManager;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.PigMessageDispatcherForTest;
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
    String heartbeatIntervalMillis = "0";
    private ProcessHeartbeatManager heartbeatManager = mock(ProcessHeartbeatManager.class);

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {
        serverProcess = null;
    }

    @After
    public void teardown() throws NotBoundException, InterruptedException, IOException {

        UiCommunicator.stopHeartbeatListener();
        if (WorkerCommunicator.isInitialized() && WorkerCommunicator.getRegistry() != null) {
            WorkerCommunicator.shutdown();
        }
        UiCommunicator.reset();
        Files.createFile(directoryRule.directory().resolve(ServerTest.SHUT_DOWN_FILE_NAME));
        TestEventDetector.detectTestEvent(1000L, () -> Files
            .exists(directoryRule.directory().resolve(ServerTest.SHUT_DOWN_DETECT_FILE_NAME)));
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
        args.add(directoryRule.directory().toString());

        serverProcess = ProcessUtils.runJava(ServerTest.class, args);
        TestEventDetector.detectTestEvent(1000L, () -> Files
            .exists(directoryRule.directory().resolve(ServerTest.SERVER_READY_FILE_NAME)));
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

        ClientSideMessageHandlerForTest messageHandler = (ClientSideMessageHandlerForTest) UiCommunicator
            .getMessageHandler();
        TestEventDetector.detectTestEvent(1000L,
            () -> messageHandler.getMessagesFromServer().size() > 0);
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
        args.add(directoryRule.directory().toString());

        serverProcess = ProcessUtils.runJava(ServerTest.class, args);
        TestEventDetector.detectTestEvent(1000L, () -> Files
            .exists(directoryRule.directory().resolve(ServerTest.SERVER_READY_FILE_NAME)));

        // Start the heartbeat manager and communicator
        MessageHandler messageHandler = new MessageHandler(
            new ConsoleMessageDispatcherForTest(null, null, false));
        messageHandler.setLastHeartbeatTimeMillis(1L);
        InstrumentedWorkerHeartbeatManager h = new InstrumentedWorkerHeartbeatManager(
            messageHandler);
        UiCommunicator.setHeartbeatManager(h);
        UiCommunicator.initializeInstance(messageHandler, port);

        // Pause to let 5 heartbeat-check intervals go by
        Path heartbeatFile = directoryRule.directory().resolve(ServerTest.SEND_HEARTBEAT_FILE_NAME);
        for (int heartbeatCount = 0; heartbeatCount < 5; heartbeatCount++) {
            Files.createFile(heartbeatFile);
            TestEventDetector.detectTestEvent(1000L, () -> !Files.exists(heartbeatFile));
            h.checkForHeartbeat();
        }

        // Start checking to see what the heartbeat handler did:
        // there should be only 1 start (i.e., no restarts)
        List<Long> messageHandlerStartTimes = h.getMessageHandlerStartTimes();
        List<Long> localStartTimes = h.getLocalStartTimes();
        assertEquals(1, messageHandlerStartTimes.size());
        assertEquals(1, localStartTimes.size());
        assertTrue(messageHandlerStartTimes.get(0) > 0);

        // There should be 5 checks after starting, all good
        List<Boolean> checkStatus = h.getCheckStatus();
        assertEquals(5, checkStatus.size());
        for (Boolean status : checkStatus) {
            assertTrue(status);
        }

    }

}
