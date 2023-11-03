package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.RunByNameTestCategory;
import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.Message1;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.Message2;
import gov.nasa.ziggy.util.os.ProcessUtils;

/**
 * Tests the RMI communication classes for Ziggy in the context where the client and server sides
 * are running in the different processes and thus different JVMs. The server is always run as the
 * external process.
 * <p>
 * The inter- and intra-process tests are in separate classes because of failures that occur when an
 * inter-process test immediately follows an intra-process test.
 *
 * @author PT
 */
@Category(RunByNameTestCategory.class)
public class RmiInterProcessCommunicationTest {

    private int port = 4788;
    private Process serverProcess;
    private Process clientProcess;
    String heartbeatIntervalMillis = "0";

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {
        serverProcess = null;
        clientProcess = null;
        ZiggyRmiClient.clearDetectedMessages();
    }

    @After
    public void teardown() throws NotBoundException, InterruptedException, IOException {

        if (ZiggyRmiServer.isInitialized() && ZiggyRmiServer.getRegistry() != null) {
            ZiggyRmiServer.shutdown();
        }
        ZiggyRmiClient.reset();
        Files.createFile(
            directoryRule.directory().resolve(RmiServerInstantiator.SHUT_DOWN_FILE_NAME));
        TestEventDetector.detectTestEvent(1000L, () -> Files.exists(
            directoryRule.directory().resolve(RmiServerInstantiator.SHUT_DOWN_DETECT_FILE_NAME)));
        if (serverProcess != null) {
            serverProcess.destroy();
            serverProcess = null;
        }
        if (clientProcess != null) {
            clientProcess.destroy();
            clientProcess = null;
        }
    }

    /**
     * Tests communication when the server and client are in separate JVMs.
     */
    @Test
    public void testInterProcessCommunication() throws IOException, InterruptedException {

        // Start the server in a remote process
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(port));
        args.add(Integer.toString(2));
        args.add(directoryRule.directory().toString());

        serverProcess = ProcessUtils.runJava(RmiServerInstantiator.class, args);
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> Files.exists(
            directoryRule.directory().resolve(RmiServerInstantiator.SERVER_READY_FILE_NAME))));

        // now start the client.
        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);

        Registry registry = ZiggyRmiClient.getRegistry();

        assertNotNull(registry);
        assertNotNull(ZiggyRmiClient.ziggyRmiServerService());

        // send two messages
        Message1 m1 = new Message1("telecaster");
        ZiggyRmiClient.send(m1, null);
        Message1 m2 = new Message1("stratocaster");
        ZiggyRmiClient.send(m2, null);

        Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected = ZiggyRmiClient
            .messagesDetected();
        TestEventDetector.detectTestEvent(1000L,
            () -> messagesDetected.size() > 0 && messagesDetected.get(Message1.class) != null
                && messagesDetected.get(Message1.class).size() > 1);
        assertEquals(2, messagesDetected.size());
        List<PipelineMessage> messages = messagesDetected.get(Message1.class);
        assertEquals(2, messages.size());
        assertTrue(messages.contains(m1));
        assertTrue(messages.contains(m2));
        assertTrue(messagesDetected.containsKey(HeartbeatMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultipleClients() throws IOException, ClassNotFoundException {

        // Start the worker in a remote process.
        List<String> args = new ArrayList<>();
        args.add(Integer.toString(port));
        args.add(Integer.toString(2));
        args.add(directoryRule.directory().toString());

        serverProcess = ProcessUtils.runJava(RmiServerInstantiator.class, args);
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> Files.exists(
            directoryRule.directory().resolve(RmiServerInstantiator.SERVER_READY_FILE_NAME))));

        // Start a client in a remote process.
        args.clear();
        args.add(Integer.toString(port));
        args.add(directoryRule.directory().toString());
        clientProcess = ProcessUtils.runJava(RmiClientInstantiator.class, args);
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> Files.exists(
            directoryRule.directory().resolve(RmiClientInstantiator.CLIENT_READY_FILE_NAME))));

        ZiggyRmiClient.initializeInstance(port, "test client");
        ZiggyRmiClient.setUseMessenger(false);

        // Send an instance of Message1
        ZiggyRmiClient.send(new Message1("payload 1"), null);

        // Send an instance of Message2
        ZiggyRmiClient.send(new Message2("payload 2"), null);

        // There should be 2 message classes detected for the local client.
        assertTrue(TestEventDetector.detectTestEvent(1000L,
            () -> ZiggyRmiClient.messagesDetected().size() > 2));
        Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messageMap = ZiggyRmiClient
            .messagesDetected();
        assertEquals(3, messageMap.size());
        assertTrue(messageMap.containsKey(Message1.class));
        List<PipelineMessage> messages = messageMap.get(Message1.class);
        assertEquals(1, messages.size());
        assertEquals("payload 1", ((Message1) messages.get(0)).getPayload());
        assertTrue(messageMap.containsKey(Message2.class));
        messages = messageMap.get(Message2.class);
        assertEquals(1, messages.size());
        assertEquals("payload 2", ((Message2) messages.get(0)).getPayload());
        assertTrue(messageMap.containsKey(HeartbeatMessage.class));

        // The external process client should have the same 2 messages detected.
        Files.createFile(directoryRule.directory()
            .resolve(RmiClientInstantiator.SERIALIZE_MESSAGE_MAP_COMMAND_FILE_NAME));
        assertTrue(
            TestEventDetector.detectTestEvent(1000L, () -> !Files.exists(directoryRule.directory()
                .resolve(RmiClientInstantiator.SERIALIZE_MESSAGE_MAP_COMMAND_FILE_NAME))));
        try (ObjectInputStream s = new ObjectInputStream(
            new FileInputStream(directoryRule.directory()
                .resolve(RmiClientInstantiator.SERIALIZED_MESSAGE_MAP_FILE_NAME)
                .toString()))) {
            messageMap = (Map<Class<? extends PipelineMessage>, List<PipelineMessage>>) s
                .readObject();
        }
        assertEquals(3, messageMap.size());
        assertTrue(messageMap.containsKey(Message1.class));
        messages = messageMap.get(Message1.class);
        assertEquals(1, messages.size());
        assertEquals("payload 1", ((Message1) messages.get(0)).getPayload());
        assertTrue(messageMap.containsKey(Message2.class));
        messages = messageMap.get(Message2.class);
        assertEquals(1, messages.size());
        assertEquals("payload 2", ((Message2) messages.get(0)).getPayload());
        assertTrue(messageMap.containsKey(HeartbeatMessage.class));
    }
}
