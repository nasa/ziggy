package gov.nasa.ziggy.services.messaging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ServerSideMessageHandlerForTest;
import gov.nasa.ziggy.util.SystemTime;

/**
 * Instantiates the {@link WorkerCommunicator}, potentially in an external process. This allows
 * testing of inter-process communication via RMI.
 *
 * @author PT
 */
public class ServerTest {

    public static final String SERVER_READY_FILE_NAME = "server-ready";
    public static final String SEND_HEARTBEAT_FILE_NAME = "send-heartbeat";
    public static final String SHUT_DOWN_FILE_NAME = "shutdown";
    public static final String SHUT_DOWN_DETECT_FILE_NAME = "shutdown-detected";

    public void startServer(int port, int nMessagesExpected, String serverReadyDir)
        throws IOException {
        ServerSideMessageHandlerForTest serverMessageHandler = new ServerSideMessageHandlerForTest();
        serverMessageHandler.setExpectedMessageCount(nMessagesExpected);

        WorkerCommunicator.initializeInstance(serverMessageHandler, port);
        WorkerCommunicator.stopHeartbeatExecutor();

        if (serverReadyDir == null) {
            return;
        }

        Path heartbeatFile = Paths.get(serverReadyDir).resolve(SEND_HEARTBEAT_FILE_NAME);
        Path shutdownFile = Paths.get(serverReadyDir).resolve(SHUT_DOWN_FILE_NAME);
        long startTime = SystemTime.currentTimeMillis();
        long currentTime = startTime;
        long heartbeatInterval = 1000L;
        while (!Files.exists(shutdownFile)) {
            if (Files.exists(heartbeatFile)) {

                // We need to simulate the passage of time between heartbeat messages
                // because if the listener sees the same heartbeat time for each
                // heartbeat it assumes that there's a problem.
                currentTime += heartbeatInterval;
                SystemTime.setUserTime(currentTime);
                WorkerCommunicator.broadcast(new WorkerHeartbeatMessage());
                Files.delete(heartbeatFile);
            }
        }
        WorkerCommunicator.shutdown(true);
        Files.createFile(Paths.get(serverReadyDir).resolve(SHUT_DOWN_DETECT_FILE_NAME));
    }

    public static void main(String[] args) throws IOException {

        int port = Integer.parseInt(args[0]);
        int expectedMessageCount = Integer.parseInt(args[1]);
        String serverReadyDir = null;
        if (args.length > 2) {
            serverReadyDir = args[2];
        }
        new ServerTest().startServer(port, expectedMessageCount, serverReadyDir);
    }

}
