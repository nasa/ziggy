package gov.nasa.ziggy.services.messaging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Instantiates the {@link ZiggyRmiServer}, potentially in an external process. This allows testing
 * of inter-process communication via RMI.
 *
 * @author PT
 */
public class RmiServerInstantiator {

    public static final String SERVER_READY_FILE_NAME = "server-ready";
    public static final String SEND_HEARTBEAT_FILE_NAME = "send-heartbeat";
    public static final String SHUT_DOWN_FILE_NAME = "shutdown";
    public static final String SHUT_DOWN_DETECT_FILE_NAME = "shutdown-detected";

    public void startServer(int port, int nMessagesExpected, String serverReadyDir)
        throws IOException {
        ZiggyRmiServer.initializeInstance(port);

        if (serverReadyDir == null) {
            return;
        }

        Paths.get(serverReadyDir).resolve(SEND_HEARTBEAT_FILE_NAME);
        Path shutdownFile = Paths.get(serverReadyDir).resolve(SHUT_DOWN_FILE_NAME);
        Path serverReadyFile = Paths.get(serverReadyDir).resolve(SERVER_READY_FILE_NAME);
        Files.createFile(serverReadyFile);
        while (!Files.exists(shutdownFile)) {
        }
        ZiggyRmiServer.shutdown(true);
        Files.createFile(Paths.get(serverReadyDir).resolve(SHUT_DOWN_DETECT_FILE_NAME));
    }

    public static void main(String[] args) throws IOException {

        int port = Integer.parseInt(args[0]);
        int expectedMessageCount = Integer.parseInt(args[1]);
        String serverReadyDir = null;
        if (args.length > 2) {
            serverReadyDir = args[2];
        }
        new RmiServerInstantiator().startServer(port, expectedMessageCount, serverReadyDir);
    }
}
