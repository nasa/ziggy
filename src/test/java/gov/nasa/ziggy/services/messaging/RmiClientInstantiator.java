package gov.nasa.ziggy.services.messaging;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Instantiates the {@link ZiggyRmiClient} in an external process. This allows multi-client testing
 * of the RMI communication system.
 *
 * @author PT
 */
public class RmiClientInstantiator {

    public static final String CLIENT_READY_FILE_NAME = "client-ready";
    public static final String SERIALIZE_MESSAGE_MAP_COMMAND_FILE_NAME = "serialize";
    public static final String SERIALIZED_MESSAGE_MAP_FILE_NAME = "message-map.ser";

    public void startClient(int port, String clientReadyDir) throws IOException {

        ZiggyRmiClient.initializeInstance(port, "external process client");
        ZiggyRmiClient.setUseMessenger(false);
        if (clientReadyDir == null) {
            return;
        }
        Path clientReadyFile = Paths.get(clientReadyDir).resolve(CLIENT_READY_FILE_NAME);
        Files.createFile(clientReadyFile);

        // Now we need to have a busy loop. There's no need to check for shutdown conditions,
        // since we will destroy the client process from the test class that created the
        // process. We do need to check for the file that tells this method to serialize the
        // detected messages map.
        while (true) {
            if (Files
                .exists(Paths.get(clientReadyDir).resolve(SERIALIZE_MESSAGE_MAP_COMMAND_FILE_NAME))
                && ZiggyRmiClient.messagesDetected().size() >= 3) {
                try (ObjectOutputStream s = new ObjectOutputStream(
                    new FileOutputStream(Paths.get(clientReadyDir)
                        .resolve(SERIALIZED_MESSAGE_MAP_FILE_NAME)
                        .toString()))) {
                    s.writeObject(ZiggyRmiClient.messagesDetected());
                    Files.delete(
                        Paths.get(clientReadyDir).resolve(SERIALIZE_MESSAGE_MAP_COMMAND_FILE_NAME));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {

        int port = Integer.parseInt(args[0]);
        String clientReadyDir = args[1];

        new RmiClientInstantiator().startClient(port, clientReadyDir);
    }
}
