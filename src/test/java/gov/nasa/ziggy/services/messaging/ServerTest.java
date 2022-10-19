package gov.nasa.ziggy.services.messaging;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.messaging.MessageHandlersForTest.ServerSideMessageHandlerForTest;
/**
 * Instantiates the {@link WorkerCommunicator}, potentially in an external process. This allows
 * testing of inter-process communication via RMI.
 *
 * @author PT
 */
public class ServerTest {

    public void startServer(int port, int nMessagesExpected, boolean stopHeartbeatExecutor) {
        ServerSideMessageHandlerForTest serverMessageHandler = new ServerSideMessageHandlerForTest();
        serverMessageHandler.setExpectedMessageCount(nMessagesExpected);

        WorkerCommunicator.initializeInstance(serverMessageHandler, port);
        if (stopHeartbeatExecutor) {
            WorkerCommunicator.stopHeartbeatExecutor();
        }
    }

    public static void main(String[] args) {

        int port = Integer.valueOf(args[0]);
        int expectedMessageCount = Integer.valueOf(args[1]);
        boolean stopHeartbeatExecutor = Boolean.valueOf(args[2]);
        String heartbeatIntervalMillis = args[3];
        if (System.getProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME) == null) {
            System.setProperty(PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME, heartbeatIntervalMillis);
        }
        new ServerTest().startServer(port, expectedMessageCount, stopHeartbeatExecutor);
    }

}
