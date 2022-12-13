package gov.nasa.ziggy.services.messaging;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messages.WorkerShutdownMessage;
import gov.nasa.ziggy.ui.cli.ClusterController;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.HeartbeatManagerExternalMethods;
import gov.nasa.ziggy.ui.messaging.PigMessageDispatcher;
import gov.nasa.ziggy.ui.mon.alerts.AlertMessageTableModel;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.ui.mon.master.WorkerStatusPanel;
import gov.nasa.ziggy.util.SystemTime;

/**
 * Assorted message handling classes for testing purposes.
 *
 * @author PT
 *
 */
public class MessageHandlersForTest {

    /**
     * Subclass of {@link ClusterController} that won't bomb when trying to check the state of the
     * (nonexistent) worker.
     *
     * @author PT
     */
    public static class ClusterControllerForTest extends ClusterController {

        public ClusterControllerForTest(int workerHeapSize, int workerThreadCount) {
            super(workerHeapSize, workerThreadCount);
        }

        @Override
        public boolean isWorkerRunning() {
            return true;
        }

        @Override
        public boolean isDatabaseRunning() {
            return true;
        }

    }

    /**
     * Subclass of {@link ProcessHeartbeatManager} that provides additional information about the
     * inner workings of the class. This should only be used in test, as the means by which the
     * additional information is provided will degrade the long-term performance of the manager.
     *
     * @author PT
     */
    public static class InstrumentedWorkerHeartbeatManager extends ProcessHeartbeatManager {

        private static final long SYS_TIME_SCALING = 100_000L;

        /**
         * Allows the timestamps to be returned with their leading digits stripped off. Not strictly
         * necessary for the tests, but useful for when a human being is looking at the results.
         */
        private synchronized static long systemTimeOffset() {
            long sysTime = SystemTime.currentTimeMillis();
            long scaledSysTime = sysTime / SYS_TIME_SCALING;
            return SYS_TIME_SCALING * scaledSysTime;
        }

        private List<Long> messageHandlerStartTimes = new ArrayList<>();
        private List<Long> localStartTimes = new ArrayList<>();
        private List<Long> messageHandlerHeartbeatTimesAtChecks = new ArrayList<>();
        private List<Long> localHeartbeatTimesAtChecks = new ArrayList<>();
        private List<Long> actualTimesAtChecks = new ArrayList<>();
        private List<Boolean> checkStatus = new ArrayList<>();
        private long systemTimeOffset = systemTimeOffset();

        public InstrumentedWorkerHeartbeatManager(MessageHandler messageHandler) {
            super(messageHandler, new ExternalMethodsMock(), new ClusterControllerForTest(100, 1));
        }

        @Override
        public void initialize() throws NoHeartbeatException {
            super.initialize();
            messageHandlerStartTimes
                .add(getMessageHandler().getLastHeartbeatTimeMillis() - systemTimeOffset);
            localStartTimes.add(getLastHeartbeatTime() - systemTimeOffset);
        }

        @Override
        protected void checkForHeartbeat() {
            messageHandlerHeartbeatTimesAtChecks
                .add(getMessageHandler().getLastHeartbeatTimeMillis() - systemTimeOffset);
            localHeartbeatTimesAtChecks.add(getLastHeartbeatTime() - systemTimeOffset);
            actualTimesAtChecks.add(System.currentTimeMillis() - systemTimeOffset);
            try {
                super.checkForHeartbeat();
                checkStatus.add(true);
            } catch (NoHeartbeatException e) {
                checkStatus.add(false);
            }
        }

        public List<Long> getMessageHandlerStartTimes() {
            return messageHandlerStartTimes;
        }

        public List<Long> getLocalStartTimes() {
            return localStartTimes;
        }

        public List<Long> getMessageHandlerHeartbeatTimesAtChecks() {
            return messageHandlerHeartbeatTimesAtChecks;
        }

        public List<Long> getlocalHeartbeatTimesAtChecks() {
            return localHeartbeatTimesAtChecks;
        }

        public List<Long> getActualTimesAtChecks() {
            return actualTimesAtChecks;
        }

        public List<Boolean> getCheckStatus() {
            return checkStatus;
        }
    }

    /**
     * Subclass of {@link HeartbeatManagerExternalMethods} that does nothing.
     *
     * @author PT
     */
    public static class ExternalMethodsMock extends HeartbeatManagerExternalMethods {

        @Override
        public void setRmiIndicator(Indicator.State state) {
        }

        @Override
        public void setWorkerIndicator(Indicator.State state) {
        }

        @Override
        public void setDatabaseIndicator(Indicator.State state) {
        }
    }

    /**
     * Performs message handling in unit tests for the "client". Specifically, all messages of
     * {@link MessageFromServer} are saved in a Set; all messages of other classes are ignored.
     *
     * @author PT
     */
    public static class ClientSideMessageHandlerForTest extends MessageHandler {

        private Set<MessageFromServer> messagesFromServer = new HashSet<>();

        public ClientSideMessageHandlerForTest() {
            super(new ConsoleMessageDispatcher(null, null, false));
        }

        @Override
        public Object handleMessage(PipelineMessage message) {
            if (message instanceof MessageFromServer) {
                messagesFromServer.add((MessageFromServer) message);
            }
            return null;
        }

        public Set<MessageFromServer> getMessagesFromServer() {
            return messagesFromServer;
        }
    }

    /**
     * Subclass of {@link PigMessageDispatcher} that doesn't try to shut down the console (because
     * it's not actually running in the console).
     *
     * @author PT
     */
    public static class ConsoleMessageDispatcherForTest extends ConsoleMessageDispatcher {

        public ConsoleMessageDispatcherForTest(AlertMessageTableModel tableModel,
            WorkerStatusPanel statusPanel, boolean shutdownEnabled) {
            super(tableModel, statusPanel, shutdownEnabled);
        }

        @Override
        public void handleShutdownMessage(WorkerShutdownMessage message) {

        }
    }

    /**
     * Performs message handling in unit tests for messages sent to the server (worker). It stores
     * all {@link MessageFromClient} instances in a Set and ignores all other kinds of messages.
     * Once a specified number of messages have been received, it replies with a
     * {@link MessageFromServer}.
     *
     * @author PT
     */
    public static class ServerSideMessageHandlerForTest implements MessageHandlerService {

        private Set<MessageFromClient> messagesFromClient = new HashSet<>();
        private int expectedMessageCount = 0;

        @Override
        public Object handleMessage(PipelineMessage message) throws RemoteException {
            if (message instanceof MessageFromClient) {
                messagesFromClient.add((MessageFromClient) message);
            }
            if (expectedMessageCount > 0 && messagesFromClient.size() == expectedMessageCount) {
                WorkerCommunicator.broadcast(new MessageFromServer(messagesFromClient));
            }
            return null;
        }

        public Set<MessageFromClient> getMessagesFromClient() {
            return messagesFromClient;
        }

        public void setServerMessages(Set<MessageFromClient> messagesFromClient) {
            this.messagesFromClient = messagesFromClient;
        }

        public int getExpectedMessageCount() {
            return expectedMessageCount;
        }

        public void setExpectedMessageCount(int expectedMessageCount) {
            this.expectedMessageCount = expectedMessageCount;
        }
    }

}
