package gov.nasa.ziggy.services.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messages.SpecifiedRequestorMessage;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Assorted message handling utilities for testing purposes.
 *
 * @author PT
 */
public class MessagingTestUtils {

    /**
     * A {@link ClusterController} that won't bomb when trying to check the state of the
     * (nonexistent) supervisor.
     *
     * @author PT
     */
    public static class ClusterControllerStub extends ClusterController {

        public ClusterControllerStub(int workerHeapSize, int workerThreadCount) {
            super(workerHeapSize, workerThreadCount);
        }

        @Override
        public boolean isSupervisorRunning() {
            return true;
        }

        @Override
        public boolean isDatabaseAvailable() {
            return true;
        }
    }

    /**
     * A {@link HeartbeatManager} that provides additional information about the inner workings of
     * the class. This should only be used in test, as the means by which the additional information
     * is provided will degrade the long-term performance of the manager.
     *
     * @author PT
     */
    public static class InstrumentedWorkerHeartbeatManager extends HeartbeatManager {

        private static final long SYS_TIME_SCALING = 100_000L;

        /**
         * Allows the timestamps to be returned with their leading digits stripped off. Not strictly
         * necessary for the tests, but useful for when a human being is looking at the results.
         */
        private synchronized static long systemTimeOffset() {
            long sysTime = SystemProxy.currentTimeMillis();
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

        public InstrumentedWorkerHeartbeatManager() {
        }

        @Override
        public void start() throws NoHeartbeatException {
            super.start();
            messageHandlerStartTimes.add(getHeartbeatTime() - systemTimeOffset);
            localStartTimes.add(getPriorHeartbeatTime() - systemTimeOffset);
        }

        @Override
        protected void checkForHeartbeat() {
            messageHandlerHeartbeatTimesAtChecks.add(getHeartbeatTime() - systemTimeOffset);
            localHeartbeatTimesAtChecks.add(getPriorHeartbeatTime() - systemTimeOffset);
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

    public static class InstrumentedHeartbeatMessage extends HeartbeatMessage {

        private static final long serialVersionUID = 20230611L;

        public void setHeartbeatTime(long heartbeatTime) {
            heartbeatTimeMillis = heartbeatTime;
        }
    }

    /**
     * Basic message class that gets sent from a client.
     *
     * @author PT
     */
    public static class Message1 extends PipelineMessage {

        private static final long serialVersionUID = 20230519L;
        private final String payload;

        public Message1(String payload) {
            this.payload = payload;
        }

        public String getPayload() {
            return payload;
        }
    }

    /**
     * Another basic message that gets sent from a client.
     *
     * @author PT
     */
    public static class Message2 extends SpecifiedRequestorMessage {

        private static final long serialVersionUID = 20230519L;
        private final String payload;

        public Message2(String payload) {
            super(() -> new UUID(1L, 2L));
            this.payload = payload;
        }

        public String getPayload() {
            return payload;
        }
    }

    /**
     * Message sent in reply to an instance of {@link Message2}.
     *
     * @author PT
     */
    public static class Message2Reply extends SpecifiedRequestorMessage {

        private static final long serialVersionUID = 20230519L;

        public Message2Reply(Message2 originalMessage) {
            super(originalMessage);
        }
    }
}
