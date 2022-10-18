package gov.nasa.ziggy.services.messaging;

import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.ui.cli.ClusterController;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager;
import gov.nasa.ziggy.ui.mon.master.Indicator;

/**
 * Subclass of {@link ProcessHeartbeatManager} that provides additional information about the inner
 * workings of the class. This should only be used in test, as the means by which the additional
 * information is provided will degrade the long-term performance of the manager.
 *
 * @author PT
 */
public class InstrumentedWorkerHeartbeatManager extends ProcessHeartbeatManager {

    private static final long SYS_TIME_SCALING = 100_000L;

    /**
     * Allows the timestamps to be returned with their leading digits stripped off. Not strictly
     * necessary for the tests, but useful for when a human being is looking at the results.
     */
    private synchronized static long systemTimeOffset() {
        long sysTime = System.currentTimeMillis();
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
        super(messageHandler, new ExternalMethodsMock());
        setClusterController(new ClusterControllerForTest(100, 1));
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

    }
}
