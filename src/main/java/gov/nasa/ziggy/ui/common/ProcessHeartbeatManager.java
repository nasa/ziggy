package gov.nasa.ziggy.ui.common;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.ui.mon.master.MasterStatusPanel;
import gov.nasa.ziggy.ui.mon.master.ProcessesIndicatorPanel;
import gov.nasa.ziggy.util.SystemTime;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Manages the process of responding to the presence or absence of heartbeat messages from the
 * worker. The worker heartbeat messages are sent out at regular intervals from the worker to
 * indicate that it has not crashed.
 * <p>
 * At startup, a UI puts the Processes summary into the "Gray" (undefined) state. When the
 * {@link UiCommunicator} is created, an instance of {@link ProcessHeartbeatManager} is also
 * created; it waits for 1 heartbeat interval to hear from the worker. If the worker is heard from
 * in that interval, the state of the Processes summary goes to "Green;" if not heard from, "Red."
 * <p>
 * Once the Processes summary is "Green," a {@link ScheduledThreadPoolExecutor} is started that
 * checks the timestamp of the latest heartbeat at an interval that is 2 * the heartbeat interval.
 * If there have been no new heartbeats since the last one recorded by the
 * {@link ProcessHeartbeatManager}, the Processes summary goes "Yellow," the {@link UiCommunicator}
 * instance is deleted, and a new instance is created; this is necessary because, if the worker has
 * restarted, it needs new copies of the {@link MessageHandler} from each UI, which are provided
 * during construction of the {@link UiCommunicator}.
 * <p>
 * If, after deleting and reconstructing the {@link UiCommunicator}, the worker is still not heard
 * from, the Processes summary goes to "Red," and the heartbeat monitoring is terminated. At this
 * point, it is assumed that whatever has happened to the worker is serious enough that it requires
 * human intervention.
 *
 * @author PT
 */
public class ProcessHeartbeatManager {

    static final String RMI_RED_MESSAGE = "Unable to establish communication with worker";
    static final String RMI_AMBER_MESSAGE = "Attempting to establish communication with worker";
    static final String WORKER_RED_MESSAGE = "Worker process has failed";
    static final String DATABASE_RED_MESSAGE = "Database process has failed";

    private MessageHandler messageHandler;
    private long heartbeatIntervalMillis;
    private ScheduledThreadPoolExecutor heartbeatListener;
    private long lastHeartbeatTime;
    private HeartbeatManagerExternalMethods externalMethodsManager;
    private boolean isInitialized = false;
    private ClusterController clusterController;
    private boolean reinitializeOnMissedHeartbeat = true;

    public ProcessHeartbeatManager(MessageHandler messageHandler) {
        this(messageHandler, new HeartbeatManagerExternalMethods(), new ClusterController(100, 1));
    }

    /**
     * Constructor for test purposes. This allows the caller to supply a mocked instance of the
     * class that performs all "external" methods (where in this case an external method is one we
     * want to mock and/or detect the use of).
     *
     * @param messageHandler
     * @param externalMethodsManager
     */
    protected ProcessHeartbeatManager(MessageHandler messageHandler,
        HeartbeatManagerExternalMethods externalMethodsManager,
        ClusterController clusterController) {
        this.messageHandler = messageHandler;
        this.externalMethodsManager = externalMethodsManager;
        heartbeatIntervalMillis = messageHandler.heartbeatIntervalMillis();
        this.clusterController = clusterController;
    }

    /**
     * Start checking for heartbeats. If they are detected, start the automated at-intervals
     * checking.
     *
     * @throws NoHeartbeatException
     */
    public void initialize() throws NoHeartbeatException {

        if (isInitialized) {
            return;
        }
        // wait for one heartbeat interval to see if we get a heartbeat message
        long t0 = SystemTime.currentTimeMillis();
        while (messageHandler.getLastHeartbeatTimeMillis() == 0
            && SystemTime.currentTimeMillis() - t0 < heartbeatIntervalMillis) {
        }
        if (messageHandler.getLastHeartbeatTimeMillis() <= 0) {
            setRmiIndicator(Indicator.State.RED);
            if (heartbeatListener != null) {
                heartbeatListener.shutdownNow();
            }
            throw new NoHeartbeatException("Unable to detect worker heartbeat messages");
        }
        setRmiIndicator(Indicator.State.GREEN);
        lastHeartbeatTime = messageHandler.getLastHeartbeatTimeMillis();
        if (heartbeatListener == null) {
            startHeartbeatListener();
        }
        isInitialized = true;
    }

    /**
     * Updates the state of the Processes idiot light.
     */
    protected void setRmiIndicator(Indicator.State state) {
        externalMethodsManager.setRmiIndicator(state);
    }

    protected void setWorkerIndicator(Indicator.State state) {
        externalMethodsManager.setWorkerIndicator(state);
    }

    protected void setDatabaseIndicator(Indicator.State state) {
        externalMethodsManager.setDatabaseIndicator(state);
    }

    /**
     * Start at-intervals checking for heartbeats, with a check interval that is 2 * the interval at
     * which the worker emits heartbeats.
     */
    protected void startHeartbeatListener() {
        heartbeatListener = new ScheduledThreadPoolExecutor(1);
        if (heartbeatIntervalMillis == 0) {
            return;
        }
        heartbeatListener.scheduleAtFixedRate(() -> {
            try {
                checkForHeartbeat();
            } catch (Exception e) {
                e.printStackTrace();
                // NB: I'm pretty certain this block could never be executed because if there's
                // a NoHeartbeatException thrown in checkForHeartbeat(), this thread will
                // immediately be shut down.
            }
        }, 2 * heartbeatIntervalMillis, 2 * heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        ZiggyShutdownHook.addShutdownHook(() -> {
            heartbeatListener.shutdownNow();
        });

    }

    /**
     * Check for a heartbeat more recent than the last one recorded. If none has happened, attempt
     * to delete and recreate the {@link UiCommunicator} and restart checking for heartbeats. If
     * those attempts fail, stop all automated checking and set the Processes summary to "Red."
     *
     * @throws NoHeartbeatException
     */
    protected void checkForHeartbeat() throws NoHeartbeatException {
        if (messageHandler.getLastHeartbeatTimeMillis() > lastHeartbeatTime) {
            lastHeartbeatTime = messageHandler.getLastHeartbeatTimeMillis();
        } else {
            setRmiIndicator(Indicator.State.AMBER);
            lastHeartbeatTime = 0L;
            messageHandler.resetLastHeartbeatTime();
            restartUiCommunicator();
            isInitialized = false;
            if (reinitializeOnMissedHeartbeat) {
                initialize();
            }
        }
        if (clusterController.isDatabaseRunning()) {
            setDatabaseIndicator(Indicator.State.GREEN);
        } else {
            setDatabaseIndicator(Indicator.State.RED);
        }
        if (clusterController.isWorkerRunning()) {
            setWorkerIndicator(Indicator.State.GREEN);
        } else {
            setWorkerIndicator(Indicator.State.RED);
        }
    }

    protected void restartUiCommunicator() {
        externalMethodsManager.restartUiCommunicator();
    }

    public ScheduledThreadPoolExecutor getHeartbeatListener() {
        return heartbeatListener;
    }

    protected MessageHandler getMessageHandler() {
        return messageHandler;
    }

    protected long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    // For testing use only.
    protected void setClusterController(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    // For testing use only.
    void setReinitializeOnMissedHeartbeat(boolean reinitialize) {
        reinitializeOnMissedHeartbeat = reinitialize;
    }

    public static class HeartbeatManagerExternalMethods {

        Indicator.State getProcessIdiotLightState() {
            return MasterStatusPanel.processesIndicator().getState();
        }

        public void setRmiIndicator(Indicator.State state) {
            setIndicator(ProcessesIndicatorPanel.messagingIndicator(), state, RMI_AMBER_MESSAGE,
                RMI_RED_MESSAGE);
        }

        public void setWorkerIndicator(Indicator.State state) {
            setIndicator(ProcessesIndicatorPanel.workerIndicator(), state, null,
                WORKER_RED_MESSAGE);
        }

        public void setDatabaseIndicator(Indicator.State state) {
            setIndicator(ProcessesIndicatorPanel.databaseIndicator(), state, null,
                DATABASE_RED_MESSAGE);
        }

        void setIndicator(Indicator indicator, Indicator.State state, String amberTooltip,
            String redTooltip) {
            if (indicator == null) {
                return;
            }
            String tooltip = null;
            if (state == Indicator.State.AMBER) {
                tooltip = amberTooltip;
            }
            if (state == Indicator.State.RED) {
                tooltip = redTooltip;
            }
            indicator.setState(state, tooltip);
            updateProcessesIndicator();

        }

        void updateProcessesIndicator() {
            MasterStatusPanel.processesIndicator()
                .setState(
                    Indicator.summaryState(ProcessesIndicatorPanel.messagingIndicator(),
                        ProcessesIndicatorPanel.workerIndicator(),
                        ProcessesIndicatorPanel.databaseIndicator()),
                    Indicator.summaryToolTipText(ProcessesIndicatorPanel.messagingIndicator(),
                        ProcessesIndicatorPanel.workerIndicator(),
                        ProcessesIndicatorPanel.databaseIndicator()));
        }

        void restartUiCommunicator() {
            UiCommunicator.restart();
        }
    }

    public static class NoHeartbeatException extends Exception {

        private static final long serialVersionUID = 20210310L;

        public NoHeartbeatException(String string) {
            super(string);
        }
    }
}
