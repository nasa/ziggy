package gov.nasa.ziggy.services.messaging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.ui.status.Indicator;
import gov.nasa.ziggy.ui.status.ProcessesStatusPanel;
import gov.nasa.ziggy.ui.status.StatusPanel;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Manages the process of responding to the presence or absence of heartbeat messages from the
 * supervisor. The worker heartbeat messages are sent out at regular intervals from the supervisor
 * to indicate that it has not crashed.
 * <p>
 * At startup, a console puts the Processes summary into the "Gray" (undefined) state. When the
 * {@link ZiggyRmiClient} is created, an instance of {@link ProcessHeartbeatManager} is also
 * created; it waits for 1 heartbeat interval to hear from the supervisor. If the supervisor is
 * heard from in that interval, the state of the Processes summary goes to "Green;" if not heard
 * from, "Red."
 * <p>
 * Once the Processes summary is "Green," a {@link ScheduledThreadPoolExecutor} is started that
 * checks the timestamp of the latest heartbeat at an interval that is 2 * the heartbeat interval.
 * If there have been no new heartbeats since the last one recorded by the
 * {@link ProcessHeartbeatManager}, the Processes summary goes "Yellow," the {@link ZiggyRmiClient}
 * instance is deleted, and a new instance is created; this is necessary because, if the supervisor
 * has restarted, it needs new {@link ZiggyRmiClient} services from each process that uses
 * {@link ZiggyRmiClient}.
 * <p>
 * If, after deleting and reconstructing the {@link ZiggyRmiClient}, the supervisor is still not
 * heard from, the Processes summary goes to "Red," and the heartbeat monitoring is terminated. At
 * this point, it is assumed that whatever has happened to the supervisor is serious enough that it
 * requires human intervention.
 *
 * @author PT
 */
public class ProcessHeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessHeartbeatManager.class);

    private static ProcessHeartbeatManager instance;
    private static boolean initializeInThread = true;

    private long heartbeatIntervalMillis;
    private ScheduledThreadPoolExecutor heartbeatListener;
    private long priorHeartbeatTime;
    private long heartbeatTime;
    protected HeartbeatManagerAssistant heartbeatManagerAssistant;
    private boolean isInitialized = false;
    private ClusterController clusterController;
    private boolean reinitializeOnMissedHeartbeat = true;
    private CountDownLatch heartbeatCountdownLatch;

    public ProcessHeartbeatManager(HeartbeatManagerAssistant heartbeatManagerAssistant) {
        this(heartbeatManagerAssistant, new ClusterController(100, 1));
    }

    /**
     * Constructor for test purposes. This allows the caller to supply a mocked instance of the
     * class that performs all "external" methods (where in this case an external method is one we
     * want to mock and/or detect the use of).
     */
    protected ProcessHeartbeatManager(HeartbeatManagerAssistant heartbeatManagerAssistant,
        ClusterController clusterController) {
        this.heartbeatManagerAssistant = heartbeatManagerAssistant;
        heartbeatIntervalMillis = HeartbeatMessage.heartbeatIntervalMillis();
        this.clusterController = clusterController;
        ZiggyMessenger.subscribe(HeartbeatMessage.class, message -> {
            heartbeatTime = message.getHeartbeatTimeMillis();
            if (heartbeatCountdownLatch != null) {
                heartbeatCountdownLatch.countDown();
            }
        });
    }

    public static void initializeInstance(HeartbeatManagerAssistant heartbeatManagerAssistant) {
        if (isInitialized()) {
            log.info("ProcessHeartbeatManager instance already available, skipping instantiation");
        }
        instance = new ProcessHeartbeatManager(heartbeatManagerAssistant);
        instance.initializeHeartbeatManager();
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    /**
     * Start checking for heartbeats. If they are detected, start the automated at-intervals
     * checking.
     */
    public void initializeHeartbeatManager() {

        if (isInitialized) {
            return;
        }
        // wait for one heartbeat interval to see if we get a heartbeat message. We
        // have to do this in a thread because this activity has to take place in
        // parallel with starting the RMI client.
        if (initializeInThread) {
            new Thread(() -> {
                initializeHeartbeatManagerInternal();
            }).start();
        } else {

            // For testing use, it's more efficient to perform the initialization
            // in the main execution thread.
            initializeHeartbeatManagerInternal();
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    private void initializeHeartbeatManagerInternal() {
        heartbeatCountdownLatch = new CountDownLatch(1);
        SystemProxy.currentTimeMillis();
        log.debug("Heartbeat time: " + heartbeatTime);
        try {
            // If the console, for example, is stopped for a few hours and then resumed,
            // checkForHeartbeat() will call initializeHeartbeatManager() a lot, which can result in
            // this code being executed simultaneously in several threads. That's how the latch can
            // be null shortly after it is created.
            // If future threading issues arise, remove this condition and synchronize this method.
            if (heartbeatCountdownLatch != null) {
                heartbeatCountdownLatch.await(heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            heartbeatCountdownLatch = null;
        }
        if (heartbeatTime <= 0) {
            log.debug("Setting RMI state to error");
            setRmiIndicator(Indicator.State.ERROR);
            if (heartbeatListener != null) {
                heartbeatListener.shutdownNow();
            }
            log.error("Unable to detect supervisor heartbeat messages");
            return;
        }
        log.debug("Setting RMI state to normal");
        setRmiIndicator(Indicator.State.NORMAL);
        priorHeartbeatTime = heartbeatTime;
        if (heartbeatListener == null) {
            startHeartbeatListener();
        }
        isInitialized = true;
        log.debug("initializeHeartbeatManagerInternal: done");
    }

    /**
     * Updates the state of the Processes idiot light.
     */
    protected void setRmiIndicator(Indicator.State state) {
        heartbeatManagerAssistant.setRmiIndicator(state);
    }

    protected void setSupervisorIndicator(Indicator.State state) {
        heartbeatManagerAssistant.setSupervisorIndicator(state);
    }

    protected void setDatabaseIndicator(Indicator.State state) {
        heartbeatManagerAssistant.setDatabaseIndicator(state);
    }

    /**
     * Start at-intervals checking for heartbeats, with a check interval that is 2 * the interval at
     * which the supervisor emits heartbeats.
     */
    protected void startHeartbeatListener() {
        log.info("Starting heartbeat listener thread");
        heartbeatListener = new ScheduledThreadPoolExecutor(1);
        if (heartbeatIntervalMillis == 0) {
            log.info("Heartbeat listener thread not started");
            return;
        }
        heartbeatListener.scheduleAtFixedRate(() -> {
            try {
                checkForHeartbeat();
            } catch (Exception e) {
                log.error("Exception occurred attempting to start heartbeat listener", e);
                // NB: I'm pretty certain this block could never be executed because if there's
                // a NoHeartbeatException thrown in checkForHeartbeat(), this thread will
                // immediately be shut down.
            }
        }, 2 * heartbeatIntervalMillis, 2 * heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        ZiggyShutdownHook.addShutdownHook(() -> {
            heartbeatListener.shutdownNow();
        });
        log.info("Heartbeat listener thread started");
    }

    /**
     * Check for a heartbeat more recent than the last one recorded. If none has happened, attempt
     * to delete and recreate the {@link ZiggyRmiClient} and restart checking for heartbeats. If
     * those attempts fail, stop all automated checking and set the Processes summary to "Red."
     */
    protected void checkForHeartbeat() throws NoHeartbeatException {
        if (heartbeatTime > priorHeartbeatTime) {
            priorHeartbeatTime = heartbeatTime;
        } else {
            setRmiIndicator(Indicator.State.WARNING);
            priorHeartbeatTime = 0L;
            heartbeatTime = 0L;
            restartClientCommunicator();
            isInitialized = false;
            if (reinitializeOnMissedHeartbeat) {
                initializeHeartbeatManager();
            }
        }
        if (clusterController.isDatabaseAvailable()) {
            setDatabaseIndicator(Indicator.State.NORMAL);
        } else {
            setDatabaseIndicator(Indicator.State.ERROR);
        }
        if (clusterController.isSupervisorRunning()) {
            setSupervisorIndicator(Indicator.State.NORMAL);
        } else {
            setSupervisorIndicator(Indicator.State.ERROR);
        }
    }

    public static synchronized void stopHeartbeatListener() {
        if (isInitialized() && instance.getHeartbeatListener() != null) {
            instance.getHeartbeatListener().shutdownNow();
        }
        instance = null;
    }

    public static synchronized void resetHeartbeatTime() {
        if (isInitialized()) {
            instance.heartbeatTime = 0L;
        }
    }

    protected void restartClientCommunicator() {
        heartbeatManagerAssistant.restartClientCommunicator();
    }

    public ScheduledThreadPoolExecutor getHeartbeatListener() {
        return heartbeatListener;
    }

    protected long getPriorHeartbeatTime() {
        return priorHeartbeatTime;
    }

    public long getHeartbeatTime() {
        return heartbeatTime;
    }

    /** For testing use only. */
    protected void setClusterController(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    /** For testing use only. */
    void setReinitializeOnMissedHeartbeat(boolean reinitialize) {
        reinitializeOnMissedHeartbeat = reinitialize;
    }

    /** For testing use only. */
    void setHeartbeatTime(long heartbeatTime) {
        this.heartbeatTime = heartbeatTime;
    }

    /** For testing use only. */
    static void setInitializeInThread(boolean initInThread) {
        initializeInThread = initInThread;
    }

    /**
     * Defines methods that are used by the {@link ProcessHeartbeatManager} to control the console
     * stoplights based on the state of heartbeat detection. These are only used by the console; for
     * the worker, the implementation of this interface is all no-op methods.
     *
     * @author PT
     */
    public interface HeartbeatManagerAssistant {

        void setRmiIndicator(Indicator.State state);

        void setSupervisorIndicator(Indicator.State state);

        void setDatabaseIndicator(Indicator.State state);

        default void restartClientCommunicator() {
            ZiggyRmiClient.restart();
        }
    }

    public static class NoHeartbeatException extends RuntimeException {

        private static final long serialVersionUID = 20210310L;

        public NoHeartbeatException(String string) {
            super(string);
        }
    }

    public static class WorkerHeartbeatManagerAssistant implements HeartbeatManagerAssistant {
        @Override
        public void setRmiIndicator(Indicator.State state) {
        }

        @Override
        public void setSupervisorIndicator(Indicator.State state) {
        }

        @Override
        public void setDatabaseIndicator(Indicator.State state) {
        }
    }

    public static class ConsoleHeartbeatManagerAssistant implements HeartbeatManagerAssistant {

        static final String RMI_ERROR_MESSAGE = "Unable to establish communication with supervisor";
        static final String RMI_WARNING_MESSAGE = "Attempting to establish communication with supervisor";
        static final String SUPERVISOR_ERROR_MESSAGE = "Supervisor process has failed";
        static final String DATABASE_ERROR_MESSAGE = "Database process has failed";

        @Override
        public void setRmiIndicator(Indicator.State state) {
            setIndicator(ProcessesStatusPanel.messagingIndicator(), state, RMI_WARNING_MESSAGE,
                RMI_ERROR_MESSAGE);
        }

        @Override
        public void setSupervisorIndicator(Indicator.State state) {
            setIndicator(ProcessesStatusPanel.supervisorIndicator(), state, null,
                SUPERVISOR_ERROR_MESSAGE);
        }

        @Override
        public void setDatabaseIndicator(Indicator.State state) {
            setIndicator(ProcessesStatusPanel.databaseIndicator(), state, null,
                DATABASE_ERROR_MESSAGE);
        }

        public void updateProcessesIndicator() {
            StatusPanel.ContentItem.PROCESSES.menuItem()
                .setState(
                    Indicator.summaryState(ProcessesStatusPanel.messagingIndicator(),
                        ProcessesStatusPanel.supervisorIndicator(),
                        ProcessesStatusPanel.databaseIndicator()),
                    Indicator.summaryToolTipText(ProcessesStatusPanel.messagingIndicator(),
                        ProcessesStatusPanel.supervisorIndicator(),
                        ProcessesStatusPanel.databaseIndicator()));
        }

        private void setIndicator(Indicator indicator, Indicator.State state, String warningTooltip,
            String errorTooltip) {
            if (indicator == null) {
                return;
            }
            String tooltip = null;
            if (state == Indicator.State.WARNING) {
                tooltip = warningTooltip;
            }
            if (state == Indicator.State.ERROR) {
                tooltip = errorTooltip;
            }
            indicator.setState(state, tooltip);
            updateProcessesIndicator();
        }
    }
}
