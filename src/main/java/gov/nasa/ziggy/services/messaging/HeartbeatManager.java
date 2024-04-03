package gov.nasa.ziggy.services.messaging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.HeartbeatCheckMessage;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
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
 * {@link ZiggyRmiClient} is created, an instance of {@link HeartbeatManager} is also created; it
 * waits for 1 heartbeat interval to hear from the supervisor. If the supervisor is heard from in
 * that interval, the state of the Processes summary goes to "Green;" if not heard from, "Red."
 * <p>
 * Once the Processes summary is "Green," a {@link ScheduledThreadPoolExecutor} is started that
 * checks the timestamp of the latest heartbeat at an interval that is 2 * the heartbeat interval.
 * If there have been no new heartbeats since the last one recorded by the {@link HeartbeatManager},
 * the Processes summary goes "Yellow," the {@link ZiggyRmiClient} instance is deleted, and a new
 * instance is created; this is necessary because, if the supervisor has restarted, it needs new
 * {@link ZiggyRmiClient} services from each process that uses {@link ZiggyRmiClient}.
 * <p>
 * If, after deleting and reconstructing the {@link ZiggyRmiClient}, the supervisor is still not
 * heard from, the Processes summary goes to "Red," and the heartbeat monitoring is terminated. At
 * this point, it is assumed that whatever has happened to the supervisor is serious enough that it
 * requires human intervention.
 *
 * @author PT
 */
public class HeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);

    private static HeartbeatManager instance;
    private static boolean initializeInThread = true;

    private long heartbeatIntervalMillis;
    private ScheduledThreadPoolExecutor heartbeatListener;
    private long priorHeartbeatTime;
    private long heartbeatTime;
    private boolean started;
    private boolean reinitializeOnMissedHeartbeat = true;
    private CountDownLatch heartbeatCountdownLatch;

    /** For testing only. Use static method startInstance() to start singleton. */
    HeartbeatManager() {
        heartbeatIntervalMillis = HeartbeatMessage.heartbeatIntervalMillis();
        ZiggyMessenger.subscribe(HeartbeatMessage.class, message -> {
            heartbeatTime = message.getHeartbeatTimeMillis();
            if (heartbeatCountdownLatch != null) {
                heartbeatCountdownLatch.countDown();
            }
        });
        heartbeatTime = -1L;
    }

    public static synchronized void startInstance() {
        if (isInstanceStarted()) {
            log.info("ProcessHeartbeatManager instance already available, skipping instantiation");
        }
        instance = new HeartbeatManager();
        instance.start();
    }

    private static boolean isInstanceStarted() {
        return instance != null && instance.started;
    }

    /**
     * Start checking for heartbeats. If they are detected, start the automated at-intervals
     * checking.
     */
    void start() {

        if (started) {
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
        ZiggyMessenger.publish(new HeartbeatCheckMessage(heartbeatTime), false);
        if (heartbeatTime <= 0) {
            if (heartbeatListener != null) {
                heartbeatListener.shutdownNow();
            }
            log.error("Unable to detect supervisor heartbeat messages");
            return;
        }
        priorHeartbeatTime = heartbeatTime;
        if (heartbeatListener == null) {
            startHeartbeatListener();
        }
        started = true;
        log.debug("initializeHeartbeatManagerInternal: done");
    }

    /**
     * Start at-intervals checking for heartbeats, with a check interval that is 2 * the interval at
     * which the supervisor emits heartbeats.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
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
                throw new AssertionError(e);
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
            priorHeartbeatTime = 0L;
            heartbeatTime = 0L;
            restartZiggyRmiClient();
            started = false;
            if (reinitializeOnMissedHeartbeat) {
                start();
            }
        }
        ZiggyMessenger.publish(new HeartbeatCheckMessage(heartbeatTime), false);
    }

    /** Broken out for unit tests. */
    void restartZiggyRmiClient() {
        ZiggyRmiClient.restart();
    }

    public static synchronized void resetHeartbeatTime() {
        if (isInstanceStarted()) {
            instance.heartbeatTime = 0L;
        }
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

    public static class NoHeartbeatException extends RuntimeException {

        private static final long serialVersionUID = 20210310L;

        public NoHeartbeatException(String string) {
            super(string);
        }
    }
}
