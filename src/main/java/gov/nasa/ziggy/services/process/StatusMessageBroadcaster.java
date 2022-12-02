package gov.nasa.ziggy.services.process;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messaging.WorkerCommunicator;

/**
 * This class periodically broadcasts status messages to the "pipeline-status" JMS topic. Status
 * providers register with this class and provide their status messages (extend
 * {@link StatusMessage} via the {@link StatusReporter} interface.
 *
 * @author Todd Klaus
 */
public class StatusMessageBroadcaster {
    private static final Logger log = LoggerFactory.getLogger(StatusMessageBroadcaster.class);

    private static final String TIMER_THREAD_NAME = "StatusMessageBroadcaster.timerThread";

    private final ProcessInfo processInfo;
    private Timer broadcastScheduler = new Timer(TIMER_THREAD_NAME);

    // Map[reporter, reportIntervalMillis]
    private final Map<StatusReporter, Integer> reporters = new HashMap<>();

    public StatusMessageBroadcaster(ProcessInfo processInfo) {
        this.processInfo = processInfo;
    }

    public synchronized void addStatusReporter(final StatusReporter reporter,
        final int reportIntervalMillis) {
        log.info("Adding a status reporter of class " + reporter.getClass().getName());
        reporters.put(reporter, reportIntervalMillis);

        broadcastScheduler.schedule(new TimerTask() {
            @Override
            public void run() {
                sendUpdate(reporter, reportIntervalMillis);
            }
        }, reportIntervalMillis, reportIntervalMillis);
    }

    public synchronized void clearStatusReporters() {
        broadcastScheduler.cancel();
        broadcastScheduler = new Timer(TIMER_THREAD_NAME);
        reporters.clear();
    }

    private void sendUpdate(StatusReporter reporter, int reportInterval) {
        try {
            StatusMessage statusMessage = reporter.reportCurrentStatus();
            if (statusMessage != null) {

                statusMessage.setSourceProcess(processInfo);
                statusMessage.setReportIntervalMillis(reportInterval);

                WorkerCommunicator.broadcast(statusMessage);
            }
        } catch (Exception e) {
            log.error("failed to send status messages", e);
        }
    }

}
