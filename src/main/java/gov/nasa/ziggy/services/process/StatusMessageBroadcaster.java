package gov.nasa.ziggy.services.process;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * This class periodically broadcasts status messages to the "pipeline-status" JMS topic. Status
 * providers register with this class and provide their status messages (extend
 * {@link StatusMessage} via the {@link StatusReporter} interface.
 * <p>
 * Status updates are sent whenever the process receives a heartbeat message from the supervisor, or
 * (for updates from the supervisor) at whatever interval the supervisor decides is appropriate.
 *
 * @author Todd Klaus
 * @author PT
 */
public class StatusMessageBroadcaster {
    private static final Logger log = LoggerFactory.getLogger(StatusMessageBroadcaster.class);

    private final ProcessInfo processInfo;

    // Map[reporter, reportIntervalMillis]
    private final Set<StatusReporter> reporters = new HashSet<>();

    public StatusMessageBroadcaster(ProcessInfo processInfo) {
        this.processInfo = processInfo;
    }

    public synchronized void addStatusReporter(final StatusReporter reporter) {
        log.info("Adding a status reporter of class " + reporter.getClass().getName());
        reporters.add(reporter);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void sendUpdates() {
        log.debug("Sending status messages");
        try {
            for (StatusReporter reporter : reporters) {
                StatusMessage statusMessage = reporter.reportCurrentStatus();
                if (statusMessage != null) {

                    statusMessage.setSourceProcess(processInfo);
                    ZiggyMessenger.publish(statusMessage);
                }
            }
        } catch (Exception e) {
            log.error("failed to send status messages", e);
        }
    }
}
