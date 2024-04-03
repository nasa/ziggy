package gov.nasa.ziggy.services.alert;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.AlertMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.services.process.ProcessInfo;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Alert service implementation.
 *
 * @author Todd Klaus
 */
public class AlertService {
    public enum Severity {
        INFRASTRUCTURE, ERROR, WARNING;

        @Override
        public String toString() {
            return name();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);
    public static final int DEFAULT_TASK_ID = -1;
    public static final boolean BROADCAST_ALERTS_ENABLED_DEFAULT = false;

    public boolean broadcastEnabled = false;

    static AlertService instance;

    public static synchronized void setInstance(AlertService alertService) {
        instance = alertService;
    }

    public static synchronized AlertService getInstance() {
        if (instance == null) {
            instance = new AlertService();
        }
        return instance;
    }

    public AlertService() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        broadcastEnabled = config.getBoolean(PropertyName.BROADCAST_ALERTS_ENABLED.property(),
            BROADCAST_ALERTS_ENABLED_DEFAULT);
    }

    public void generateAlert(String sourceComponent, String message) {
        generateAlert(sourceComponent, DEFAULT_TASK_ID, message);
    }

    public void generateAlert(String sourceComponent, AlertService.Severity severity,
        String message) {
        generateAlert(sourceComponent, DEFAULT_TASK_ID, severity, message);
    }

    /**
     * @see gov.nasa.ziggy.services.alert.AlertService#generateAlert(java.lang.String, long,
     * java.lang.String)
     */
    public void generateAlert(String sourceComponent, long sourceTaskId, String message) {
        generateAlert(sourceComponent, sourceTaskId, AlertService.Severity.ERROR, message);
    }

    public void generateAndBroadcastAlert(String sourceComponent, long sourceTaskId,
        AlertService.Severity severity, String message) {
        boolean storedBroadcastFlag = broadcastEnabled;
        broadcastEnabled = true;
        generateAlert(sourceComponent, sourceTaskId, severity, message);
        broadcastEnabled = storedBroadcastFlag;
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void generateAlert(String sourceComponent, long sourceTaskId,
        AlertService.Severity severity, String message) {
        log.debug("ALERT:[" + sourceComponent + "]: " + message);

        Date timestamp = new Date();
        String processName = null;
        String processHost = null;
        long processId = -1;

        // get ProcessInfo, if available
        ProcessInfo processInfo = AbstractPipelineProcess.getProcessInfo();

        if (processInfo != null) {
            processName = processInfo.getName();
            processHost = processInfo.getHost();
            processId = processInfo.getPid();
        } else {
            try {
                processHost = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                // This can never occur. The localhost will always have a correctly
                // configured host name.
                throw new AssertionError(e);
            }
        }

        Alert alertData = new Alert(timestamp, sourceComponent, sourceTaskId, processName,
            processHost, processId, severity.toString(), message);

        // store alert in db
        try {
            AlertLogCrud alertCrud = new AlertLogCrud();
            alertCrud.persist(new AlertLog(alertData));
        } catch (PipelineException e) {
            log.error("Failed to store Alert in database", e);
        }

        if (broadcastEnabled || severity == AlertService.Severity.INFRASTRUCTURE) {
            ZiggyMessenger.publish(new AlertMessage(alertData));
        }
    }
}
