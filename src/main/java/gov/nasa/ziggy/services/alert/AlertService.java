package gov.nasa.ziggy.services.alert;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messaging.WorkerCommunicator;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.services.process.ProcessInfo;

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

    public static final String BROADCAST_ENABLED_PROP = "pi.alerts.jmsBroadcast.enabled";
    public static final boolean BROADCAST_ENABLED_DEFAULT = false;

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
        Configuration configService = ZiggyConfiguration.getInstance();
        try {
            broadcastEnabled = configService.getBoolean(BROADCAST_ENABLED_PROP,
                BROADCAST_ENABLED_DEFAULT);
        } catch (Exception ignore) {
        }
    }

    public void generateAlert(String sourceComponent, String message) {
        generateAlert(sourceComponent, -1, message);
    }

    public void generateAlert(String sourceComponent, AlertService.Severity severity,
        String message) {
        generateAlert(sourceComponent, -1, severity, message);
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

    public void generateAlert(String sourceComponent, long sourceTaskId,
        AlertService.Severity severity, String message) {
        log.debug("ALERT:[" + sourceComponent + "]: " + message);

        Date timestamp = new Date();
        String processName = null;
        String processHost = null;
        int processId = -1;

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
                log.warn("failed to get hostname", e);
            }
        }

        Alert alertData = new Alert(timestamp, sourceComponent, sourceTaskId, processName,
            processHost, processId, severity.toString(), message);

        // store alert in db
        try {
            AlertLogCrud alertCrud = new AlertLogCrud();
            alertCrud.create(new AlertLog(alertData));
        } catch (PipelineException e) {
            log.error("Failed to store Alert in database", e);
        }

        if (broadcastEnabled || severity == AlertService.Severity.INFRASTRUCTURE) {
            // broadcast alert on MessagingService
            try {
                WorkerCommunicator.broadcast(new AlertMessage(alertData));
            } catch (PipelineException e) {
                log.error("Failed to broadcast Alert", e);
            }
        }
    }

}
