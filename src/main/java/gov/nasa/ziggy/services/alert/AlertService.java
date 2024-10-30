package gov.nasa.ziggy.services.alert;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.Alert.Severity;
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
    private static final Logger log = LoggerFactory.getLogger(AlertService.class);
    public static final PipelineTask DEFAULT_TASK = null;
    public static final boolean BROADCAST_ALERTS_ENABLED_DEFAULT = false;

    public boolean broadcastEnabled = false;
    private AlertLogOperations alertLogOperations = new AlertLogOperations();

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

    public void generateAlert(String sourceComponent, PipelineTask sourceTask, String message) {
        generateAlert(sourceComponent, sourceTask, Severity.ERROR, message);
    }

    public void generateAlert(String sourceComponent, Severity severity, String message) {
        generateAlert(sourceComponent, DEFAULT_TASK, severity, message);
    }

    public void generateAndBroadcastAlert(String sourceComponent, PipelineTask sourceTask,
        Severity severity, String message) {
        boolean storedBroadcastFlag = broadcastEnabled;
        broadcastEnabled = true;
        generateAlert(sourceComponent, sourceTask, severity, message);
        broadcastEnabled = storedBroadcastFlag;
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void generateAlert(String sourceComponent, PipelineTask sourceTask, Severity severity,
        String message) {
        log.debug("ALERT:[{}]: {}", sourceComponent, message);

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

        Alert alertData = new Alert(timestamp, sourceComponent, sourceTask, processName,
            processHost, processId, severity, message);

        try {
            alertLogOperations().persist(new AlertLog(alertData));
        } catch (PipelineException e) {
            log.error("Failed to store alert in database", e);
        }

        if (broadcastEnabled || severity == Severity.INFRASTRUCTURE) {
            ZiggyMessenger.publish(new AlertMessage(alertData));
        }
    }

    AlertLogOperations alertLogOperations() {
        return alertLogOperations;
    }
}
