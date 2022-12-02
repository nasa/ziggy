package gov.nasa.ziggy.services.process;

import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tanukisoftware.wrapper.WrapperManager;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.ZiggyBuild;

/**
 * Superclass for all pipeline process bootstrap classes. Provides common functionality, including
 * starting the {@link StatusMessageBroadcaster} for broadcasting {@link Metric}s, and a JMS
 * listener for handling basic administrative functions (shutdown, restart, status, pause, resume,
 * etc.)
 * <p>
 * "Pipeline processes" include daemons such as Data Receipt, Worker, File Store, etc.
 *
 * @author Todd Klaus
 */
public abstract class AbstractPipelineProcess {
    private static final Logger log = LoggerFactory.getLogger(AbstractPipelineProcess.class);

    private static final String STATUS_BROADCASTER_ENABLED_PROP = "services.process.statusBroadcaster.enabled";
    private static final boolean STATUS_BROADCASTER_ENABLED_DEFAULT = true;

    public static final String PROCESS_STATUS_REPORT_INTERVAL_MILLIS_PROP = "services.statusReport.process.reportIntervalMillis";
    public static final String METRICS_STATUS_REPORT_INTERVAL_MILLIS_PROP = "services.statusReport.metrics.reportIntervalMillis";

    public static final int REPORT_INTERVAL_MILLIS_DEFAULT = 60000;

    private boolean initDatabaseService = true;

    protected static long startTime = System.currentTimeMillis();

    private static ProcessInfo processInfo = null;

    private StatusMessageBroadcaster processStatusBroadcaster;

    public AbstractPipelineProcess(String name) {
        this(name, true, true);
    }

    public AbstractPipelineProcess(String name, boolean initMessagingService,
        boolean initDatabaseService) {
        this.initDatabaseService = initDatabaseService;

        String host = "?";
        try {
            host = HostNameUtils.truncatedHostName();
        } catch (UnknownHostException e) {
            // This is a fatal error because we sometimes use this as a database key
            log.error("Failed to get hostname.", e);
            throw new IllegalStateException(e);
        }

        int pid = 0;
        int jvmid = 0;
        if (WrapperManager.isControlledByNativeWrapper()) {
            pid = WrapperManager.getJavaPID();
            jvmid = WrapperManager.getJVMId();
        } else {
            log.info("JVM is NOT controlled by Native Wrapper");
        }

        processInfo = new ProcessInfo(name, host, pid, jvmid);
    }

    protected void initialize() {
        log.debug("initialize(String[]) - start");

        log.info("Starting initialization for Process: " + processInfo);
        ZiggyBuild.logVersionInfo(log);

        log.info("jvm version:");
        log.info("  java.runtime.name=" + System.getProperty("java.runtime.name"));
        log.info("  sun.boot.library.path=" + System.getProperty("sun.boot.library.path"));
        log.info("  java.vm.version=" + System.getProperty("java.vm.version"));

        log.info("Initializing ConfigurationService...");
        Configuration configService = ZiggyConfiguration.getInstance();

        if (initDatabaseService) {
            log.info("Initializing DatabaseService...");
            DatabaseService.getInstance();
        }

        boolean statusBroadcasterEnabled = configService.getBoolean(STATUS_BROADCASTER_ENABLED_PROP,
            STATUS_BROADCASTER_ENABLED_DEFAULT);

        if (statusBroadcasterEnabled) {
            log.info("Initializing StatusMessageBroadcaster...");
            processStatusBroadcaster = new StatusMessageBroadcaster(processInfo);

        }

        log.debug("initialize(String[]) - end");
    }

    protected void addProcessStatusReporter(StatusReporter reporter, int reportIntervalMillis) {
        if (processStatusBroadcaster != null) {
            log.info("Adding a process status broadcast to the system");
            processStatusBroadcaster.addStatusReporter(reporter, reportIntervalMillis);
        }
    }

    public static long getStartTime() {
        return startTime;
    }

    public boolean isInitDatabaseService() {
        return initDatabaseService;
    }

    public static ProcessInfo getProcessInfo() {
        return processInfo;
    }

    /**
     * Abstract pipeline process should or should not attempt to initialize the datbaase server when
     * starting up.
     *
     * @param initDatabaseService
     */
    public void setInitDatabaseService(boolean initDatabaseService) {
        this.initDatabaseService = initDatabaseService;
    }

}
