package gov.nasa.ziggy.services.process;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tanukisoftware.wrapper.WrapperManager;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.os.ProcessUtils;

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

    private static final boolean STATUS_BROADCAST_ENABLED_DEFAULT = true;

    private boolean initDatabaseService = true;

    protected static long startTime = System.currentTimeMillis();

    private static ProcessInfo processInfo = null;

    public static StatusMessageBroadcaster processStatusBroadcaster;

    public AbstractPipelineProcess(String name) {
        this(name, true, true);
    }

    public AbstractPipelineProcess(String name, boolean initMessagingService,
        boolean initDatabaseService) {
        this.initDatabaseService = initDatabaseService;

        long pid = ProcessUtils.getPid();
        int jvmid = 0;
        if (WrapperManager.isControlledByNativeWrapper()) {
            pid = WrapperManager.getJavaPID();
            jvmid = WrapperManager.getJVMId();
        } else {
            log.info("JVM is NOT controlled by Native Wrapper");
        }

        processInfo = new ProcessInfo(name, pid, jvmid);
    }

    protected void initialize() {
        log.debug("initialize() - start");

        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        log.info("Starting process {} ({})", processInfo, BuildInfo.ziggyVersion());
        ZiggyConfiguration.logJvmProperties();

        if (initDatabaseService) {
            log.info("Initializing DatabaseService...");
            DatabaseService.getInstance();
        }

        boolean statusBroadcasterEnabled = config.getBoolean(
            PropertyName.STATUS_BROADCAST_ENABLED.property(), STATUS_BROADCAST_ENABLED_DEFAULT);

        if (statusBroadcasterEnabled && processStatusBroadcaster == null) {
            log.info("Initializing StatusMessageBroadcaster...");
            processStatusBroadcaster = new StatusMessageBroadcaster(processInfo);
        }

        log.debug("initialize() - end");
    }

    protected void addProcessStatusReporter(StatusReporter reporter) {
        if (processStatusBroadcaster != null) {
            log.info("Adding a process status broadcast to the system");
            processStatusBroadcaster.addStatusReporter(reporter);
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
     */
    public void setInitDatabaseService(boolean initDatabaseService) {
        this.initDatabaseService = initDatabaseService;
    }

    /**
     * Send updates to the supervisor, or (if this is the supervisor), broadcast them to all
     * clients.
     */
    public static void sendUpdates() {
        if (processStatusBroadcaster != null) {
            processStatusBroadcaster.sendUpdates();
        }
    }
}
