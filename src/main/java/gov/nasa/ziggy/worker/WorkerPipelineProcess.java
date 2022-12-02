package gov.nasa.ziggy.worker;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricsDumper;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.WorkerMemoryManager;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud.ClearStaleStateResults;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.events.ZiggyEventCrud;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.WorkerCommunicator;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.messaging.TriggerRequestManager;
import gov.nasa.ziggy.worker.messaging.WorkerMessageDispatcher;
import hdf.hdf5lib.H5;

/**
 * @author Todd Klaus
 * @author PT
 */
public class WorkerPipelineProcess extends AbstractPipelineProcess {
    public static final String NAME = "Worker";

    private static final Logger log = LoggerFactory.getLogger(WorkerPipelineProcess.class);

    public static final String WORKER_STATUS_REPORT_INTERVAL_MILLIS_PROP = "services.statusReport.workerThread.reportIntervalMillis";
    public static final int WORKER_STATUS_REPORT_INTERVAL_MILLIS_DEFAULT = 15000;

    private static final String WORKER_CLEAN_TMP_AT_STARTUP_PROP = "pi.worker.cleanTmp.enabled";
    private static final boolean WORKER_CLEAN_TMP_AT_STARTUP_DEFAULT = false;

    private WorkerMemoryManager memoryManager = null;
    private static int workerThreadCount;
    private static long heapSize;
    private static final Set<ZiggyEventHandler> ziggyEventHandlers = new HashSet<>();
    public static final List<WorkerTaskRequestHandler> workerThreads = new LinkedList<>();

    public static final PriorityBlockingQueue<WorkerTaskRequest> workerTaskRequestQueue = new PriorityBlockingQueue<>();

    public WorkerPipelineProcess(int threadCount) {
        super(NAME);
        checkArgument(threadCount > 0, "workerThreadCount must be positive");
        workerThreadCount = threadCount;
        heapSize = Runtime.getRuntime().maxMemory();
    }

    public WorkerPipelineProcess(boolean messaging, boolean database) {
        super(NAME, messaging, database);
    }

    public void go() {
        try {
            initialize();

            Configuration config = ZiggyConfiguration.getInstance();

            clearStaleTaskStates(getProcessInfo().getHost());

            clearTemporaryDirs();

            // if HDF5 is to be used as the default binfile format (or indeed at all),
            // load the library now -- note that (a) this is necessary because in linux
            // some kind of conflict occurs when libhdf5 is loaded before libjhdf5, but not
            // vice versa; (b) use of the System.loadLibrary doesn't work because at the first
            // call to an HDF5 method, the code looks at a private static var that tells it
            // whether the library is loaded, and that var is false (because H5 didn't load it).
            // It doesn't look to see whether the library was loaded by somebody other than H5.
            // It thus attempts to load again and we get the conflict.

            H5.loadH5Lib();

            try {
                memoryManager = new WorkerMemoryManager();
            } catch (IOException e) {
                log.warn("unable to determine system physical memory, disabling memory manager", e);
            }

            int rmiPort = config.getInt(MessageHandler.RMI_REGISTRY_PORT_PROP,
                MessageHandler.RMI_REGISTRY_PORT_PROP_DEFAULT);
            log.info("Starting RMI communications server with registry on port " + rmiPort);
            WorkerCommunicator.initializeInstance(
                new MessageHandler(new WorkerMessageDispatcher(new TriggerRequestManager())),
                rmiPort);

            log.info("Starting " + workerThreadCount + " worker task threads");

            for (int i = 0; i < workerThreadCount; i++) {
                log.info("Starting worker task thread #" + (i + 1) + " of " + workerThreadCount);
                WorkerTaskRequestHandler workerThread = new WorkerTaskRequestHandler(
                    getProcessInfo(), i, memoryManager);

                /*
                 * Add this taskHandler to the ProcessStatusBroadcaster so that it will be
                 * periodically queried for state
                 */
                int workerReportIntervalMillis = config.getInt(
                    WORKER_STATUS_REPORT_INTERVAL_MILLIS_PROP,
                    WORKER_STATUS_REPORT_INTERVAL_MILLIS_DEFAULT);
                log.info("Adding worker thread to status reporter");
                addProcessStatusReporter(workerThread.getTaskDispatcher(),
                    workerReportIntervalMillis);

                workerThread.start();

                workerThreads.add(workerThread);
            }

            log.info("Adding shutdown hook");
            ZiggyShutdownHook
                .addShutdownHook(new WorkerShutdownHook(getProcessInfo().getPid(), workerThreads));

            log.info("Starting metrics dumper thread...");
            MetricsDumper metricsDumper = new MetricsDumper(
                AbstractPipelineProcess.getProcessInfo().getPid());
            Thread metricsDumperThread = new Thread(metricsDumper, "MetricsDumper");
            metricsDumperThread.setDaemon(true);
            metricsDumperThread.start();

            log.info("Starting algorithm monitor threads...");
            AlgorithmMonitor.initialize();

            log.info("Loading event handlers...");
            ziggyEventHandlers.addAll(new ZiggyEventCrud().retrieveAllEventHandlers());
            for (ZiggyEventHandler handler : ziggyEventHandlers) {
                if (handler.isEnableOnClusterStart()) {
                    handler.start();
                }
            }
            log.info("Loading event handlers...done");

        } catch (Exception e) {
            log.error("Initialization failed!", e);
            System.exit(-1);
        }
    }

    private void clearTemporaryDirs() {
        Configuration config = ZiggyConfiguration.getInstance();
        boolean enabled = config.getBoolean(WORKER_CLEAN_TMP_AT_STARTUP_PROP,
            WORKER_CLEAN_TMP_AT_STARTUP_DEFAULT);

        if (enabled) {
            log.info("Cleaning worker tmp dir");

            try {
                File dataDir = DirectoryProperties.taskDataDir().toFile();

                FileUtils.cleanDirectory(dataDir);
            } catch (IOException e) {
                log.warn("Failed to clean worker tmp dir, caught e = " + e, e);
            }
        }
    }

    /**
     * Set the state to ERROR for all tasks assigned to this worker where the state is currently
     * PROCESSING or SUBMITTED. Used at worker startup to reset the state after an abnormal worker
     * exit while processing.
     *
     * @param workerHost
     */
    public void clearStaleTaskStates(String workerHost) {

        /*
         * Set the pipeline task state to ERROR for any tasks assigned to this worker that are in
         * the PROCESSING state or the SUBMITTED state. These conditions indicate that the previous
         * instance of the worker process on this host died abnormally
         */
        ClearStaleStateResults clearStateResults = (ClearStaleStateResults) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
                return pipelineTaskCrud.clearStaleState(workerHost);
            });

        /*
         * Update the pipeline instance state for the instances associated with the stale tasks from
         * above since that change may result in a change to the instances
         */

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineExecutor pe = new PipelineExecutor();
            PipelineInstanceCrud instanceCrud = new PipelineInstanceCrud();

            for (Long instanceId : clearStateResults.uniqueInstanceIds) {
                log.info("Updating instance state for instanceId = " + instanceId);
                PipelineInstance instance = instanceCrud.retrieve(instanceId);
                pe.updateInstanceState(instance);
            }
            return null;
        });

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        int workerThreads = Integer.parseInt(args[0]);
        WorkerPipelineProcess p = new WorkerPipelineProcess(workerThreads);
        p.go();
    }

    public static int workerThreadCount() {
        return workerThreadCount;
    }

    public static long heapSize() {
        return heapSize;
    }

    public static Set<ZiggyEventHandler> ziggyEventHandlers() {
        return ziggyEventHandlers;
    }

    public static Set<ZiggyEventHandlerInfoForDisplay> serializableZiggyEventHandlers() {
        return ziggyEventHandlers.stream()
            .map(s -> new ZiggyEventHandlerInfoForDisplay(s))
            .collect(Collectors.toSet());
    }

}
