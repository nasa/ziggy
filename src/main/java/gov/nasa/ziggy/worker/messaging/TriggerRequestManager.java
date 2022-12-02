package gov.nasa.ziggy.worker.messaging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.messaging.WorkerCommunicator;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.PipelineInstanceManager;

/**
 * Provides lifecycle storage of trigger requests to the worker and manages messages associated with
 * such requests.
 * <p>
 * Specifically, the class responds to {@link WorkerFireTriggerRequest} messages by creating a new
 * {@link Thread} for an instance of {@link PipelineInstanceManager}, which in turn fires the
 * trigger and manages any subsequent re-firings of the trigger that get scheduled. The class
 * responds to {@link RunningPipelinesCheckRequest} messages by checking whether there are any live
 * threads running {@link PipelineInstanceManager} instances, and, if not, broadcasting a
 * {@link NoRunningOrQueuedPipelinesMessage}.
 *
 * @author PT
 */
public class TriggerRequestManager {

    private static final Logger log = LoggerFactory.getLogger(TriggerRequestManager.class);

    private ExecutorService triggerManagerThreads = Executors.newCachedThreadPool();

    /**
     * Constructor. Establishes a shutdown hook that interrupts any {@link Thread}s that are still
     * running active {@link PipelineInstanceManager} instances.
     */
    public TriggerRequestManager() {
        ZiggyShutdownHook.addShutdownHook(() -> {
            log.info("SHUTDOWN: Shutting down trigger manager threads");
            triggerManagerThreads.shutdownNow();
            log.info("SHUTDOWN: Trigger manager thread shutdown complete");
        });
    }

    /**
     * Launches a {@link PipelineInstanceManager} in a {@link Thread} to respond to a
     * {@link WorkerFireTriggerRequest} message.
     */
    public synchronized void processRequest(WorkerFireTriggerRequest request) {
        log.info("Got a message, processing...");
        triggerManagerThreads.execute(() -> {
            new PipelineInstanceManager(request).fireTrigger();
        });
    }

    /**
     * Checks for live {@link Thread}s with {@link PipelineInstanceManager} instances, and if none
     * are found, broadcasts a {@link NoRunningOrQueuedPipelinesMessage}. This tells the console
     * that the current run(s) are really and truly complete and it can thus turn the pipeline
     * "idiot light" to dark gray.
     */
    public synchronized void processRequest(RunningPipelinesCheckRequest request) {
        ThreadPoolExecutor e = (ThreadPoolExecutor) triggerManagerThreads;
        if (e.getActiveCount() == 0) {
            log.info("Sending no-more-instances message to console");
            WorkerCommunicator.broadcast(new NoRunningOrQueuedPipelinesMessage());
        }
    }

}
