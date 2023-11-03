package gov.nasa.ziggy.supervisor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.FireTriggerRequest;
import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Provides lifecycle storage of trigger requests to the worker and manages messages associated with
 * such requests.
 * <p>
 * Specifically, the class responds to {@link FireTriggerRequest} messages by creating a new
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
    private TriggerRequestManager() {
        ZiggyShutdownHook.addShutdownHook(() -> {
            log.info("SHUTDOWN: Shutting down trigger manager threads");
            triggerManagerThreads.shutdownNow();
            log.info("SHUTDOWN: Trigger manager thread shutdown complete");
        });
    }

    public static void start() {
        TriggerRequestManager triggerRequestManager = new TriggerRequestManager();
        ZiggyMessenger.subscribe(RunningPipelinesCheckRequest.class, message -> {
            triggerRequestManager.processRequest();
        });
        ZiggyMessenger.subscribe(FireTriggerRequest.class, message -> {
            triggerRequestManager.processRequest(message);
        });
    }

    /**
     * Launches a {@link PipelineInstanceManager} in a {@link Thread} to respond to a
     * {@link FireTriggerRequest} message.
     */
    private synchronized void processRequest(FireTriggerRequest request) {
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
    private synchronized void processRequest() {
        ThreadPoolExecutor e = (ThreadPoolExecutor) triggerManagerThreads;
        if (e.getActiveCount() == 0) {
            log.info("Sending no-more-instances message to console");
            ZiggyMessenger.publish(new NoRunningOrQueuedPipelinesMessage());
        }
    }
}
