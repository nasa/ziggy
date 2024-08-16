package gov.nasa.ziggy.supervisor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.StartPipelineRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Provides lifecycle storage of start-pipeline requests to the supervisor and manages messages
 * associated with such requests.
 * <p>
 * Specifically, the class responds to {@link StartPipelineRequest} messages by creating a new
 * {@link Thread} for an instance of {@link PipelineInstanceManager}, which in turn starts the
 * pipeline and manages any subsequent repetitions of the pipeline that get scheduled. The class
 * responds to {@link RunningPipelinesCheckRequest} messages by checking whether there are any live
 * threads running {@link PipelineInstanceManager} instances, and, if not, broadcasting a
 * {@link NoRunningOrQueuedPipelinesMessage}.
 *
 * @author PT
 */
public class StartPipelineRequestManager {

    private static final Logger log = LoggerFactory.getLogger(StartPipelineRequestManager.class);

    private ExecutorService pipelineManagerThreads = Executors.newCachedThreadPool();

    /**
     * Constructor. Establishes a shutdown hook that interrupts any {@link Thread}s that are still
     * running active {@link PipelineInstanceManager} instances.
     */
    private StartPipelineRequestManager() {
        ZiggyShutdownHook.addShutdownHook(() -> {
            log.info("SHUTDOWN: Shutting down start-pipeline manager threads");
            pipelineManagerThreads.shutdownNow();
            log.info("SHUTDOWN: Start-pipeline manager thread shutdown complete");
        });
    }

    public static void start() {
        StartPipelineRequestManager startPipelineRequestManager = new StartPipelineRequestManager();
        ZiggyMessenger.subscribe(RunningPipelinesCheckRequest.class, message -> {
            startPipelineRequestManager.processRequest();
        });
        ZiggyMessenger.subscribe(StartPipelineRequest.class, message -> {
            startPipelineRequestManager.processRequest(message);
        });
    }

    /**
     * Launches a {@link PipelineInstanceManager} in a {@link Thread} to respond to a
     * {@link StartPipelineRequest} message.
     */
    private synchronized void processRequest(StartPipelineRequest request) {
        log.info("Got a message, processing...");
        pipelineManagerThreads.execute(() -> {
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
        ThreadPoolExecutor e = (ThreadPoolExecutor) pipelineManagerThreads;
        if (e.getActiveCount() == 0) {
            log.info("Sending no-more-instances message to console");
            ZiggyMessenger.publish(new NoRunningOrQueuedPipelinesMessage());
        }
    }
}
