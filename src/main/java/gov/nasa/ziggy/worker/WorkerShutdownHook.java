package gov.nasa.ziggy.worker;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shutdown hook for the pipeline worker process. Responsible for killing any running MATLAB
 * external processes and rolling back outstanding transactions.
 *
 * @author Todd Klaus
 */
public class WorkerShutdownHook implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerShutdownHook.class);

    private final List<WorkerTaskRequestHandler> workerThreads;

    public WorkerShutdownHook(int workerPid, List<WorkerTaskRequestHandler> workerThreads) {
        this.workerThreads = workerThreads;
    }

    @Override
    public void run() {
        try {
            int threadIndex = 0;
            for (WorkerTaskRequestHandler workerThread : workerThreads) {
                log.info("SHUTDOWN: Shutting down worker thread #" + threadIndex + "...");
                workerThread.interrupt();
                threadIndex++;
            }

            log.info("SHUTDOWN: waiting for tasks to rollback...");
            Thread.sleep(5000);

            log.info("SHUTDOWN: exiting");
        } catch (Throwable t) {
            log.error("SHUTDOWN: Caught exception during shutdown", t);
        }
    }
}
