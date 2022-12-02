package gov.nasa.ziggy.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.WorkerMemoryManager;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.process.ProcessInfo;

/**
 * Interacts with the worker task queue to handle task requests. Hands off incoming messages to the
 * {@link WorkerTaskRequestDispatcher} for processing.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 * @author PT
 */
public class WorkerTaskRequestHandler extends Thread {
    private static final Logger log = LoggerFactory.getLogger(WorkerTaskRequestHandler.class);

    private WorkerTaskRequestDispatcher taskDispatcher = null;

    public WorkerTaskRequestHandler(ProcessInfo processInfo, int threadNum,
        WorkerMemoryManager memoryManager) {
        super("task-" + threadNum);

        ZiggyConfiguration.getInstance();

        taskDispatcher = new WorkerTaskRequestDispatcher(processInfo, threadNum, memoryManager);
    }

    @Override
    public void run() {
        log.debug("run() - start");

        while (!isInterrupted()) {
            try {
                WorkerTaskRequest request = WorkerPipelineProcess.workerTaskRequestQueue.take();
                log.info("Retrieved a request, processing");
                taskDispatcher.processMessage(request);
            } catch (InterruptedException e) {

                // If there was an interrupted exception, re-interrupt the thread so that
                // this loop will exit.
                interrupt();
            }
        }
    }

    /**
     * @return the taskDispatcher
     */
    public WorkerTaskRequestDispatcher getTaskDispatcher() {
        return taskDispatcher;
    }

}
