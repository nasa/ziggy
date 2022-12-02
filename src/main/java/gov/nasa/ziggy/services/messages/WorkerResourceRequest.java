package gov.nasa.ziggy.services.messages;

import java.io.Serializable;

import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Message sent from the client to the worker requesting the number of worker threads and the Java
 * heap size. There's no message content other than the message itself.
 *
 * @author PT
 */
public class WorkerResourceRequest extends PipelineMessage {

    private static final long serialVersionUID = 20220614L;

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return messageHandler.handleResourceRequest();
    }

    public static class WorkerResources implements Serializable {

        private static final long serialVersionUID = 20220614L;

        private final int workerThreads;
        private final long heapSize;

        public WorkerResources(int workerThreads, long heapSize) {
            this.workerThreads = workerThreads;
            this.heapSize = heapSize;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public long getHeapSize() {
            return heapSize;
        }
    }
}
