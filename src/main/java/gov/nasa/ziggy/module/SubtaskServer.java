package gov.nasa.ziggy.module;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Serves sub-tasks to clients using {@link SubtaskAllocator}. Clients should use
 * {@link SubtaskClient} to communicate with an instance of this class.
 *
 * @author Todd Klaus
 * @author PT
 */
public class SubtaskServer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SubtaskServer.class);

    // Don't allow the request queue to be null, so initialize to the smallest
    // ArrayBlockingQueue possible, which has 1 entry.
    private static ArrayBlockingQueue<Request> requestQueue = new ArrayBlockingQueue<>(1);

    private SubtaskAllocator subtaskAllocator;
    private final CountDownLatch serverThreadReady = new CountDownLatch(1);
    private TaskConfigurationManager inputsHandler;
    private Thread listenerThread;

    public SubtaskServer(int subtaskMasterCount, TaskConfigurationManager inputsHandler) {
        initializeRequestQueue(subtaskMasterCount);
        this.inputsHandler = inputsHandler;
    }

    public static void initializeRequestQueue(int subtaskMasterCount) {
        requestQueue = new ArrayBlockingQueue<>(subtaskMasterCount);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void start() {
        log.info("Starting SubtaskServer for inputs: " + inputsHandler);

        try {
            // NB: if the listener thread constructor and setDaemon() calls are moved
            // to the class constructor, above, then the listener process will fail.
            // Specifically, SubtaskClient requests will never get answered. I don't
            // know why this should matter, but it does.
            listenerThread = new Thread(this, "SubtaskServer-listener");
            listenerThread.setDaemon(true);
            listenerThread.start();
            serverThreadReady.await();
            log.info("SubtaskServer thread ready");
        } catch (InterruptedException e) {
            throw new PipelineException("SubtaskServer start interrupted", e);
        }
    }

    // request commands
    public enum RequestType {
        NOOP, GET_NEXT, REPORT_DONE, REPORT_LOCKED;
    }

    // request commands
    public enum ResponseType {
        OK, TRY_AGAIN, NO_MORE;
    }

    /**
     * Shuts down the listener thread. This is accomplished by interrupting the listener thread,
     * which will cause the {@link ArrayBlockingQueue#take()} call in the listener to terminate with
     * an {@link InterruptException}.
     */
    public void shutdown() {
        listenerThread.interrupt();
    }

    public boolean isListenerRunning() {
        return listenerThread != null && listenerThread.isAlive();
    }

    /**
     * Adds a {@link Request} to the queue. The {@link SubtaskClient} that submits the request will
     * block until the request is accepted into the queue.
     *
     * @param request
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static void submitRequest(Request request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            // This can never occur. It happens if the put() operation is waiting
            // for a slot to open up in the blocking queue, and the thread is
            // interrupted while waiting. Because the queue size equals the number
            // of SubtaskMaster instances, even if all the SubtaskMasters submit
            // requests simultaneously there will be room enough in the queue for
            // all the requests, and a SubtaskMaster can never have more than 1
            // request on the queue at a time.
            throw new AssertionError(e);
        }
    }

    public static final class Request {

        public static final int NONE = -1;

        public RequestType type;
        public int subtaskIndex;
        public SubtaskClient client;

        public Request(RequestType type, int subtaskIndex, SubtaskClient client) {
            this.type = type;
            this.subtaskIndex = subtaskIndex;
            this.client = client;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Request [type=");
            sb.append(type);
            sb.append(", subtaskIndex=");
            sb.append(subtaskIndex);
            sb.append("]");

            return sb.toString();
        }
    }

    public static final class Response {

        public ResponseType status;
        public int subtaskIndex = -1;

        public Response(ResponseType status) {
            this.status = status;
        }

        public Response(ResponseType status, int subTaskIndex) {
            this.status = status;
            subtaskIndex = subTaskIndex;
        }

        public boolean successful() {
            return status == ResponseType.OK;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Response [status=");
            sb.append(status);
            sb.append(", subTaskIndex=");
            sb.append(subtaskIndex);
            sb.append("]");

            return sb.toString();
        }
    }

    // Implements the request listener loop.
    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_IN_RUNNABLE)
    public void run() {
        log.info("Initializing SubtaskServer server thread");
        serverThreadReady.countDown();

        while (true) {

            // If the ComputeNodeMaster is shutting down, we can detect it here
            // and halt the loop.
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            try {

                // Retrieve the next request, or block until one is provided.
                Request request = requestQueue.take();

                log.debug("listen[server,before]: request: " + request);

                Response response = null;

                RequestType type = request.type;

                if (type == RequestType.GET_NEXT) {
                    SubtaskAllocation nextSubtask = subtaskAllocator().nextSubtask();

                    log.debug("Allocated: " + nextSubtask);

                    ResponseType status = nextSubtask.getStatus();
                    int subtaskIndex = nextSubtask.getSubtaskIndex();

                    response = new Response(status, subtaskIndex);
                } else if (type == RequestType.REPORT_DONE) {
                    subtaskAllocator().markSubtaskComplete(request.subtaskIndex);
                    response = new Response(ResponseType.OK);
                } else if (type == RequestType.NOOP) {
                    log.debug("Got a NO-OP");
                    response = new Response(ResponseType.OK);
                } else if (type == RequestType.REPORT_LOCKED) {
                    subtaskAllocator().markSubtaskLocked(request.subtaskIndex);
                    response = new Response(ResponseType.OK);
                } else {
                    log.error("Unknown command: " + type);
                }

                log.debug("listen[server,after], response: " + response);

                // Send the response back to the client.
                request.client.submitResponse(response);
            } catch (InterruptedException e) {
                // If the ComputeNodeMaster started to shut down while
                // this loop was blocked at the take() call, above, execution
                // will end up here and we can exit the loop.
                break;
            }
        }
    }

    // For testing only.
    static ArrayBlockingQueue<Request> getRequestQueue() {
        return requestQueue;
    }

    // For testing only.
    Thread getListenerThread() {
        return listenerThread;
    }

    /**
     * Returns a {@link SubtaskAllocator} for the server. Package scope so that the allocator can be
     * replaced with a mocked instance.
     */
    SubtaskAllocator subtaskAllocator() {
        if (subtaskAllocator == null) {
            subtaskAllocator = new SubtaskAllocator(inputsHandler);
            if (subtaskAllocator.isEmpty()) {
                throw new PipelineException("InputsHandler contains no elements!");
            }
        }
        return subtaskAllocator;
    }
}
