package gov.nasa.ziggy.module;

import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskServer.Request;
import gov.nasa.ziggy.module.SubtaskServer.RequestType;
import gov.nasa.ziggy.module.SubtaskServer.Response;
import gov.nasa.ziggy.module.SubtaskServer.ResponseType;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Client class used to communicate with a {@link SubtaskServer} instance in another JVM. The client
 * can perform three functions: retrieve the next subtask to be processed, receive confirmation that
 * no further subtasks remain to be processed, or notify the {@link SubtaskServer} that a subtask
 * processed in this thread has completed or is locked by another job.
 * <p>
 * The class functions by putting a {@link Request} into the {@link ArrayBlockingQueue} of the
 * {@link SubtaskServer}. The {@link SubtaskServer} then puts its {@link Response} into the
 * instance's own {@link ArrayBlockingQueue}. This use of blocking queues allows both the server and
 * the client to dispense with busy loops and simply block until they are called upon to do
 * something.
 *
 * @author PT
 */
public class SubtaskClient {
    private static final Logger log = LoggerFactory.getLogger(SubtaskClient.class);

    // How long should the client wait after a TRY_AGAIN response?
    public static final long TRY_AGAIN_WAIT_TIME_MILLIS = 2000L;

    // The SubtaskClient only handles one request / response at a time, hence the
    // ArrayBlockingQueue needs only one entry.
    private final ArrayBlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(1);

    public SubtaskClient() {
        log.debug("New SubtaskClient started");
    }

    /**
     * Client method to report that a subtask has completed.
     */
    public Response reportSubtaskComplete(int subtaskIndex) {
        return request(RequestType.REPORT_DONE, subtaskIndex);
    }

    /**
     * Client method to report that a subtask is locked by another compute node.
     */
    public Response reportSubtaskLocked(int subtaskIndex) {
        return request(RequestType.REPORT_LOCKED, subtaskIndex);
    }

    /**
     * Get the next subtask for processing.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public Response nextSubtask() {
        Response response = null;

        while (true) {
            response = request(RequestType.GET_NEXT);
            if (response == null || response.status != ResponseType.TRY_AGAIN) {
                break;
            }
            try {
                Thread.sleep(tryAgainWaitTimeMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.debug("getNextSubtask: Got a response: {}", response);
        return response;
    }

    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public void submitResponse(Response response) {
        try {
            responseQueue.put(response);
        } catch (InterruptedException ignored) {
            // If this thread is ever interrupted, it's in the process of the ComputeNodeMaster
            // shutting down, hence there is nothing to be done, hence we can swallow this
            // exception.
        }
    }

    /**
     * Sends a message to the {@link SubtaskServer} that is not associated with a subtask index.
     *
     * @return non-null response
     */
    private Response request(RequestType command) {
        return request(command, -1);
    }

    /**
     * Send a request to the subtask server and receive a response.
     *
     * @return non-null response
     */
    private Response request(RequestType command, int subtaskIndex) {

        log.debug("Sending request {} with subtaskIndex {}", command, subtaskIndex);
        send(command, subtaskIndex);
        return receive();
    }

    /**
     * Sends a request to the {@link SubtaskServer}. This is acomplished by creating a new instance
     * of {@link Request}, which is then put onto the server's {@link ArrayBlockingQueue}.
     */
    private void send(RequestType command, int subtaskIndex) {
        log.debug("Connected to subtask server, sending request");
        Request request = new Request(command, subtaskIndex, this);
        SubtaskServer.submitRequest(request);
    }

    /**
     * Receives a {@link Response) from the {@link SubtaskServer} to a prior {@link Request}. The
     * method will block until the server responds or an {@link InterruptedException} occurs.
     */
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    private Response receive() {
        try {
            return responseQueue.take();
        } catch (InterruptedException ignored) {
            // Here we can swallow the InterruptedException. If this thread has been
            // interrupted, it means that the entire ComputeNodeMaster is in the process
            // of shutting down, so we can simply return null and exit.
            return null;
        }
    }

    long tryAgainWaitTimeMillis() {
        return TRY_AGAIN_WAIT_TIME_MILLIS;
    }
}
