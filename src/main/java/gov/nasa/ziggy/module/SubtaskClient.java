package gov.nasa.ziggy.module;

import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskServer.Request;
import gov.nasa.ziggy.module.SubtaskServer.RequestType;
import gov.nasa.ziggy.module.SubtaskServer.Response;
import gov.nasa.ziggy.module.SubtaskServer.ResponseType;

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
 * <p>
 * All of the methods in this class can throw {@link InterruptedException}. This indicates that the
 * {@link ComputeNodeMaster} is attempting to shut down all the instances of {@link SubtaskMaster},
 * so any instances of {@link SubtaskClient} that are blocked and waiting for the
 * {@link SubtaskServer} need to be interrupted.
 *
 * @author PT
 */
public class SubtaskClient {
    private static final Logger log = LoggerFactory.getLogger(SubtaskClient.class);

    // The SubtaskClient only handles one request / response at a time, hence the
    // ArrayBlockingQueue needs only one entry.
    private final ArrayBlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(1);

    public SubtaskClient() {
        log.debug("New SubtaskClient started");
    }

    /**
     * Client method to report that a sub-task has completed
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     */
    public Response reportSubtaskComplete(int subTaskIndex) throws InterruptedException {
        return request(RequestType.REPORT_DONE, subTaskIndex);
    }

    /**
     * Client method to report that a sub-task is locked by another compute node.
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     */
    public Response reportSubtaskLocked(int subTaskIndex) throws InterruptedException {
        return request(RequestType.REPORT_LOCKED, subTaskIndex);
    }

    /**
     * Get the next subtask for processing.
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     * @return non-null.
     */
    public Response nextSubtask() throws InterruptedException {
        Response response = null;

        while (true) {
            response = request(RequestType.GET_NEXT);
            if (response.status != ResponseType.TRY_AGAIN) {
                break;
            }
        }
        log.debug("getNextSubTask: Got a response: " + response);
        return response;
    }

    public void submitResponse(Response response) throws InterruptedException {
        responseQueue.put(response);
    }

    /**
     * Sends a message to the {@link SubtaskServer} that is not associated with a subtask index.
     *
     * @return non-null.
     * @throws InterruptedException
     */
    private Response request(RequestType command) throws InterruptedException {
        return request(command, -1);
    }

    /**
     * Send a request to the subtask server and receive a response.
     *
     * @throws InterruptedException if the thread is interrupted (indicates that the
     * {@link ComputeNodeMaster} has interrupted all algorithm threads).
     * @return non-null.
     */
    private Response request(RequestType command, int subtaskIndex) throws InterruptedException {

        log.debug("Sending request " + command + " with subtaskIndex " + subtaskIndex);
        // If another thread has detected that the server has failed, then there will be
        // an attempt to interrupt this thread; if so, detect it and then don't even bother
        // to try to talk to the server, it's gone.
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        send(command, subtaskIndex);
        return receive();
    }

    /**
     * Sends a request to the {@link SubtaskServer}. This is acomplished by creating a new instance
     * of {@link Request}, which is then put onto the server's {@link ArrayBlockingQueue}.
     */
    private void send(RequestType command, int subTaskIndex) throws InterruptedException {
        log.debug("Connected to subtask server, sending request");
        Request request = new Request(command, subTaskIndex, this);
        SubtaskServer.submitRequest(request);
    }

    /**
     * Receives a {@link Response) from the {@link SubtaskServer} to a prior {@link Request}. The
     * method will block until the server responds or an {@link InterruptedException} occurs.
     */
    private Response receive() throws InterruptedException {
        return responseQueue.take();
    }

}
