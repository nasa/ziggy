package gov.nasa.ziggy.module;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

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
 * All of the methods in this class can throw the following exceptions:
 * <ol>
 * <li>If this thread is unable to communicate with the {@link SubtaskServer} after several retries,
 * it indicates that the server has failed, in which case a {@link SocketException} is thrown.
 * <li>If another thread determines that the server has failed the {@link ComputeNodeMaster} will
 * end processing on all threads, resulting in an {@link InterruptedException}.
 * <li>If the class of the response from the server cannot be instantiated, a
 * {@link ClassNotFoundException} will be thrown.
 * </ol>
 *
 * @author PT
 */
public class SubtaskClient {
    private static final Logger log = LoggerFactory.getLogger(SubtaskClient.class);

    private static final long SLEEP_TIME_MILLIS = 10000;
    private static final int MAX_ATTEMPTS = 6;

    private final String host;
    private final int serverPort;

    public SubtaskClient(String host, int serverPort) {
        log.debug("Starting new SubtaskClient host " + host + " server port " + serverPort);
        this.host = host;
        this.serverPort = serverPort;
        log.debug("New SubtaskClient started");
    }

    /**
     * Client method to report that a sub-task has completed
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     * @throws ClassNotFoundException if the response from the server could not be instantiated.
     * @throws SocketException if all attempts to reach the server have failed, which indicates that
     * the server itself has failed.
     */
    public Response reportSubTaskComplete(int subTaskIndex)
        throws ClassNotFoundException, SocketException, InterruptedException {
        return request(RequestType.REPORT_DONE, subTaskIndex);
    }

    /**
     * Client method to report that a sub-task is locked by another compute node.
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     * @throws ClassNotFoundException if the response from the server could not be instantiated.
     * @throws SocketException if all attempts to reach the server have failed, which indicates that
     * the server itself has failed.
     */
    public Response reportSubTaskLocked(int subTaskIndex)
        throws ClassNotFoundException, SocketException, InterruptedException {
        return request(RequestType.REPORT_LOCKED, subTaskIndex);
    }

    /**
     * Get the next subtask for processing.
     *
     * @throws InterruptedException if the thread was interrupted because a different thread was
     * unable to reach the subtask server.
     * @throws ClassNotFoundException if the response from the server could not be instantiated.
     * @throws SocketException if all attempts to reach the server have failed, which indicates that
     * the server itself has failed.
     * @return non-null.
     */
    public Response nextSubtask()
        throws InterruptedException, ClassNotFoundException, SocketException {
        Response response = null;

        while (true) {
            response = request(RequestType.GET_NEXT);
            if (response.status != ResponseType.TRY_AGAIN) {
                break;
            }
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException e) {
                throw e;
            }
        }
        log.debug("getNextSubTask: Got a response: " + response);
        return response;
    }

    private Socket connect() throws UnknownHostException, IOException {
        log.debug("Connecting to subtask server at: " + host);
        return new Socket(host, serverPort);
    }

    private void send(ObjectOutputStream out, RequestType command, int subTaskIndex)
        throws IOException {
        log.debug("Connected to subtask server at: " + host + ", sending request");
        Request request = new Request(command, subTaskIndex);
        out.writeObject(request);
        out.flush();
    }

    private Response receive(ObjectInputStream in) throws IOException, ClassNotFoundException {
        return (Response) in.readObject();
    }

    private void disconnect(Socket socket) throws IOException {
        socket.close();
    }

    /**
     * Performs a single attempt to make a request to the {@link SubtaskServer}.
     *
     * @return Response to request.
     * @throws IOException if any of the (connect, send, receive, disconnect) steps experiences an
     * i/o error.
     * @throws ClassNotFoundException if the class of the response cannot be found.
     * @throws InterruptedException if the thread has been interrupted.
     * @return non-null.
     */
    private Response attemptRequest(RequestType command, int subTaskIndex)
        throws IOException, ClassNotFoundException, InterruptedException {

        // If another thread has detected that the server has failed, then there will be
        // an attempt to interrupt this thread; if so, detect it and then don't even bother
        // to try to talk to the server, it's gone.
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Socket socket = connect();
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            send(out, command, subTaskIndex);
            Response response = receive(in);
            disconnect(socket);
            return response;
        }
    }

    private Response request(RequestType command)
        throws ClassNotFoundException, SocketException, InterruptedException {
        return request(command, -1);
    }

    /**
     * Send a request to the subtask server and receive a response. If the attempt fails due to i/o
     * errors, retry until MAX_ATTEMPTS is exhausted.
     *
     * @throws InterruptedException if the thread is interrupted (indicates that the
     * {@link RemoteJobMaster} has interrupted all algorithm threads).
     * @throws ClassNotFoundException if the response from the server cannot be instantiated.
     * @throws SocketException if all attempts to reach the server have failed, which indicates that
     * the server itself has failed.
     * @return non-null.
     */
    private Response request(RequestType command, int subtaskIndex)
        throws InterruptedException, ClassNotFoundException, SocketException {

        log.debug("Sending request " + command + " with subtaskIndex " + subtaskIndex);
        Response response = null;
        for (int iAttempt = 0; iAttempt < MAX_ATTEMPTS; iAttempt++) {
            try {
                response = attemptRequest(command, subtaskIndex);
                break;
            } catch (IOException ioException) {
                // swallow this exception, since we'll retry in this case
            } catch (InterruptedException interruptedException) {
                // Rethrow so the caller sees the InterruptedException
                throw interruptedException;
            }

            // pause before the next attempt to get a response
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException e1) {
                throw e1;
            }
        }

        // If we've gotten this far and response is still null, it means that the
        // attempts to get a response from the subtask server have been exhausted.
        // In this case, throw a SocketException so that the caller knows that the
        // subtask server has failed.
        if (response == null) {
            throw new SocketException("Failed to connect with SubTaskServer on host " + host
                + " after " + MAX_ATTEMPTS + " attempts");
        }
        return response;
    }
}
