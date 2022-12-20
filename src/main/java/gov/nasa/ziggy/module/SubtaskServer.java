package gov.nasa.ziggy.module;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves sub-tasks to clients using {@link SubtaskAllocator}. Clients should use
 * {@link SubtaskClient} to communicate with an instance of this class.
 *
 * @author Todd Klaus
 * @author PT
 */
public class SubtaskServer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SubtaskServer.class);

    private static final int MAX_EXCEPTIONS = 100;

    private final String host;
    private ServerSocket serverSocket;
    private SubtaskAllocator subtaskAllocator = null;
    private final CountDownLatch serverThreadReady = new CountDownLatch(1);
    private boolean shuttingDown = false;
    private int serverPort;
    private TaskConfigurationManager inputsHandler;

    public SubtaskServer(String host, TaskConfigurationManager inputsHandler) {
        this.host = host;
        this.inputsHandler = inputsHandler;
        subtaskAllocator = new SubtaskAllocator(inputsHandler);

        if (subtaskAllocator.isEmpty()) {
            throw new PipelineException("InputsHandler contains no elements!");
        }

    }

    public void startSubtaskServer() throws InterruptedException {
        log.info("Starting SubtaskServer for inputs: " + inputsHandler);

        Thread t = new Thread(this, "SubtaskServer-listener");
        t.setDaemon(true);
        t.start();

        serverThreadReady.await();

        log.info("SubtaskServer thread ready");
    }

    public int getServerPort() {
        return serverPort;
    }

    public void shutdownServer() throws Exception {
        shuttingDown = true;
        if (serverSocket != null) {
            serverSocket.close();
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

    public static final class Request implements Serializable {
        private static final long serialVersionUID = -3336544526225919889L;

        public static final int NONE = -1;

        public RequestType type;
        public int subtaskIndex;

        public Request(RequestType type, int subTaskIndex) {
            this.type = type;
            subtaskIndex = subTaskIndex;
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

    public static final class Response implements Serializable {
        private static final long serialVersionUID = 4517789890439531336L;

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

    @Override
    public void run() {
        listen();
    }

    private void listen() {
        log.info("Initializing SubtaskServer server thread");

        try {
            serverSocket = new ServerSocket(0);
            serverPort = serverSocket.getLocalPort();
        } catch (IOException e) {
            log.error("Cannot initialize, caught: " + e);
            return;
        }

        serverThreadReady.countDown();

        log.info("Listening for connections on: " + host + ":" + serverPort);
        int exceptionCount = 0;

        while (true) {
            try (Socket clientSocket = serverSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())) {
                log.debug("Accepted new connection: " + clientSocket.toString());

                Request request = (Request) in.readObject();

                log.debug("listen[server,before]: request: " + request);

                Response response = null;

                RequestType type = request.type;

                if (type == RequestType.GET_NEXT) {
                    SubtaskAllocation nextSubtask = subtaskAllocator.nextSubtask();

                    log.debug("Allocated: " + nextSubtask);

                    ResponseType status = nextSubtask.getStatus();
                    int subtaskIndex = nextSubtask.getSubtaskIndex();

                    response = new Response(status, subtaskIndex);
                } else if (type == RequestType.REPORT_DONE) {
                    subtaskAllocator.markSubtaskComplete(request.subtaskIndex);
                    response = new Response(ResponseType.OK);
                } else if (type == RequestType.NOOP) {
                    log.debug("Got a NO-OP");
                    response = new Response(ResponseType.OK);
                } else if (type == RequestType.REPORT_LOCKED) {
                    subtaskAllocator.markSubtaskLocked(request.subtaskIndex);
                    response = new Response(ResponseType.OK);
                } else {
                    log.error("Unknown command: " + type);
                }

                log.debug("listen[server,after], response: " + response);

                out.writeObject(response);
            } catch (IOException | ClassNotFoundException e) {
                if (shuttingDown) {
                    log.info("Got shutdown signal, exiting server thread");
                    return;
                }
                exceptionCount++;
                log.error("Caught e = " + e, e);
                if (exceptionCount >= MAX_EXCEPTIONS) {
                    log.error("Max SubtaskServer exceptions " + MAX_EXCEPTIONS
                        + " exceeded, server exiting");
                    break;
                }
            }
        }
    }
}
