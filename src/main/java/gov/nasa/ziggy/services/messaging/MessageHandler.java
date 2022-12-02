package gov.nasa.ziggy.services.messaging;

import java.io.IOException;
import java.util.Set;

import gov.nasa.ziggy.services.alert.AlertMessage;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.DeleteTasksRequest;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messages.WorkerResourceRequest;
import gov.nasa.ziggy.services.messages.WorkerShutdownMessage;
import gov.nasa.ziggy.services.messages.WorkerSingleTaskLogRequest;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messages.WorkerTaskLogInformationRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;

/**
 * Provides support for handling messages. Specifically, the {@link MessageHandler} instance
 * provides class instances or other information / objects that are needed for the handleMessage()
 * method that each {@link PipelineMessage} provides. The {@link PipelineMessage} method does the
 * actual work of handling the message. This saves a lot of tedious if-then blocks by forcing each
 * subclass of {@link PipelineMessage} to provide all the class-specific handling code it requires.
 * <p>
 * The {@link MessageHandler} class is part of the Remote Method Invocation (RMI) communication
 * between the client and the worker. For this reason it must implement the
 * {@link MessageHandlerService} interface.
 * <p>
 * Consider extending UnicastRemoteObject if we ever have to implement equals() and hashCode().
 * According to Bill Grosso's Java RMI book, implementing these methods in stubs is "tricky."
 *
 * @author PT
 * @author Bill Wohler
 */
public class MessageHandler implements MessageHandlerService {

    public static final String RMI_REGISTRY_PORT_PROP = "rmi.registry.port";
    public static final int RMI_REGISTRY_PORT_PROP_DEFAULT = 1099;

    protected ClientMessageHandler clientMessageHandler;
    protected WorkerMessageHandler workerMessageHandler;

    /**
     * Time at which the last heartbeat from the worker was detected by the UI. This is used to
     * determine whether the worker has failed and a new connection must be established.
     */
    private volatile long lastHeartbeatTimeMillis;

    /**
     * Constructor used by clients. This requires a valid {@link ClientMessageHandler} instance
     * which can do the work of handling messages received by the clients.
     */
    public MessageHandler(ClientMessageHandler clientMessageHandler) {
        this.clientMessageHandler = clientMessageHandler;
    }

    /**
     * Constructor used by the worker. This requires a valid {@link WorkerMessageHandler} instance
     * which can do the work of handling messages received by the worker.
     */
    public MessageHandler(WorkerMessageHandler workerMessageHandler) {
        this.workerMessageHandler = workerMessageHandler;
    }

    @Override
    public Object handleMessage(PipelineMessage message) {
        return message.handleMessage(this);
    }

    public void setLastHeartbeatTimeMillis(long lastHeartbeatTime) {
        lastHeartbeatTimeMillis = lastHeartbeatTime;
    }

    public long getLastHeartbeatTimeMillis() {
        return lastHeartbeatTimeMillis;
    }

    public void resetLastHeartbeatTime() {
        lastHeartbeatTimeMillis = 0L;
    }

    public long heartbeatIntervalMillis() {
        return WorkerHeartbeatMessage.heartbeatIntervalMillis();
    }

    public void handleAlert(AlertMessage message) {
        clientMessageHandler.handleAlert(message);
    }

    public void handleWorkerStatusMessage(WorkerStatusMessage message) {
        clientMessageHandler.handleWorkerStatusMessage(message);
    }

    public void handleShutdownMessage(WorkerShutdownMessage message) {
        clientMessageHandler.handleShutdownMessage(message);
    }

    public void handleNoRunningOrQueuedPipelinesMessage() {
        clientMessageHandler.handleNoRunningOrQueuedPipelinesMessage();
    }

    public void handleTriggerRequest(WorkerFireTriggerRequest fireTriggerRequest) {
        workerMessageHandler.handleTriggerRequest(fireTriggerRequest);
    }

    public void handleRunningPipelinesCheckRequest(RunningPipelinesCheckRequest checkRequest) {
        workerMessageHandler.handleRunningPipelinesCheckRequest(checkRequest);
    }

    public void handleWorkerTaskRequest(WorkerTaskRequest request) {
        workerMessageHandler.handleWorkerTaskRequest(request);
    }

    public void handleStartMemdroneRequest(StartMemdroneRequest request) {
        workerMessageHandler.handleStartMemdroneRequest(request);
    }

    public Set<TaskLogInformation> handleTaskLogInformationRequest(
        WorkerTaskLogInformationRequest request) {
        return workerMessageHandler.handleTaskLogInformationRequest(request);
    }

    public String handleSingleTaskLogRequest(WorkerSingleTaskLogRequest request) {
        return workerMessageHandler.handleSingleTaskLogRequest(request);
    }

    public WorkerResourceRequest.WorkerResources handleResourceRequest() {
        return workerMessageHandler.handleResourceRequest();
    }

    public Set<ZiggyEventHandlerInfoForDisplay> handleEventHandlerRequest() {
        return workerMessageHandler.handleEventHandlerRequest();
    }

    public void handleEventHandlerToggleRequest(String handlerName) {
        workerMessageHandler.handleEventHandlerToggleRequest(handlerName);
    }

    public boolean deleteTasks(DeleteTasksRequest request) throws IOException {
        return workerMessageHandler.deleteTasks(request);
    }

}
