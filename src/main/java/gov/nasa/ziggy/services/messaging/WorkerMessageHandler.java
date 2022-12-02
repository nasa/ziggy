package gov.nasa.ziggy.services.messaging;

import java.io.IOException;
import java.util.Set;

import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.DeleteTasksRequest;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.messages.WorkerResourceRequest;
import gov.nasa.ziggy.services.messages.WorkerSingleTaskLogRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskLogInformationRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;

/**
 * Provides worker-side abstraction for processing message requests from a client.
 *
 * @author PT
 * @author Bill Wohler
 */
public interface WorkerMessageHandler {

    void handleTriggerRequest(WorkerFireTriggerRequest fireTriggerRequest);

    void handleRunningPipelinesCheckRequest(RunningPipelinesCheckRequest checkRequest);

    void handleWorkerTaskRequest(WorkerTaskRequest request);

    void handleStartMemdroneRequest(StartMemdroneRequest request);

    Set<TaskLogInformation> handleTaskLogInformationRequest(
        WorkerTaskLogInformationRequest request);

    String handleSingleTaskLogRequest(WorkerSingleTaskLogRequest request);

    WorkerResourceRequest.WorkerResources handleResourceRequest();

    Set<ZiggyEventHandlerInfoForDisplay> handleEventHandlerRequest();

    void handleEventHandlerToggleRequest(String eventHandlerName);

    boolean deleteTasks(DeleteTasksRequest request) throws IOException;

}
