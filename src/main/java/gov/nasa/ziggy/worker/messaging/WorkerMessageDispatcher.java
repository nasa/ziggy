package gov.nasa.ziggy.worker.messaging;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.DeleteTasksRequest;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.messages.WorkerResourceRequest;
import gov.nasa.ziggy.services.messages.WorkerSingleTaskLogRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskLogInformationRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.messaging.WorkerMessageHandler;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;

/**
 * Provides worker-side abstraction for processing message requests from a client.
 *
 * @author PT
 */
public class WorkerMessageDispatcher implements WorkerMessageHandler {

    Logger log = LoggerFactory.getLogger(WorkerMessageDispatcher.class);

    private TriggerRequestManager triggerRequestManager;

    public WorkerMessageDispatcher(TriggerRequestManager triggerRequestManager) {
        this.triggerRequestManager = triggerRequestManager;
    }

    @Override
    public synchronized void handleTriggerRequest(WorkerFireTriggerRequest fireTriggerRequest) {
        triggerRequestManager.processRequest(fireTriggerRequest);
    }

    @Override
    public synchronized void handleRunningPipelinesCheckRequest(
        RunningPipelinesCheckRequest checkRequest) {
        triggerRequestManager.processRequest(checkRequest);
    }

    @Override
    public synchronized void handleWorkerTaskRequest(WorkerTaskRequest request) {
        WorkerPipelineProcess.workerTaskRequestQueue.put(request);
    }

    @Override
    public synchronized void handleStartMemdroneRequest(StartMemdroneRequest request) {
        try {
            if (Memdrone.memdroneEnabled()) {
                request.memdrone().startMemdrone();
            }
        } catch (IOException e) {
            throw new PipelineException("Unable to start Memdrone", e);
        }

    }

    @Override
    public synchronized Set<TaskLogInformation> handleTaskLogInformationRequest(
        WorkerTaskLogInformationRequest request) {
        return TaskLog.searchForLogFiles(request.getInstanceId(), request.getTaskId());
    }

    @Override
    public synchronized String handleSingleTaskLogRequest(WorkerSingleTaskLogRequest request) {

        File taskLogFile = new File(request.getTaskLogInformation().getFullPath());
        String stepLogContents = "<Request timed-out!>";
        log.info("Reading task log: " + taskLogFile);

        Charset defaultCharset = null;
        StringBuilder fileContents = new StringBuilder(1024 * 4);
        try {
            fileContents.append(FileUtils.readFileToString(taskLogFile, defaultCharset));
            log.info("Returning task log (" + fileContents.length() + " chars): " + taskLogFile);
            stepLogContents = fileContents.toString();
        } catch (IOException e) {
            log.warn("Exception waiting for response from: worker ", e);
        }
        return stepLogContents;
    }

    @Override
    public synchronized WorkerResourceRequest.WorkerResources handleResourceRequest() {
        return new WorkerResourceRequest.WorkerResources(WorkerPipelineProcess.workerThreadCount(),
            WorkerPipelineProcess.heapSize());
    }

    @Override
    public synchronized Set<ZiggyEventHandlerInfoForDisplay> handleEventHandlerRequest() {
        return WorkerPipelineProcess.serializableZiggyEventHandlers();
    }

    @Override
    public synchronized void handleEventHandlerToggleRequest(String handlerName) {
        WorkerPipelineProcess.ziggyEventHandlers()
            .stream()
            .filter(s -> s.getName().equals(handlerName))
            .forEach(s -> s.toggleStatus());
    }

    @Override
    public boolean deleteTasks(DeleteTasksRequest request) throws IOException {
        return request.deleteTasks();
    }

}
