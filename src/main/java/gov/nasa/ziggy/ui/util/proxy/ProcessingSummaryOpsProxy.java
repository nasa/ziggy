package gov.nasa.ziggy.ui.util.proxy;

import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * @author Todd Klaus
 */
public class ProcessingSummaryOpsProxy {
    public ProcessingSummaryOpsProxy() {
    }

    public ProcessingSummary retrieveByTaskId(final long taskId) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new ProcessingSummaryOperations().processingSummaryInternal(taskId));
    }

    public Map<Long, ProcessingSummary> retrieveByInstanceId(final long instanceId) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new ProcessingSummaryOperations()
                .processingSummariesForInstanceInternal(instanceId));
    }
}
