package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;
import gov.nasa.ziggy.services.security.Privilege;

public class PipelineTaskOperationsProxy {

    public List<PipelineTask> updateAndRetrieveTasks(PipelineInstance pipelineInstance) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new PipelineTaskOperations().updateJobs(pipelineInstance));
    }
}
