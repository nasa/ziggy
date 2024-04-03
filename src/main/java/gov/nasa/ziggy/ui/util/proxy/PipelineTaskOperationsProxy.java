package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;

public class PipelineTaskOperationsProxy {

    public List<PipelineTask> updateAndRetrieveTasks(PipelineInstance pipelineInstance) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new PipelineTaskOperations().updateJobs(pipelineInstance));
    }
}
