package gov.nasa.ziggy.ui.util.proxy;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionNodeCrud;

public class PipelineDefinitionNodeCrudProxy {

    public PipelineDefinitionNodeExecutionResources merge(
        PipelineDefinitionNodeExecutionResources executionResources) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new PipelineDefinitionNodeCrud().merge(executionResources));
    }

    public PipelineDefinitionNodeExecutionResources retrieveRemoteExecutionConfiguration(
        PipelineDefinitionNode node) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new PipelineDefinitionNodeCrud().retrieveExecutionResources(node));
    }
}
