package gov.nasa.ziggy.pipeline.definition.crud;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources_;

/**
 * CRUD class for {@link PipelineDefinitionNode}.
 *
 * @author PT
 */
public class PipelineDefinitionNodeCrud extends AbstractCrud<PipelineDefinitionNode> {

    @Override
    public Class<PipelineDefinitionNode> componentClass() {
        return PipelineDefinitionNode.class;
    }

    /**
     * Retrieves the {@link PipelineDefinitionNodeExecutionResources} for a given
     * {@link PipelineDefinitionNode}. If none exists, one is created and persisted (and then
     * returned, of course).
     */
    public PipelineDefinitionNodeExecutionResources retrieveExecutionResources(
        PipelineDefinitionNode node) {

        ZiggyQuery<PipelineDefinitionNodeExecutionResources, PipelineDefinitionNodeExecutionResources> query = createZiggyQuery(
            PipelineDefinitionNodeExecutionResources.class);
        query.column(PipelineDefinitionNodeExecutionResources_.pipelineName)
            .in(node.getPipelineName());
        query.column(PipelineDefinitionNodeExecutionResources_.pipelineModuleName)
            .in(node.getModuleName());
        PipelineDefinitionNodeExecutionResources executionResources = uniqueResult(query);
        if (executionResources == null) {
            executionResources = new PipelineDefinitionNodeExecutionResources(
                node.getPipelineName(), node.getModuleName());
            persist(executionResources);
        }
        return executionResources;
    }
}
