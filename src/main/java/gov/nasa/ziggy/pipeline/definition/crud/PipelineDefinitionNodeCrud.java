package gov.nasa.ziggy.pipeline.definition.crud;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

/**
 * CRUD class for {@link PipelineDefinitionNode}.
 * <p>
 * The PipelineDefinitionNode requires only the generic CRUD features like
 * {@link AbstractCrud#persist(Object)} and {@link AbstractCrud#merge(Object)}. Thus no other
 * class-specific methods are defined here.
 *
 * @author PT
 */
public class PipelineDefinitionNodeCrud extends AbstractCrud<PipelineDefinitionNode> {

    @Override
    public Class<PipelineDefinitionNode> componentClass() {
        return PipelineDefinitionNode.class;
    }
}
