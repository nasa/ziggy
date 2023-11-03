package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition_;

/**
 * Provides CRUD methods for {@link PipelineModuleDefinition}
 *
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrud
    extends UniqueNameVersionPipelineComponentCrud<PipelineModuleDefinition> {

    public PipelineModuleDefinitionCrud() {
    }

    public List<PipelineModuleDefinition> retrieveAll() {
        return list(
            createZiggyQuery(PipelineModuleDefinition.class).column(PipelineModuleDefinition_.NAME)
                .ascendingOrder());
    }

    @Override
    protected void populateXmlFields(Collection<PipelineModuleDefinition> objects) {
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "pipeline module definition";
    }

    @Override
    public Class<PipelineModuleDefinition> componentClass() {
        return PipelineModuleDefinition.class;
    }
}
