package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition_;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources_;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

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

    public PipelineModuleExecutionResources retrieveExecutionResources(
        PipelineModuleDefinition module) {
        ZiggyQuery<PipelineModuleExecutionResources, PipelineModuleExecutionResources> query = createZiggyQuery(
            PipelineModuleExecutionResources.class);
        query.column(PipelineModuleExecutionResources_.pipelineModuleName).in(module.getName());
        PipelineModuleExecutionResources resources = uniqueResult(query);
        if (resources == null) {
            resources = new PipelineModuleExecutionResources();
            resources.setPipelineModuleName(module.getName());
            resources = merge(resources);
        }
        return resources;
    }

    public ClassWrapper<UnitOfWorkGenerator> retrieveUnitOfWorkGenerator(String moduleName) {
        return retrieveLatestVersionForName(moduleName).getUnitOfWorkGenerator();
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
