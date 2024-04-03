package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrudProxy {
    public PipelineModuleDefinitionCrudProxy() {
    }

    public void delete(final PipelineModuleDefinition module) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.remove(module);
            return null;
        });
    }

    public PipelineModuleDefinition rename(final PipelineModuleDefinition moduleDef,
        final String newName) {

        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.rename(moduleDef, newName);
        });
    }

    public List<PipelineModuleDefinition> retrieveAll() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveAll();
        });
    }

    public PipelineModuleDefinition retrieveLatestVersionForName(final String name) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveLatestVersionForName(name);
        });
    }

    public List<PipelineModuleDefinition> retrieveLatestVersions() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveLatestVersions();
        });
    }

    public PipelineModuleDefinition merge(PipelineModuleDefinition module) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.merge(module);
        });
    }

    public PipelineModuleExecutionResources retrievePipelineModuleExecutionResources(
        PipelineModuleDefinition module) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveExecutionResources(module);
        });
    }

    public PipelineModuleExecutionResources mergeExecutionResources(
        PipelineModuleExecutionResources executionResources) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.merge(executionResources);
        });
    }

    public ClassWrapper<UnitOfWorkGenerator> retrieveUnitOfWorkGenerator(String moduleName) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveUnitOfWorkGenerator(moduleName);
        });
    }
}
