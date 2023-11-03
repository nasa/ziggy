package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrudProxy {
    public PipelineModuleDefinitionCrudProxy() {
    }

    public void delete(final PipelineModuleDefinition module) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.remove(module);
            return null;
        });
    }

    public PipelineModuleDefinition rename(final PipelineModuleDefinition moduleDef,
        final String newName) {

        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.rename(moduleDef, newName);
        });
    }

    public List<PipelineModuleDefinition> retrieveAll() {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveAll();
        });
    }

    public PipelineModuleDefinition retrieveLatestVersionForName(final String name) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveLatestVersionForName(name);
        });
    }

    public List<PipelineModuleDefinition> retrieveLatestVersions() {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            return crud.retrieveLatestVersions();
        });
    }

    public void createOrUpdate(PipelineModuleDefinition module) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.merge(module);
            return null;
        });
    }
}
