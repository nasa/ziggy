package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ModuleName;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrudProxy extends CrudProxy {
    public PipelineModuleDefinitionCrudProxy() {
    }

    public void save(final PipelineModuleDefinition module) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            updateAuditInfo(module.getAuditInfo());
            crud.create(module);
            return null;
        });
    }

    public void delete(final PipelineModuleDefinition module) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.delete(module);
            return null;
        });
    }

    public void rename(final PipelineModuleDefinition moduleDef, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.rename(moduleDef, newName);
            return null;
        });
    }

    public List<PipelineModuleDefinition> retrieveAll() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            List<PipelineModuleDefinition> r = crud.retrieveAll();
            return r;
        });
    }

    public PipelineModuleDefinition retrieveLatestVersionForName(final ModuleName name) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            PipelineModuleDefinition r = crud.retrieveLatestVersionForName(name);
            return r;
        });
    }

    public List<PipelineModuleDefinition> retrieveLatestVersions() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            List<PipelineModuleDefinition> r = crud.retrieveLatestVersions();
            return r;
        });
    }

}
