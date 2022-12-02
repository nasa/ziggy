package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class ParameterSetCrudProxy extends CrudProxy {

    public ParameterSetCrudProxy() {
    }

    public void save(final ParameterSet moduleParameterSet) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            updateAuditInfo(moduleParameterSet.getAuditInfo());
            crud.create(moduleParameterSet);
            return null;
        });
    }

    public void rename(final ParameterSet parameterSet, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            updateAuditInfo(parameterSet.getAuditInfo());
            crud.rename(parameterSet, newName);
            return null;
        });
    }

    public List<ParameterSet> retrieveAll() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<ParameterSet> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                ParameterSetCrud crud = new ParameterSetCrud();
                List<ParameterSet> r = crud.retrieveAll();
                return r;
            });
        return result;
    }

    public List<ParameterSet> retrieveAllVersionsForName(final String name) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<ParameterSet> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                ParameterSetCrud crud = new ParameterSetCrud();
                List<ParameterSet> r = crud.retrieveAllVersionsForName(name);
                return r;
            });
        return result;
    }

    public ParameterSet retrieveLatestVersionForName(final ParameterSetName name) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        ParameterSet result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                ParameterSetCrud crud = new ParameterSetCrud();
                ParameterSet r = crud.retrieveLatestVersionForName(name);
                return r;
            });
        return result;
    }

    public List<ParameterSet> retrieveLatestVersions() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<ParameterSet> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                ParameterSetCrud crud = new ParameterSetCrud();
                List<ParameterSet> r = crud.retrieveLatestVersions();
                return r;
            });
        return result;
    }

    public void delete(final ParameterSet moduleParameterSet) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            crud.delete(moduleParameterSet);
            return null;
        });
    }
}
