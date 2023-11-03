package gov.nasa.ziggy.ui.util.proxy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * @author Todd Klaus
 */
public class ParameterSetCrudProxy extends RetrieveLatestVersionsCrudProxy<ParameterSet> {

    public ParameterSetCrudProxy() {
    }

    public void save(final ParameterSet moduleParameterSet) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            updateAuditInfo(moduleParameterSet.getAuditInfo());
            crud.persist(moduleParameterSet);
            return null;
        });
    }

    public ParameterSet rename(final ParameterSet parameterSet, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            updateAuditInfo(parameterSet.getAuditInfo());
            return crud.rename(parameterSet, newName);
        });
    }

    public List<ParameterSet> retrieveAll() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveAll();
        });
    }

    public List<ParameterSet> retrieveAllVersionsForName(final String name) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveAllVersionsForName(name);
        });
    }

    public ParameterSet retrieveLatestVersionForName(final String name) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveLatestVersionForName(name);
        });
    }

    @Override
    public List<ParameterSet> retrieveLatestVersions() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveLatestVersions();
        });
    }

    public void delete(final ParameterSet moduleParameterSet) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            crud.remove(moduleParameterSet);
            return null;
        });
    }

    @Override
    public ParameterSet update(ParameterSet entity) {
        checkArgument(entity instanceof ParameterSet, "entity must be ParameterSet");
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.merge(entity);
        });
    }
}
