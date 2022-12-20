package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceFilter;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class PipelineInstanceCrudProxy extends CrudProxy {

    public PipelineInstanceCrudProxy() {
    }

    public void save(final PipelineInstance instance) {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.create(instance);
            return null;
        });
    }

    /**
     * Update the name of a pipeline instance (normally by the operator in the console) This is done
     * with SQL update rather than via the Hibernate object because we don't want to perturb the
     * other fields which can be set by the worker processes.
     *
     * @param id
     * @param newName
     */
    public void updateName(final long id, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.updateName(id, newName);
            return null;
        });
    }

    public void delete(final PipelineInstance instance) {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.delete(instance);
            return null;
        });
    }

    public PipelineInstance retrieve(final long id) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            PipelineInstance r = crud.retrieve(id);
            return r;
        });
    }

    public List<PipelineInstance> retrieve() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            List<PipelineInstance> r = crud.retrieveAll();
            return r;
        });
    }

    public List<PipelineInstance> retrieve(final PipelineInstanceFilter filter) {
        return retrieve(filter, false);
    }

    public List<PipelineInstance> retrieve(final PipelineInstanceFilter filter, boolean silent) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            List<PipelineInstance> r = crud.retrieve(filter);
            return r;
        }, silent);
    }

    public List<PipelineInstance> retrieveAllActive() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            List<PipelineInstance> r = crud.retrieveAllActive();
            return r;
        });
    }
}
