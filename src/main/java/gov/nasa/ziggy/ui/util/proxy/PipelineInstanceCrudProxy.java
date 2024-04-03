package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceFilter;

/**
 * @author Todd Klaus
 */
public class PipelineInstanceCrudProxy {

    public PipelineInstanceCrudProxy() {
    }

    public void save(final PipelineInstance instance) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.persist(instance);
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
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.updateName(id, newName);
            return null;
        });
    }

    public void delete(final PipelineInstance instance) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            crud.remove(instance);
            return null;
        });
    }

    public PipelineInstance retrieve(final long id) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            return crud.retrieve(id);
        });
    }

    public List<PipelineInstance> retrieve() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            return crud.retrieveAll();
        });
    }

    public List<PipelineInstance> retrieve(final PipelineInstanceFilter filter) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            return crud.retrieve(filter);
        });
    }

    public List<PipelineInstance> retrieveAllActive() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceCrud crud = new PipelineInstanceCrud();
            return crud.retrieveAllActive();
        });
    }
}
