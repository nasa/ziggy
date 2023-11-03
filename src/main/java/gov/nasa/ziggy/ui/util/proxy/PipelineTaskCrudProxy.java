package gov.nasa.ziggy.ui.util.proxy;

import java.util.Collection;
import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * @author Todd Klaus
 */
public class PipelineTaskCrudProxy {

    public PipelineTaskCrudProxy() {
    }

    public void save(final PipelineTask task) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            crud.persist(task);
            return null;
        });
    }

    public PipelineTask retrieve(final long id) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieve(id);
        });
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            List<PipelineTask> r = crud.retrieveTasksForInstance(instance);
            for (PipelineTask task : r) {
                Hibernate.initialize(task.getSummaryMetrics());
                Hibernate.initialize(task.getExecLog());
                Hibernate.initialize(task.getProducerTaskIds());
            }
            return r;
        });
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance,
        final PipelineTask.State state) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieveAll(instance, state);
        });
    }

    public List<PipelineTask> retrieveAll(final Collection<Long> taskIds) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieveAll(taskIds);
        });
    }
}
