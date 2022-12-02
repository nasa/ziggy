package gov.nasa.ziggy.ui.proxy;

import java.util.Collection;
import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class PipelineTaskCrudProxy extends CrudProxy {

    public PipelineTaskCrudProxy() {
    }

    public void save(final PipelineTask task) {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            crud.create(task);
            return null;
        });
    }

    public PipelineTask retrieve(final long id) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        PipelineTask result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineTaskCrud crud = new PipelineTaskCrud();
                PipelineTask r = crud.retrieve(id);
                return r;
            });
        return result;
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance) {
        return retrieveAll(instance, false);
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance, boolean silent) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<PipelineTask> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineTaskCrud crud = new PipelineTaskCrud();
                List<PipelineTask> r = crud.retrieveTasksForInstance(instance);
                for (PipelineTask task : r) {
                    Hibernate.initialize(task.getSummaryMetrics());
                    Hibernate.initialize(task.getExecLog());
                    Hibernate.initialize(task.getProducerTaskIds());
                }
                return r;
            }, silent);
        return result;
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance,
        final PipelineTask.State state) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<PipelineTask> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineTaskCrud crud = new PipelineTaskCrud();
                List<PipelineTask> r = crud.retrieveAll(instance, state);
                return r;
            });
        return result;
    }

    public List<PipelineTask> retrieveAll(final Collection<Long> taskIds) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<PipelineTask> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineTaskCrud crud = new PipelineTaskCrud();
                List<PipelineTask> r = crud.retrieveAll(taskIds);
                return r;
            });
        return result;
    }

}
