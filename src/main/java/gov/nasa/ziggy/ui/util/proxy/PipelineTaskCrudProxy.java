package gov.nasa.ziggy.ui.util.proxy;

import java.util.Collection;
import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;

/**
 * @author Todd Klaus
 */
public class PipelineTaskCrudProxy {

    public PipelineTaskCrudProxy() {
    }

    public void save(final PipelineTask task) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            crud.persist(task);
            return null;
        });
    }

    public PipelineTask retrieve(final long id) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieve(id);
        });
    }

    public List<PipelineTask> retrieveAll(final PipelineInstance instance) {
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
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieveAll(instance, state);
        });
    }

    public List<PipelineTask> retrieveAll(final Collection<Long> taskIds) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            return crud.retrieveAll(taskIds);
        });
    }
}
