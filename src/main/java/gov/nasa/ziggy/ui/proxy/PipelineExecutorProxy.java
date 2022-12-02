package gov.nasa.ziggy.ui.proxy;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.models.DatabaseModelRegistry;

/**
 * @author Todd Klaus
 */
public class PipelineExecutorProxy extends CrudProxy {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineExecutorProxy.class);

    public PipelineExecutorProxy() {
    }

    /**
     * Wrapper for the PipelineExecutor.restartTask() method that also handles messaging and
     * database service transactions.
     *
     * @param task
     * @throws Exception
     */
    public void restartTask(final PipelineTask task, RunMode restartMode) throws Exception {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        List<PipelineTask> taskList = new ArrayList<>();
        taskList.add(task);

        restartTasks(taskList, restartMode);
    }

    /**
     * Wrapper for the PipelineExecutor.restartFailedTask() method that also handles messaging and
     * database service transactions. Note that restart task requests are always sent with maximum
     * priority.
     *
     * @param failedTasks
     * @throws Exception
     */
    public void restartTasks(final List<PipelineTask> failedTasks, final RunMode restartMode)
        throws Exception {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {

            PipelineExecutor pipelineExecutor = new PipelineExecutor();
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            for (PipelineTask failedTask1 : failedTasks) {
                PipelineTask dbTask = pipelineTaskCrud.retrieve(failedTask1.getId());
                dbTask.resetAutoResubmitCount();
                pipelineTaskCrud.update(dbTask);
                pipelineExecutor.restartFailedTask(failedTask1, false);
            }
            DatabaseService.getInstance().flush();
            for (PipelineTask failedTask2 : failedTasks) {
                WorkerTaskRequest workerTaskRequest = new WorkerTaskRequest(
                    failedTask2.getPipelineInstance().getId(),
                    failedTask2.getPipelineInstanceNode().getId(), failedTask2.getId(), 0, false,
                    restartMode);
                UiCommunicator.send(workerTaskRequest);
            }
            UiCommunicator.send(new StartMemdroneRequest(failedTasks.get(0).getModuleName(),
                failedTasks.get(0).getPipelineInstance().getId()));
            return null;
        });
        // invalidate the models since restarting a task changes other states.
        DatabaseModelRegistry.invalidateModels();
    }
}
