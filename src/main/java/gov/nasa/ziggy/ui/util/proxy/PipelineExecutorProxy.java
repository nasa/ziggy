package gov.nasa.ziggy.ui.util.proxy;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.models.DatabaseModelRegistry;

/**
 * @author Todd Klaus
 */
public class PipelineExecutorProxy {

    public PipelineExecutorProxy() {
    }

    /**
     * Wrapper for the PipelineExecutor.restartTask() method that also handles messaging and
     * database service transactions. Note that restart task requests are always sent with maximum
     * priority.
     */
    public void restartTask(final PipelineTask task, RunMode restartMode) throws Exception {
        restartTasks(List.of(task), restartMode, null);
    }

    /**
     * Wrapper for the PipelineExecutor.restartFailedTask() method that also handles messaging and
     * database service transactions. Note that restart task requests are always sent with maximum
     * priority.
     */
    public void restartTasks(final List<PipelineTask> failedTasks, final RunMode restartMode)
        throws Exception {
        restartTasks(failedTasks, restartMode, null);
    }

    /**
     * Wrapper for the PipelineExecutor.restartFailedTask() method that also handles messaging and
     * database service transactions. Note that restart task requests are always sent with maximum
     * priority. If a latch is provided, it is decremented once after a related message is
     * published.
     */
    public void restartTasks(final List<PipelineTask> failedTasks, final RunMode restartMode,
        CountDownLatch messageSentLatch) throws Exception {

        checkNotNull(failedTasks, "failedTasks");
        checkArgument(failedTasks.size() > 0, "failedTasks must not be empty");

        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            List<PipelineTask> databaseTasks = new ArrayList<>();
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            for (PipelineTask failedTask : failedTasks) {
                PipelineTask dbTask = pipelineTaskCrud.retrieve(failedTask.getId());
                dbTask.resetAutoResubmitCount();
                databaseTasks.add(dbTask);
            }
            new PipelineExecutor().restartFailedTasks(databaseTasks, false, restartMode);
            DatabaseService.getInstance().flush();
            ZiggyMessenger.publish(new StartMemdroneRequest(failedTasks.get(0).getModuleName(),
                failedTasks.get(0).getPipelineInstance().getId()), messageSentLatch);
            return null;
        });

        // invalidate the models since restarting a task changes other states.
        DatabaseModelRegistry.invalidateModels();
    }
}
