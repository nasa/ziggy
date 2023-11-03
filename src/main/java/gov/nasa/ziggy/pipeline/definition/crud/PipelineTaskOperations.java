package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;
import java.util.Set;

import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.QstatMonitor;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Manages modifications to {@link PipelineTask} instances in the database. Note that some methods
 * require that they be called in the context of a database transaction; those methods can modify
 * the {@link PipelineTask} instance passed as an argument to the method. Other methods only modify
 * the database copy of the instance; these methods can be called at any time and take the ID of the
 * {@link PipelineTask} as argument, rather than the {@link PipelineTask} instance itself.
 *
 * @author PT
 */
public class PipelineTaskOperations {

    private static final Logger log = LoggerFactory.getLogger(PipelineTaskOperations.class);

    /**
     * Creates instances of {@link RemoteJob} in the database using information obtained from the
     * output of the remote cluster's "qstat" command. The instances are initialized to estimated
     * cost of zero and unfinished status.
     */
    public void createRemoteJobsFromQstat(long pipelineTaskId) {

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask databaseTask = pipelineTaskCrud.retrieve(pipelineTaskId);
            QueueCommandManager queueCommandManager = queueCommandManager();
            QstatMonitor qstatMonitor = new QstatMonitor(queueCommandManager);
            qstatMonitor.addToMonitoring(databaseTask);
            qstatMonitor.update();
            Set<Long> allIncompleteJobIds = qstatMonitor.allIncompleteJobIds(databaseTask);
            log.info("Job IDs for task " + databaseTask.getId() + " from qstat : "
                + allIncompleteJobIds.toString());
            Set<RemoteJob> remoteJobs = databaseTask.getRemoteJobs();
            for (long jobId : allIncompleteJobIds) {
                remoteJobs.add(new RemoteJob(jobId));
            }
            return null;
        });
    }

    /**
     * Checks to see whether any {@link RemoteJob}s associated with a {@link PipelineTask} have
     * completed since the last such check. For any newly-completed jobs, the status will be changed
     * to finished and the cost estimate will be updated.
     * <p>
     * Note that {@link #checkForFinishedJobs(PipelineTask)} can only be called in the context of a
     * running database transaction, otherwise the updates to the state will not be performed.
     */
    private void checkForFinishedJobs(PipelineTask pipelineTask) {

        QueueCommandManager queueCommandManager = queueCommandManager();
        Set<RemoteJob> remoteJobs = pipelineTask.getRemoteJobs();
        for (RemoteJob job : remoteJobs) {
            if (!job.isFinished()) {
                boolean finished = queueCommandManager.exitStatus(job.getJobId()) != null;
                if (finished) {
                    Log.info("Job " + job.getJobId() + " marked as finished");
                    RemoteJob.RemoteJobQstatInfo jobInfo = queueCommandManager
                        .remoteJobQstatInfo(job.getJobId());
                    job.setFinished(true);
                    job.setCostEstimate(jobInfo.costEstimate());
                    log.info("Job " + job.getJobId() + " cost estimate: " + job.getCostEstimate());
                }
            }
        }
    }

    /**
     * Updates the state of all {@link RemoteJob}s associated with a {@link PipelineTask}. Any jobs
     * that have completed since the last update will be marked as finished and get their final cost
     * estimates calculated; any that are still running will get an up-to-the-minute cost estimate
     * calculated.
     * <p>
     * Note that {@link #updateJobs(PipelineTask)} can only be called in the context of a running
     * database transaction, otherwise the updates to the state will not be performed.
     */
    public void updateJobs(PipelineTask pipelineTask) {

        checkForFinishedJobs(pipelineTask);
        QueueCommandManager queueCommandManager = queueCommandManager();
        Set<RemoteJob> remoteJobs = pipelineTask.getRemoteJobs();
        for (RemoteJob job : remoteJobs) {
            if (!job.isFinished()) {
                RemoteJob.RemoteJobQstatInfo jobInfo = queueCommandManager
                    .remoteJobQstatInfo(job.getJobId());
                log.debug("job " + job.getJobId() + " nodes " + jobInfo.getNodes() + " model "
                    + jobInfo.getModel() + " cost estimate " + jobInfo.costEstimate());
                job.setCostEstimate(jobInfo.costEstimate());
                log.info("Incomplete job " + job.getJobId() + " running cost estimate: "
                    + job.getCostEstimate());
            }
        }
    }

    /**
     * Updates all of the {@link PipelineTask} instances associated with a particular
     * {@link PipelineInstance} and returns them as a List.
     * <p>
     * Note that {@link #updateJobs(PipelineInstance)} can only be called in the context of a
     * running database transaction, otherwise the updates to the state will not be performed.
     */
    public List<PipelineTask> updateJobs(PipelineInstance pipelineInstance) {

        List<PipelineTask> tasks = new PipelineTaskCrud()
            .retrieveTasksForInstance(pipelineInstance);
        for (PipelineTask task : tasks) {
            updateJobs(task);
        }
        return tasks;
    }

    QueueCommandManager queueCommandManager() {
        return QueueCommandManager.newInstance();
    }
}
