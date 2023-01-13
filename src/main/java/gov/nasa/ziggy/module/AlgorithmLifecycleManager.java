package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.worker.TaskFileCopy;
import gov.nasa.ziggy.worker.TaskFileCopyParameters;

/**
 * This class manages the lifecycle of a pipeline algorithm.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmLifecycleManager implements AlgorithmLifecycle {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmLifecycleManager.class);

    private static WorkingDirManager workingDirManager = null;
    private File defaultWorkingDir = null;

    private PipelineTask pipelineTask;
    private AlgorithmExecutor executor;

    public AlgorithmLifecycleManager(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;

        // We need an executor at construction time, though it may get replaced later.
        executor = AlgorithmExecutor.newInstance(pipelineTask);
    }

    @Override
    public void executeAlgorithm(TaskConfigurationManager inputs) {

        // Replace the pipeline task and the executor now, since we have new information
        // about the task's subtask counts.
        pipelineTask = (PipelineTask) DatabaseTransactionFactory.performTransactionInThread(() -> {
            PipelineTask p = new PipelineTaskCrud().retrieve(pipelineTask.getId());
            Hibernate.initialize(p.getModuleParameterSets());
            Hibernate.initialize(p.getPipelineInstance());
            Hibernate.initialize(p.getPipelineInstance().getPipelineParameterSets());
            return p;
        });
        executor = AlgorithmExecutor.newInstance(pipelineTask);
        executor.submitAlgorithm(inputs);
    }

    @Override
    public void doPostProcessing() {
        doTaskFileCopy();
    }

    @Override
    public File getTaskDir(boolean cleanExisting) {
        File taskDir = allocateWorkingDir(cleanExisting);
        if (isRemote()) {
            File stateFileLockFile = new File(taskDir, StateFile.LOCK_FILE_NAME);
            try {
                stateFileLockFile.createNewFile();
            } catch (IOException e) {
                throw new PipelineException("Unable to create lock file for state file", e);
            }
        }
        return taskDir;
    }

    private void doTaskFileCopy() throws PipelineException {
        if (pipelineTask != null) {
            TaskFileCopyParameters copyParams = pipelineTask
                .getParameters(TaskFileCopyParameters.class, false);
            if (copyParams != null && copyParams.isEnabled()) {
                final TaskFileCopy copier = new TaskFileCopy(pipelineTask, copyParams);

                log.info("Starting copy of task files for pipelineTask : " + pipelineTask.getId());

                try {
                    IntervalMetric.measure(PipelineMetrics.COPY_TASK_FILES_METRIC, () -> {
                        copier.copyTaskFiles();
                        return null;
                    });
                } catch (Exception e) {
                    throw new PipelineException(e);
                }

                log.info("Finished copy of task files for pipelineTask : " + pipelineTask.getId());
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see gov.nasa.ziggy.module.AlgorithmLifecycle#isRemote()
     */
    @Override
    public boolean isRemote() {
        return executor.algorithmType() == AlgorithmExecutor.AlgorithmType.REMOTE;
    }

    @Override
    public AlgorithmExecutor getExecutor() {
        return executor;
    }

    /**
     * Allocate the working directory using the default naming convention:
     * INSTANCEID-TASKID-MODULENAME
     *
     * @return
     */
    private File allocateWorkingDir(boolean cleanExisting) {
        return allocateWorkingDir(pipelineTask, cleanExisting);
    }

    /**
     * Allocate the working directory using the specified prefix.
     *
     * @param workingDirNamePrefix
     * @param pipelineTask
     * @return
     */
    private File allocateWorkingDir(PipelineTask pipelineTask, boolean cleanExisting) {
        synchronized (ExternalProcessPipelineModule.class) {
            if (workingDirManager == null) {
                workingDirManager = new WorkingDirManager();
            }
        }

        if (defaultWorkingDir == null) {
            try {
                defaultWorkingDir = workingDirManager.allocateWorkingDir(pipelineTask,
                    cleanExisting);
                log.info("defaultWorkingDir = " + defaultWorkingDir);

            } catch (IOException e) {
                throw new ModuleFatalProcessingException(
                    "failed to execute external program, e = " + e);
            }
        }
        return defaultWorkingDir;
    }

}
