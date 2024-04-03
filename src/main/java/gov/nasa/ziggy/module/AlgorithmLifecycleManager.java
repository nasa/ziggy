package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.supervisor.TaskFileCopy;
import gov.nasa.ziggy.supervisor.TaskFileCopyParameters;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * This class manages the lifecycle of a pipeline algorithm.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmLifecycleManager implements AlgorithmLifecycle {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmLifecycleManager.class);

    private TaskDirectoryManager taskDirManager;
    private Path taskDir;

    private PipelineTask pipelineTask;
    private AlgorithmExecutor executor;

    public AlgorithmLifecycleManager(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
        taskDirManager = new TaskDirectoryManager(pipelineTask);

        // We need an executor at construction time, though it may get replaced later.
        executor = AlgorithmExecutor.newInstance(pipelineTask);
    }

    @Override
    public void executeAlgorithm(TaskConfiguration inputs) {

        // Replace the pipeline task and the executor now, since we have new information
        // about the task's subtask counts.
        pipelineTask = (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {
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
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public File getTaskDir(boolean cleanExisting) {
        File taskDir = allocateTaskDir(cleanExisting);
        if (isRemote()) {
            File stateFileLockFile = new File(taskDir, StateFile.LOCK_FILE_NAME);
            try {
                stateFileLockFile.createNewFile();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to create file " + stateFileLockFile.toString(), e);
            }
        }
        return taskDir;
    }

    private void doTaskFileCopy() {
        if (pipelineTask != null) {
            TaskFileCopyParameters copyParams = pipelineTask
                .getParameters(TaskFileCopyParameters.class, false);
            if (copyParams != null && copyParams.isEnabled()) {
                final TaskFileCopy copier = new TaskFileCopy(pipelineTask, copyParams);

                log.info("Starting copy of task files for pipelineTask : " + pipelineTask.getId());

                IntervalMetric.measure(PipelineMetrics.COPY_TASK_FILES_METRIC, () -> {
                    copier.copyTaskFiles();
                    return null;
                });

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
     * Allocate the working directory using the specified prefix.
     *
     * @param workingDirNamePrefix
     * @param pipelineTask
     * @return
     */
    private File allocateTaskDir(boolean cleanExisting) {

        if (taskDirManager == null) {
            taskDirManager = new TaskDirectoryManager(pipelineTask);
        }

        if (taskDir == null) {
            taskDir = taskDirManager.allocateTaskDir(cleanExisting);
            log.info("defaultWorkingDir = " + taskDir);
        }
        return taskDir.toFile();
    }
}
