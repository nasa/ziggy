package gov.nasa.ziggy.pipeline.step.io;

import java.nio.file.Path;

import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskInformation;
import gov.nasa.ziggy.util.io.Persistable;
import gov.nasa.ziggy.util.io.ProxyIgnore;

/**
 * Defines the capabilities that any pipeline step needs its inputs class to support. The
 * functionality required for an inputs class is as follows:
 * <ol>
 * <li>Copy or symlink the input files from the datastore to the task directory
 * ({@link #copyDatastoreFilesToTaskDirectory(TaskConfiguration, Path)}).
 * <li>Write the pipeline and node parameters to the task directory
 * ({@link #writeParameterSetsToTaskDirectory()}).
 * <li>Perform a per-subtask initialization prior to starting the algorithm on a given subtask
 * ({@link #beforeAlgorithmExecution()}).
 * <li>Provide information about the task and its subtasks
 * {@link #subtaskInformation(PipelineNode)}.
 * </ol>
 * <p>
 * The {@code pipelineTask} and {@code taskDirectory} fields are not guaranteed to be non-null,
 * since this class can be used in contexts where they are not available. Therefore, implementations
 * must provide suitable alternatives if these fields are null.
 * <p>
 * The reference implementation is {@link DatastoreDirectoryPipelineInputs}.
 *
 * @author PT
 */
public abstract class PipelineInputs implements Persistable {

    @ProxyIgnore
    private PipelineTask pipelineTask;

    @ProxyIgnore
    private Path taskDirectory;

    /**
     * Creates a {@code PipelineInputs} object with null {@code pipelineTask} and
     * {@code taskDirectory} fields.
     */
    public PipelineInputs() {
        pipelineTask = null;
        taskDirectory = null;
    }

    /**
     * Creates a {@code PipelineInputs} object with the given {@code pipelineTask} and
     * {@code taskDirectory} fields. These may be null.
     */
    public PipelineInputs(PipelineTask pipelineTask, Path taskDirectory) {
        this.pipelineTask = pipelineTask;
        this.taskDirectory = taskDirectory;
    }

    /**
     * Used by the pipeline step to identify the files in the datastore that are needed in the task
     * directory in order to form the inputs, and copy them to that location.
     */
    public abstract void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfiguration,
        Path taskDirectory);

    /** Provides information about the task and its subtasks. */
    public abstract SubtaskInformation subtaskInformation(PipelineNode pipelineNode);

    /**
     * Performs any preparation that has to happen after the supervisor hands off the task to a
     * worker, but before a given subtask's algorithm executes.
     */
    public abstract void beforeAlgorithmExecution();

    /** Writes the pipeline and node parameters to the task directory. */
    public abstract void writeParameterSetsToTaskDirectory();

    /**
     * Returns the pipeline task associated with this object. May be null.
     */
    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public void setPipelineTask(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    /**
     * Returns the task directory associated with this object. May be null.
     */
    public Path getTaskDirectory() {
        return taskDirectory;
    }

    public void setTaskDirectory(Path taskDirectory) {
        this.taskDirectory = taskDirectory;
    }
}
