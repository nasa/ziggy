package gov.nasa.ziggy.pipeline.step.io;

import java.nio.file.Path;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.io.Persistable;
import gov.nasa.ziggy.util.io.ProxyIgnore;

/**
 * Defines the capabilities that any pipeline step needs its outputs to support. The functionality
 * required for an outputs class is as follows:
 * <ol>
 * <li>Copy outputs files from the task directory back to the datastore
 * ({@link #copyTaskFilesToDatastore()}).
 * <li>Determine whether a given subtask produced any outputs {@link #subtaskProducedOutputs()}).
 * <li>Perform any necessary actions after execution of the processing algorithm
 * ({@link #afterAlgorithmExecution()}).
 * </ol>
 * <p>
 * The {@code pipelineTask} and {@code taskDirectory} fields are not guaranteed to be non-null,
 * since this class can be used in contexts where they are not available. Therefore, implementations
 * must provide suitable alternatives if these fields are null.
 * <p>
 * The reference implementation is {@link DatastoreDirectoryPipelineOutputs}.
 *
 * @author PT
 */
public abstract class PipelineOutputs implements Persistable {

    @ProxyIgnore
    private PipelineTask pipelineTask;

    @ProxyIgnore
    private Path taskDirectory;

    /**
     * Creates a {@code PipelineInputs} object with null {@code pipelineTask} and
     * {@code taskDirectory} fields.
     */
    public PipelineOutputs() {
        pipelineTask = null;
        taskDirectory = null;
    }

    /**
     * Creates a {@code PipelineInputs} object with the given {@code pipelineTask} and
     * {@code taskDirectory} fields. These may be null.
     */

    public PipelineOutputs(PipelineTask pipelineTask, Path taskDirectory) {
        this.pipelineTask = pipelineTask;
        this.taskDirectory = taskDirectory;
    }

    /**
     * Converts the contents of the outputs file in the subtask directory into one or more files in
     * the task directory, and returns the datastore paths of the resulting file copies.
     */
    public abstract Set<Path> copyTaskFilesToDatastore();

    /**
     * Determines whether the subtask that ran in the current working directory produced results. A
     * subtask that runs to completion may nonetheless produce no results, in which case the subtask
     * will not be counted as failed and will not be re-run in the event that the task is
     * resubmitted. The determination as to whether a subtask produced results is potentially
     * implementation-specific: for example, an algorithm that produces multiple different results
     * files for each subtask may want to identify a subset of those results files to use in the
     * determination of whether results were produced, which would allow some results files to be
     * necessary to the determination but others optional.
     *
     * @return true if required results files were produced for a given subtask, false otherwise.
     */
    public abstract boolean subtaskProducedOutputs();

    /**
     * Performs any activities that must be performed in the subtask directory immediately after
     * algorithm execution for the subtask completes.
     */
    public abstract void afterAlgorithmExecution();

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
