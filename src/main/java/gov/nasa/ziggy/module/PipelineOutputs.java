package gov.nasa.ziggy.module;

import java.nio.file.Path;
import java.util.Set;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Defines the capabilities that any pipeline module needs its outputs to support. The functionality
 * required for an outputs class is as follows:
 * <ol>
 * <li>Copy outputs files from the task directory back to the datastore
 * ({@link #copyTaskFilesToDatastore()}).
 * <li>Determine whether a given subtask produced any outputs {@link #subtaskProducedOutputs()}).
 * <li>Perform any necessary actions after execution of the processing algorithm
 * ({@link #afterAlgorithmExecution()}).
 * </ol>
 * <p>
 * The {@link PipelineOutputs} interface also provides a number of default methods that can be used
 * by implementations in the course of their duties.
 * <p>
 * Users are discouraged from calling constructors directly when desirous of instantiating an object
 * of a {@link PipelineOutputs} implementation. Instead, use
 * {@link PipelineInputsOutputsUtils#newPipelineOutputs(gov.nasa.ziggy.pipeline.definition.ClassWrapper, PipelineTask, Path)}.
 * This will ensure that the {@link PipelineTask} and task directory are correctly populated.
 * <p>
 * The reference implementation of {@link PipelineOutputs} is
 * {@link DatastoreDirectoryPipelineOutputs}.
 *
 * @author PT
 */
public interface PipelineOutputs extends Persistable {

    /**
     * Converts the contents of the outputs file in the subtask directory into one or more files in
     * the task directory, and returns the datastore paths of the resulting file copies.
     */
    Set<Path> copyTaskFilesToDatastore();

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
    boolean subtaskProducedOutputs();

    /**
     * Performs any activities that must be performed in the subtask directory immediately after
     * algorithm execution for the subtask completes.
     */
    void afterAlgorithmExecution();

    void setPipelineTask(PipelineTask pipelineTask);

    PipelineTask getPipelineTask();

    void setTaskDirectory(Path taskDirectory);

    Path getTaskDirectory();
}
