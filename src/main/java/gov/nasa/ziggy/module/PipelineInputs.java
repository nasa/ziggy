package gov.nasa.ziggy.module;

import java.nio.file.Path;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Defines the capabilities that any pipeline module needs its inputs class to support. The
 * functionality required for an inputs class is as follows:
 * <ol>
 * <li>Copy or symlink the input files from the datastore to the task directory
 * ({@link #copyDatastoreFilesToTaskDirectory(TaskConfiguration, Path)}).
 * <li>Write the pipeline and module parameters to the task directory
 * ({@link #writeParameterSetsToTaskDirectory()}).
 * <li>Perform a per-subtask initialization prior to starting the algorithm on a given subtask
 * ({@link #beforeAlgorithmExecution()}).
 * <li>Provide information about the task and its subtasks {@link #subtaskInformation()}.
 * </ol>
 * Direct use of constructors for {@link PipelineInputs} implementations is discouraged. Instead,
 * use {@link PipelineInputsOutputsUtils#newPipelineInputs(ClassWrapper, PipelineTask, Path)} to
 * ensure that the {@link PipelineTask} and task directory are correctly populated.
 * <p>
 * The reference implementation is {@link DatastoreDirectoryPipelineInputs}.
 *
 * @author PT
 */
public interface PipelineInputs extends Persistable {

    /**
     * Used by the pipeline module to identify the files in the datastore that are needed in the
     * task directory in order to form the inputs, and copy them to that location.
     */
    void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfiguration, Path taskDirectory);

    /** Provides information about the task and its subtasks. */
    SubtaskInformation subtaskInformation();

    /**
     * Performs any preparation that has to happen after the supervisor hands off the task to a
     * worker, but before a given subtask's algorithm executes.
     */
    void beforeAlgorithmExecution();

    /** Writes the pipeline and module parameters to the task directory. */
    void writeParameterSetsToTaskDirectory();

    void setPipelineTask(PipelineTask pipelineTask);

    PipelineTask getPipelineTask();

    void setTaskDirectory(Path taskDirectory);

    Path getTaskDirectory();
}
