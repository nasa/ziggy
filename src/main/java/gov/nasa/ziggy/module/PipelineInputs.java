package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.module.PipelineInputsOutputsUtils.moduleName;
import static gov.nasa.ziggy.module.PipelineInputsOutputsUtils.taskDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DataFileInfo;
import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DatastorePathLocator;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Superclass for all pipeline inputs classes. The pipeline inputs class for a given pipeline module
 * contains all the information required for that module: data, models, and parameters. This
 * information must be assembled from the contents of the pipeline's datastore and relational
 * database. The class provides functionality in support of that assembly:
 * <ol>
 * <li>Identifies parameter classes that are needed by the pipeline (see
 * {@link requiredParameters()}).
 * <li>Identifies datastore files needed to be used as inputs and copies them to the task directory
 * (see {@link copyDatastoreFilesToTaskDirectory(TaskConfigurationManager, PipelineTask, Path)}).
 * <li>Supplies an instance of a DatastorePathLocator subclass for use in this task (see
 * {@link datastorePathLocator(PipelineTask)}).
 * <li>Serializes parameters to an HDF5 file in the task directory (see
 * {@link writeToTaskDir(PipelineTask, File)}).
 * <li>Identifies files in the task directory that contain data and models from the datastore
 * required for processing (see {@link resultsFiles()}). Those files were copied to the task
 * directory by the pipeline module prior to execution of {@link resultsFiles()}.
 * <li>Reads the parameter file from the task directory (see {@link readFromTaskDir()}).
 * <li>Reads the contents of datastore files that contain data or models required for processing
 * (see {@link #readResultsFile(DataFileInfo, PipelineResults)}).
 * <li>Serializes an HDF5 file containing all of the inputs for processing into the sub-task
 * directory (see {@link writeSubTaskInputs(int)}).
 * </ol>
 * <p>
 * The abstract method {@link populateSubTaskInputs()} performs the steps that read from the task
 * directory, populate the members of the inputs class instance, and serialize that instance to the
 * sub-task directory using the other methods of the class provided here as tools.
 * <p>
 * The method {@link #deleteTempInputsFromTaskDirectory(PipelineTask, Path)} deletes the files in
 * the task directory that were copied to that location by the
 * {@link copyDatastoreFilesToTaskDirectory(TaskConfigurationManager, PipelineTask, Path)} method.
 * This is executed after the pipeline algorithm has completed, at which time the datastore files
 * are superfluous.
 *
 * @author PT
 */
public abstract class PipelineInputs implements Persistable {

    private static final Logger log = LoggerFactory.getLogger(PipelineInputs.class);

    @ProxyIgnore
    private Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();

    @ProxyIgnore
    private Integer subTaskIndex = null;

    /**
     * Used to identify the parameter classes that a pipeline requires in order to execute.
     * PipelineInputs subclasses should override this method to provide required parameters in cases
     * where there are such.
     *
     * @return List of parameter classes.
     */
    public List<Class<? extends ParametersInterface>> requiredParameters() {
        return new ArrayList<>();
    }

    /**
     * Returns an instance of a DatastorePathLocator subclass for use in the pipeline.
     *
     * @param pipelineTask PipelineTask for the current task.
     * @return DatastorePathLocator for this task.
     */
    public abstract DatastorePathLocator datastorePathLocator(PipelineTask pipelineTask);

    /**
     * Used by the ExternalProcessPipelineModule, or its subclasses, to identify the files in the
     * datastore that are needed in the task directory in order to form the inputs, and copy them to
     * that location.
     *
     * @param taskConfigurationManager TaskConfigurationManager for this task.
     * @param pipelineTask PipelineTask for this task.
     * @param taskDirectory task directory for this task.
     */
    public abstract void copyDatastoreFilesToTaskDirectory(
        TaskConfigurationManager taskConfigurationManager, PipelineTask pipelineTask,
        Path taskDirectory);

    /**
     * Used by {@link ExternalProcessPipelineModule}, or its subclasses, to identify the files in
     * the datastore that are provided to the current task as inputs.
     *
     * @param pipelineTask PipelineTask for this task.
     * @return {@link Set} of {@link Path} instances for task inputs. Must never be null.
     */
    public abstract Set<Path> findDatastoreFilesForInputs(PipelineTask pipelineTask);

    /**
     * Generates the inputs for a specific sub-task from the contents of the datastore files that
     * have been copied to the task directory.
     */
    public abstract void populateSubTaskInputs();

    /**
     * Provides an instance of {@link SubtaskInformation} for a given {@link PipelineTask}. This is
     * the default implementation, in which there is 1 subtask per task. For inputs classes that
     * potentially generate multiple subtasks, this method must be overridden with one that provides
     * the correct information.
     */
    public SubtaskInformation subtaskInformation(PipelineTask pipelineTask) {

        return new SubtaskInformation(pipelineTask.getModuleName(),
            pipelineTask.uowTaskInstance().briefState(), 1, 1);
    }

    /**
     * Provides information on whether a given module sets limits on the number of subtasks that can
     * be processed in parallel. The default behavior is that modules do not set such limits (i.e.,
     * all subtasks can potentially be processed in parallel), ergo the default method returns
     * false. For pipeline modules that do have such limits, override the default method with one
     * that returns true.
     */
    public boolean parallelLimits() {
        return false;
    }

    /**
     * Writes a partially-populated input to an HDF5 file in the task directory. This allows the
     * pipeline to provide a set of inputs that are common to all sub-tasks in the task directory.
     * This is intended to be used by the worker, which has access to the pipeline task instance.
     *
     * @param taskDir
     * @param pipelineTask
     */
    public void writeToTaskDir(PipelineTask pipelineTask, File taskDir) {
        String filename = ModuleInterfaceUtils.inputsFileName(pipelineTask.getModuleName());
        log.info("Writing partial inputs to file " + filename + " in task dir");
        File inputInTaskDir = new File(taskDir, filename);
        hdf5ModuleInterface.writeFile(inputInTaskDir, this, true);
    }

    /**
     * Reads a partially-populated input from an HDF5 file in the task directory. This allows the
     * pipeline to provide a set of inputs that are common to all sub-tasks in the task directory,
     * and this can be used as a starting point for populating the sub-task inputs.
     */
    public void readFromTaskDir() {
        String filename = ModuleInterfaceUtils.inputsFileName(moduleName());
        log.info("Populating inputs object from file " + filename + " in task dir");
        File inputInTaskDir = taskDir().resolve(filename).toFile();
        hdf5ModuleInterface.readFile(inputInTaskDir, this, true);
    }

    /**
     * Returns a non-{@code null} set of DataFileInfo subclasses that are needed to populate the
     * initial pipeline task inputs. Concrete subclasses of this class should override this with a
     * method that returns the needed DatastoreId classes.
     */
    public Set<Class<? extends DataFileInfo>> requiredDataFileInfoClasses() {
        return Collections.emptySet();
    }

    /**
     * Returns the sub-task index. Assumes that the working directory is the sub-task directory.
     *
     * @return
     */
    public int subtaskIndex() {
        if (subTaskIndex == null) {
            String regex = "st-(\\d+)";
            Pattern pattern = Pattern.compile(regex);
            File userDir = DirectoryProperties.workingDir().toFile();
            String subTaskDirName = userDir.getName();
            Matcher m = pattern.matcher(subTaskDirName);
            m.matches();
            subTaskIndex = Integer.valueOf(m.group(1));
        }
        return subTaskIndex;
    }

    /**
     * Returns the files for the current subtask, based on the contents of a serialized instance of
     * {@link TaskConfigurationManager}.
     *
     * @return
     */
    public Set<String> filesForSubtask() {
        return TaskConfigurationManager.restoreAndRetrieveFilesForSubtask(
            PipelineInputsOutputsUtils.taskDir().toFile(), subtaskIndex());
    }

    /**
     * Returns a map from DataFileInfo subclasses to files in the parent directory that can be
     * managed by each subclass.
     *
     * @param dataFileInfoClasses
     * @return
     */
    public Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> resultsFiles(
        Set<Class<? extends DataFileInfo>> dataFileInfoClasses) {
        return new DataFileManager().dataFilesMap(taskDir(), dataFileInfoClasses);
    }

    /**
     * Returns a map from DataFileInfo subclasses to files in the parent directory that can be
     * managed by each subclass, where the set of subclasses is the set of all DataFileInfo
     * subclasses required by a given PipelineInputs class.
     *
     * @return
     */
    public Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> resultsFiles() {
        return resultsFiles(requiredDataFileInfoClasses());
    }

    /**
     * Loads an HDF5 file into a PipelineResults instance.
     *
     * @param <S>
     * @param <T>
     * @param dataFileInfo
     * @param resultsInstance
     */
    public <S extends DataFileInfo, T extends PipelineResults> void readResultsFile(S dataFileInfo,
        T resultsInstance) {
        log.info("Reading data file " + dataFileInfo.getName().toString());
        hdf5ModuleInterface.readFile(taskDir().resolve(dataFileInfo.getName()).toFile(),
            resultsInstance, true);
    }

    /**
     * Saves the object as an HDF5 file in the sub-task directory.
     *
     * @param seqNum
     */
    public void writeSubTaskInputs() {
        String moduleName = moduleName();
        String filename = ModuleInterfaceUtils.inputsFileName(moduleName);
        log.info("Writing file " + filename + " to sub-task directory");
        hdf5ModuleInterface.writeFile(DirectoryProperties.workingDir()
            .resolve(ModuleInterfaceUtils.inputsFileName(moduleName))
            .toFile(), this, true);
        ModuleInterfaceUtils.writeCompanionXmlFile(this, moduleName);
    }

    /**
     * Deletes temporary copies of datastore files used as task inputs from the task directory.
     *
     * @param pipelineTask pipeline task that used the inputs
     * @param taskDirectory Directory to be cleared of temporary inputs.
     */
    public void deleteTempInputsFromTaskDirectory(PipelineTask pipelineTask, Path taskDirectory) {
        DataFileManager fileManager = new DataFileManager(null, null, taskDirectory);
        Set<? extends DataFileInfo> inputsSet = fileManager.datastoreFiles(taskDirectory,
            requiredDataFileInfoClasses());
        fileManager.deleteFromTaskDirectory(inputsSet);
    }
}
