package gov.nasa.ziggy.module;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.ModuleParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Reference implementation of the {@link PipelineInputs} interface.
 * <p>
 * {@link DatastoreDirectoryPipelineInputs} provides an inputs class for pipeline modules that use
 * the {@link DatastoreDirectoryUnitOfWorkGenerator} to generate units of work. It uses the
 * {@link DataFileType} classes that are specified as inputs to the pipeline module to identify the
 * directories in the datastore that contain input files for the current module. This is combined
 * with information in the task's {@link UnitOfWork} to identify the exact files required for the
 * current task. These files are then copied or symlinked to the task directory. The
 * {@link DatastoreFileManager} class is also used for many of the low-level file location and file
 * copy operations.
 * <p>
 * The class also manages the models required for the pipeline module: the model types that are
 * stored with the pipeline definition node are used to copy the current versions of all needed
 * models to the task directory. Their names are stored in the modelFilenames member.
 * <p>
 * The class contains an instance of {@link ModuleParameters} that is used to hold the parameter
 * sets required for this pipeline module, which in turn are retrieved from the
 * {@link PipelineTask}.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineInputs implements PipelineInputs {

    @ProxyIgnore
    private static final Logger log = LoggerFactory
        .getLogger(DatastoreDirectoryPipelineInputs.class);

    private List<String> dataFilenames = new ArrayList<>();
    private List<String> modelFilenames = new ArrayList<>();
    private ModuleParameters moduleParameters = new ModuleParameters();

    @ProxyIgnore
    private PipelineTask pipelineTask;
    @ProxyIgnore
    private DatastoreFileManager datastoreFileManager;
    @ProxyIgnore
    private AlertService alertService = new AlertService();
    @ProxyIgnore
    private Path taskDirectory;

    public DatastoreDirectoryPipelineInputs() {
    }

    /** Locates input files in the datastore using {@link DataFileType} instances. */

    /**
     * Prepares the task directory for processing. Subtasks are generated based on whether the unit
     * of work indicates that a single subtask, or multiple subtasks, should be utilized. Data files
     * are copied into subtask directories. Module parameters are inserted into the parameterSets
     * member. An instance of {@link DatastoreDirectoryPipelineInputs} is serialized to each subtask
     * directory, with the input files for the given subtask included in the instance serialized to
     * that directory.
     */
    @Override
    public void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfiguration,
        Path taskDirectory) {

        log.info("Preparing task directory...");

        // Determine the files that need to be copied / linked to the task directory.
        // The result will be a List of Set<Path> instances, one list element for each
        // subtask. Later on we'll deal with the possibility that the pipeline definition
        // node wants a single subtask.
        Map<String, Set<Path>> filesForSubtasks = datastoreFileManager().filesForSubtasks();
        Map<Path, String> modelFilesForTask = datastoreFileManager().modelFilesForTask();

        // Populate the module parameters
        moduleParameters.setModuleParameters(getModuleParameters(getPipelineTask()));

        // Populate the subtasks.
        Map<Path, Set<Path>> pathsBySubtaskDirectory = datastoreFileManager()
            .copyDatastoreFilesToTaskDirectory(new HashSet<>(filesForSubtasks.values()),
                modelFilesForTask);

        // Capture the file name regular expressions for output data file types. This will
        // be used later to determine whether any given subtask has any outputs.
        Set<DataFileType> outputDataFileTypes = pipelineTask.pipelineDefinitionNode()
            .getOutputDataFileTypes();

        // Note: for some reason, when I try to use the outputDataFileTypes directly,
        // rather than putting them into a new Set, PipelineInputsOutputsUtils
        // attempts to serialize the PipelineDefinitionNode.
        PipelineInputsOutputsUtils.serializeOutputFileTypesToTaskDirectory(
            new HashSet<>(outputDataFileTypes), taskDirectory);

        // Write the inputs to each of the subtask directories, with the correct file names
        // in the file names list and the correct model names in the model names list.
        for (Map.Entry<Path, Set<Path>> entry : pathsBySubtaskDirectory.entrySet()) {
            dataFilenames.clear();
            modelFilenames.clear();
            for (Path file : entry.getValue()) {
                dataFilenames.add(file.getFileName().toString());
            }
            modelFilenames.addAll(modelFilesForTask.values());
            PipelineInputsOutputsUtils.writePipelineInputsToDirectory(this,
                getPipelineTask().getModuleName(), entry.getKey());
        }

        taskConfiguration.setSubtaskCount(pathsBySubtaskDirectory.size());
        log.info("Preparing task directory...done");
    }

    /**
     * Determines the number of subtasks for a {@link PipelineTask}. This is done by checking to see
     * whether the UOW indicates that a single subtask is required, and if not, counting the data
     * files of any of the input data file types in the datastore directories that will be used by
     * the {@link PipelineTask}.
     */
    @Override
    public SubtaskInformation subtaskInformation() {
        if (singleSubtask()) {
            return new SubtaskInformation(getPipelineTask().getModuleName(),
                getPipelineTask().uowTaskInstance().briefState(), 1);
        }
        int subtaskCount = datastoreFileManager().subtaskCount();
        return new SubtaskInformation(getPipelineTask().getModuleName(),
            getPipelineTask().uowTaskInstance().briefState(), subtaskCount);
    }

    /**
     * Returns the module-level and pipeline-level parameter sets.
     */
    private List<ParametersInterface> getModuleParameters(PipelineTask pipelineTask) {

        List<ParametersInterface> allParameters = new ArrayList<>();
        log.info("Retrieving module and pipeline parameters");
        allParameters.addAll(getModuleParameters(
            getPipelineTask().getPipelineInstance().getPipelineParameterSets()));
        allParameters.addAll(getModuleParameters(
            getPipelineTask().getPipelineInstanceNode().getModuleParameterSets()));
        log.info("Retrieved {} parameter sets", allParameters.size());
        return allParameters;
    }

    /** Returns parameter sets from a given {@link Map}. */
    private List<ParametersInterface> getModuleParameters(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetMap) {
        List<ParametersInterface> parameters = new ArrayList<>();

        for (ParameterSet parameterSet : parameterSetMap.values()) {
            Parameters instance = parameterSet.parametersInstance();
            if (instance instanceof Parameters) {
                Parameters defaultInstance = instance;
                defaultInstance.setName(parameterSet.getName());
            }
            parameters.add(instance);
        }
        return parameters;
    }

    public List<String> getDataFilenames() {
        return dataFilenames;
    }

    public void setDataFilenames(List<String> filenames) {
        dataFilenames = filenames;
    }

    public List<String> getModelFilenames() {
        return modelFilenames;
    }

    public void setModelFilenames(List<String> filenames) {
        modelFilenames = filenames;
    }

    public void setModuleParameters(ModuleParameters moduleParameters) {
        this.moduleParameters = moduleParameters;
    }

    public ModuleParameters getModuleParameters() {
        return moduleParameters;
    }

    AlertService alertService() {
        return alertService;
    }

    DatastoreFileManager datastoreFileManager() {
        if (datastoreFileManager == null) {
            datastoreFileManager = new DatastoreFileManager(getPipelineTask(), taskDirectory);
        }
        return datastoreFileManager;
    }

    /** Populates the log stream identifier just prior to algorithm execution. */
    @Override
    public void beforeAlgorithmExecution() {
        PipelineInputsOutputsUtils.putLogStreamIdentifier();
    }

    @Override
    public void writeParameterSetsToTaskDirectory() {
        // This isn't actually needed, since the parameter sets are included in the
        // DatastoreDirectoryPipelineInputs instance, which is serialized to the
        // task directory.
    }

    @Override
    public void setPipelineTask(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    @Override
    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    boolean singleSubtask() {
        return getPipelineTask().getPipelineInstanceNode()
            .getPipelineDefinitionNode()
            .getSingleSubtask();
    }

    @Override
    public void setTaskDirectory(Path taskDirectory) {
        this.taskDirectory = taskDirectory;
    }

    @Override
    public Path getTaskDirectory() {
        return taskDirectory;
    }
}
