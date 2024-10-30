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
import gov.nasa.ziggy.data.datastore.DatastoreCopier;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.SubtaskDefinition;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
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
 * current task. These files are then copied or hard linked to the task directory. The
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

    @ProxyIgnore
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    @ProxyIgnore
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    @ProxyIgnore
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

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
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager().subtaskDefinitions();
        Map<Path, String> modelTaskFilesByDatastorePath = datastoreFileManager()
            .modelTaskFilesByDatastorePath();

        // Populate the module parameters
        moduleParameters.addParameterSets(getParameterSets(getPipelineTask()));

        // Populate the subtasks.
        Map<Path, Set<Path>> pathsBySubtaskDirectory = datastoreFileManager()
            .copyDatastoreFilesToTaskDirectory(subtaskDefinitions, modelTaskFilesByDatastorePath);

        // Capture the file name regular expressions for output data file types. This will
        // be used later to determine whether any given subtask has any outputs.
        Set<DataFileType> outputDataFileTypes = pipelineTaskOperations()
            .outputDataFileTypes(pipelineTask);

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
            modelFilenames.addAll(modelTaskFilesByDatastorePath.values());
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
    public SubtaskInformation subtaskInformation(PipelineDefinitionNode pipelineDefinitionNode) {
        if (pipelineDefinitionNode.getSingleSubtask()) {
            return new SubtaskInformation(getPipelineTask().getModuleName(),
                getPipelineTask().getUnitOfWork().briefState(), 1);
        }
        return new SubtaskInformation(getPipelineTask().getModuleName(),
            getPipelineTask().getUnitOfWork().briefState(),
            datastoreFileManager().subtaskCount(pipelineDefinitionNode));
    }

    private Set<ParameterSet> getParameterSets(PipelineTask pipelineTask) {
        log.info("Retrieving module and pipeline parameters");
        Set<ParameterSet> parameterSets = pipelineInstanceOperations()
            .parameterSets(pipelineTaskOperations().pipelineInstance(getPipelineTask()));
        parameterSets.addAll(pipelineInstanceNodeOperations()
            .parameterSets(pipelineTaskOperations().pipelineInstanceNode(pipelineTask)));
        log.info("Retrieved {} parameter sets", parameterSets.size());
        return parameterSets;
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
            if (datastoreToTaskDirCopier() != null) {
                datastoreFileManager.setDatastoreToTaskDirCopier(datastoreToTaskDirCopier());
            }
        }
        return datastoreFileManager;
    }

    /**
     * Determines the {@link DatastoreCopier} that will be used by the {@link DatastoreFileManager}
     * to copy files from the datastore to the task directory. Subclasses can override this method
     * if they want the file copier to do something different from its nominal behavior (example:
     * use symbolic links instead of hard links, etc.).
     */
    public DatastoreCopier datastoreToTaskDirCopier() {
        return null;
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
        return pipelineTaskOperations().pipelineDefinitionNode(getPipelineTask())
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

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }
}
