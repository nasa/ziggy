package gov.nasa.ziggy.module;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.data.management.DatastorePathLocator;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.parameters.ModuleParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.TaskConfigurationParameters;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Default pipeline inputs class for use by pipeline modules that employ DataFileType instances to
 * define their data file needs. The combination of the DataFileType instances and the task unit of
 * work make it possible to identify all the files needed by each subtask and to determine the total
 * number of subtasks. Class methods can then copy all the files to the task directory and configure
 * units of work for each subtask.
 * <p>
 * The class also manages the models required for the pipeline module: the model types that are
 * stored with the pipeline definition node are used to copy the current versions of all needed
 * models to the task directory. Their names are stored in the modelFilenames member.
 * <p>
 * The DefaultPipelineInputs class can only be used in cases where the pipeline module's unit of
 * work is the {@link DatastoreDirectoryUnitOfWorkGenerator} and where the DataFileTypes are used
 * for all data files required by the pipeline module; for cases where either a single subtask or
 * one subtask per dataset is used; and for cases in which all subtasks can execute in parallel. For
 * modules that require more complicated arrangements, users are directed to write their own
 * extensions of the PipelineInputs abstract class.
 *
 * @author PT
 */
public class DefaultPipelineInputs extends PipelineInputs {

    @ProxyIgnore
    private static final Logger log = LoggerFactory.getLogger(DefaultPipelineInputs.class);

    private List<String> dataFilenames = new ArrayList<>();
    private List<String> modelFilenames = new ArrayList<>();
    private ModuleParameters moduleParameters = new ModuleParameters();
    private List<DataFileType> outputDataFileTypes = new ArrayList<>();

    @ProxyIgnore
    private DataFileManager dataFileManager;
    @ProxyIgnore
    private AlertService alertService;

    public DefaultPipelineInputs() {
    }

    /**
     * Constructor for test purposes only. Allows a partially mocked DataFileManager to be inserted.
     */
    DefaultPipelineInputs(DataFileManager dataFileManager, AlertService alertService) {
        this.dataFileManager = dataFileManager;
        this.alertService = alertService;
    }

    /**
     * This implementation of PipelineInputs does not use a DatastorePathLocator.
     */
    @Override
    public DatastorePathLocator datastorePathLocator(PipelineTask pipelineTask) {
        return null;
    }

    @Override
    public Set<Path> findDatastoreFilesForInputs(PipelineTask pipelineTask) {

        // Obtain the data file types that the module requires
        Set<DataFileType> dataFileTypes = pipelineTask.getPipelineDefinitionNode()
            .getInputDataFileTypes();

        UnitOfWork uow = pipelineTask.getUowTask().getInstance();

        // find the data files for the task
        DataFileManager dataFileManager = dataFileManager(DirectoryProperties.datastoreRootDir(),
            null, pipelineTask);

        return dataFileManager.dataFilesForInputs(
            Paths.get(
                uow.getParameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME).getString()),
            dataFileTypes, pipelineTask.getParameters(TaskConfigurationParameters.class));
    }

    /**
     * Prepares the task directory for processing. All data files are copied to the task directory
     * based on the data file types needed for this module and the section of the datastore that the
     * unit of work indicates should be used. Subtasks are generated based on whether the unit of
     * work indicates that a single subtask, or multiple subtasks, should be utilized. Module
     * parameters are inserted into the parameterSets member and serialized to HDF5 in the task
     * directory.
     */
    @Override
    public void copyDatastoreFilesToTaskDirectory(TaskConfigurationManager taskConfigurationManager,
        PipelineTask pipelineTask, Path taskDirectory) {

        // Obtain the data file types that the module requires
        Set<DataFileType> dataFileTypes = pipelineTask.getPipelineDefinitionNode()
            .getInputDataFileTypes();

        // Store the output data file types
        outputDataFileTypes
            .addAll(pipelineTask.getPipelineDefinitionNode().getOutputDataFileTypes());

        // Obtain the unit of work
        UnitOfWork uow = pipelineTask.getUowTask().getInstance();
        String directory = DirectoryUnitOfWorkGenerator.directory(uow);
        log.info("Unit of work directory: " + directory);

        // populate the module parameters
        populateModuleParameters(pipelineTask);

        // Identify the files to be copied from the datastore to the task directory
        DataFileManager dataFileManager = dataFileManager(DirectoryProperties.datastoreRootDir(),
            taskDirectory, pipelineTask);
        Map<DataFileType, Set<Path>> dataFilesMap = dataFileManager
            .datastoreDataFilesMap(Paths.get(directory), dataFileTypes);

        Set<String> truncatedFilenames = filterDataFilesIfUnequalCounts(dataFilesMap,
            pipelineTask.getId());

        // Copy the data files from the datastore to the task directory
        log.info("Copying data files of " + dataFileTypes.size() + " type(s) to working directory "
            + taskDirectory.toString());
        dataFileManager.copyDataFilesByTypeToTaskDirectory(dataFilesMap,
            pipelineTask.getParameters(TaskConfigurationParameters.class));
        log.info("Data file copy completed");

        // Copy the current models of the required types to the task directory
        Set<ModelType> modelTypes = pipelineTask.getPipelineDefinitionNode().getModelTypes();
        ModelRegistry modelRegistry = pipelineTask.getPipelineInstance().getModelRegistry();
        modelFilenames
            .addAll(dataFileManager.copyModelFilesToTaskDirectory(modelRegistry, modelTypes, log));

        // Construct a Map that goes from the truncated file names to a Set<Path> of objects
        // for each truncated file name
        Map<String, Set<Path>> subtaskPathsMap = new TreeMap<>();
        for (String truncatedFileName : truncatedFilenames) {
            subtaskPathsMap.put(truncatedFileName, new HashSet<Path>());
        }

        // Loop over DataFileType instances from the dataFilesMap
        for (DataFileType dataFileType : dataFilesMap.keySet()) {
            Set<Path> datastorePaths = dataFilesMap.get(dataFileType);
            for (Path datastorePath : datastorePaths) {

                // For each file, find its truncated name ...
                String truncatedFilename = datastorePath.getFileName().toString().split("\\.")[0];

                // ... and then put the task dir path into that set of paths!
                Set<Path> subtaskPaths = subtaskPathsMap.get(truncatedFilename);
                subtaskPaths.add(Paths.get(
                    dataFileType.taskDirFileNameFromDatastoreFileName(datastorePath.toString())));
            }
        }

        // now we do different things depending on the desired subtask configuration
        boolean singleSubtask = DatastoreDirectoryUnitOfWorkGenerator.singleSubtask(uow);

        if (truncatedFilenames.size() != 0) {
            if (singleSubtask) {
                log.info("Configuring single subtask for task");
            } else {
                log.info("Configuring " + truncatedFilenames.size() + " subtasks for task");
            }
        } else {
            log.info("No files require processing in this task, no subtasks configured");
        }

        Set<String> subtaskFilenamesAllSubtasks = new TreeSet<>();
        for (String truncatedFilename : subtaskPathsMap.keySet()) {
            Set<String> subtaskFilenames = new TreeSet<>();
            subtaskPathsMap.get(truncatedFilename)
                .stream()
                .map(s -> s.getFileName().toString())
                .forEach(s -> subtaskFilenames.add(s));
            subtaskFilenamesAllSubtasks.addAll(subtaskFilenames);
            if (!singleSubtask) {
                taskConfigurationManager.addFilesForSubtask(subtaskFilenames);
            }
        }
        if (singleSubtask && truncatedFilenames.size() != 0) {
            taskConfigurationManager.addFilesForSubtask(subtaskFilenamesAllSubtasks);
        }

        // write the contents of this file to HDF5 in the task directory
        log.info("Writing parameters to task directory");
        writeToTaskDir(pipelineTask, taskDirectory.toFile());
        log.info("Task directory preparation complete");

    }

    /**
     * Handles the case in which the different data file types have different numbers of files
     * identified for this UOW. This can happen if, for example, a task combines results from a
     * prior task with another source of inputs: in this case, if the user doesn't processes a
     * subset of available files in the prior task, the file counts of these two data file types
     * will not match.
     * <p>
     * In this case, we assume that the data file type that has the fewest files is the one that
     * controls the selection of files in the other types. We also assume that we can use the
     * standard approach of matching files from the different data file types: their base names
     * should match. Thus we can discard any file that has a base name that is not represented in
     * the shortest set of data file paths.
     * <p>
     *
     * @param dataFilesMap {@link Map} between the instances of {@link DataFileType}, and the
     * {@link Set} of {@link Path} instances found for that type in the datastore. This map is
     * altered in place to contain only the files that should be copied to the task directory.
     * @return the {@link Set} of truncated file names that are present in this UOW.
     */
    private Set<String> filterDataFilesIfUnequalCounts(Map<DataFileType, Set<Path>> dataFilesMap,
        long pipelineTaskId) {

        List<Integer> pathSetSizes = new ArrayList<>();
        int minPathSetSize = Integer.MAX_VALUE;
        Set<Path> shortestSetOfPaths = null;
        for (Set<Path> paths : dataFilesMap.values()) {
            pathSetSizes.add(paths.size());
            if (paths.size() < minPathSetSize) {
                shortestSetOfPaths = paths;
                minPathSetSize = paths.size();
            }
        }
        boolean setLengthsMatch = true;
        for (int pathSetSize : pathSetSizes) {
            setLengthsMatch = setLengthsMatch && pathSetSize == minPathSetSize;
        }

        // Now we need to identify the files in each set that match a file in the shortest
        // set. First step: construct a set of file base names.
        Set<String> baseNames = shortestSetOfPaths.stream()
            .map(this::baseName)
            .collect(Collectors.toSet());

        // Here is where we handle the case of mismatched file set lengths.
        if (!setLengthsMatch) {
            log.warn("Mismatch in data file counts for UOW: " + pathSetSizes.toString());
            alertService().generateAndBroadcastAlert("PI (DefaultPipelineInputs)", pipelineTaskId,
                AlertService.Severity.WARNING,
                "Mismatch in data file counts for UOW: " + pathSetSizes.toString());
            for (DataFileType dataFileType : dataFilesMap.keySet()) {
                Set<Path> filteredPaths = dataFilesMap.get(dataFileType)
                    .stream()
                    .filter(s -> baseNames.contains(baseName(s)))
                    .collect(Collectors.toSet());

                dataFilesMap.put(dataFileType, filteredPaths);
            }
        }
        return new TreeSet<>(baseNames);
    }

    private String baseName(Path dataFilePath) {
        return dataFilePath.getFileName().toString().split("\\.")[0];
    }

    /**
     * Prepares the per-subtask inputs HDF5 file. In the case of the DefaultPipelineInputs, the HDF5
     * file contains only a list of files to be processed by the selected unit of work and all
     * parameter sets associated with this processing module. The files are also copied to the
     * subtask directory.
     */
    @Override
    public void populateSubTaskInputs() {

        // Set the subtask information into the thread for logging purposes
        PipelineInputsOutputsUtils.putLogStreamIdentifier();
        // Recover the parameter sets from the task directory
        readFromTaskDir();
        dataFilenames = new ArrayList<>();

        Set<String> uowFilenames = filesForSubtask();
        dataFilenames.addAll(uowFilenames);

        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        dataFileManager = new DataFileManager(DirectoryProperties.datastoreRootDir(), taskDir,
            null);
        log.info(dataFilenames.size() + " filenames added to UOW");
        log.info("Copying inputs files into subtask directory");
        dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(dataFilenames);

        // now copy the models from the task directory to the working directory
        if (!modelFilenames.isEmpty()) {
            log.info("Copying " + modelFilenames.size() + " model files into subtask directory");
            dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(modelFilenames);
        }
        log.info("Persisting inputs information to subtask directory");
        writeSubTaskInputs(0);
        log.info("Persisting inputs completed");

    }

    /**
     * Deletes the copies of datastore files used as inputs. This method is run by the
     * ExternalProcessPipelineModule after the module processing has completed successfully.
     */
    @Override
    public void deleteTempInputsFromTaskDirectory(PipelineTask pipelineTask, Path taskDirectory) {

        // Obtain the data file types that the module requires
        Set<DataFileType> dataFileTypes = pipelineTask.getPipelineDefinitionNode()
            .getInputDataFileTypes();

        // Use the DataFileManager to delete the temporary data files
        dataFileManager(null, taskDirectory, pipelineTask)
            .deleteDataFilesByTypeFromTaskDirectory(dataFileTypes);

        // Get the model registry and the set of model types
        ModelRegistry modelRegistry = pipelineTask.getPipelineInstance().getModelRegistry();
        Set<ModelType> modelTypes = pipelineTask.getPipelineDefinitionNode().getModelTypes();

        // delete all the model files in the task directory
        for (ModelType modelType : modelTypes) {
            ModelMetadata modelMetadata = modelRegistry.getModels().get(modelType);
            Path modelFile = taskDirectory.resolve(modelMetadata.getOriginalFileName());
            if (Files.isRegularFile(modelFile)) {
                try {
                    Files.delete(modelFile);
                } catch (IOException e) {
                    throw new PipelineException("Unable to delete model file "
                        + modelFile.getFileName().toString() + " from task directory");
                }
            }
        }

    }

    /**
     * Populates the moduleParameters member with module-level and pipeline-level parameter sets.
     */
    protected void populateModuleParameters(PipelineTask pipelineTask) {

        List<Parameters> allParameters = new ArrayList<>();
        log.info("Retrieving module and pipeline parameters");
        allParameters.addAll(
            getModuleParameters(pipelineTask.getPipelineInstance().getPipelineParameterSets()));
        allParameters.addAll(
            getModuleParameters(pipelineTask.getPipelineInstanceNode().getModuleParameterSets()));
        log.info("Retrieved " + allParameters.size() + " parameter sets");
        moduleParameters.setModuleParameters(allParameters);
    }

    /**
     * Determines the number of subtasks for a {@link PipelineTask}. This is done by checking to see
     * whether the UOW indicates that a single subtask is required, and if not, counting the data
     * files of any of the input data file types in the datastore directory that will be managed by
     * the {@link PipelineTask}.
     */
    @Override
    public SubtaskInformation subtaskInformation(PipelineTask pipelineTask) {
        UnitOfWork uow = pipelineTask.getUowTask().getInstance();
        if (DatastoreDirectoryUnitOfWorkGenerator.singleSubtask(uow)) {
            return new SubtaskInformation(pipelineTask.getModuleName(), uow.briefState(), 1, 1);
        }

        Set<DataFileType> dataFileTypes = pipelineTask.getPipelineDefinitionNode()
            .getInputDataFileTypes();
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        DataFileManager dataFileManager = dataFileManager(datastoreRoot, null, pipelineTask);
        int subtaskCount = dataFileManager.countDatastoreFilesOfType(
            dataFileTypes.iterator().next(), Paths.get(DirectoryUnitOfWorkGenerator.directory(uow)),
            pipelineTask.getParameters(TaskConfigurationParameters.class));
        return new SubtaskInformation(pipelineTask.getModuleName(), uow.briefState(), subtaskCount,
            subtaskCount);
    }

    /**
     * Inner method for parameter retrieval.
     */
    private List<Parameters> getModuleParameters(
        Map<ClassWrapper<Parameters>, ParameterSet> parameterSetMap) {
        List<Parameters> parameters = new ArrayList<>();

        Collection<ParameterSet> parameterSets = parameterSetMap.values();
        for (ParameterSet parameterSet : parameterSets) {
            Parameters instance = parameterSet.parametersInstance();
            if (instance instanceof DefaultParameters) {
                DefaultParameters defaultInstance = (DefaultParameters) instance;
                defaultInstance.setName(parameterSet.getName().getName());
            }
            parameters.add(instance);
        }
        return parameters;

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

    public List<DataFileType> getOutputDataFileTypes() {
        return outputDataFileTypes;
    }

    public void setOutputDataFileTypes(List<DataFileType> outputDataFileTypes) {
        this.outputDataFileTypes = outputDataFileTypes;
    }

    // Package scope so that a partially mocked-out DataFileManager can be supplied.
    DataFileManager dataFileManager(Path datastorePath, Path taskDirPath,
        PipelineTask pipelineTask) {
        if (dataFileManager == null) {
            dataFileManager = new DataFileManager(datastorePath, taskDirPath, pipelineTask);
        }
        return dataFileManager;
    }

    private AlertService alertService() {
        if (alertService == null) {
            alertService = AlertService.getInstance();
        }
        return alertService;
    }

}
