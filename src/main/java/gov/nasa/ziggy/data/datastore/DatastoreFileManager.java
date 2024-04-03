package gov.nasa.ziggy.data.datastore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.module.AlgorithmStateFiles;
import gov.nasa.ziggy.module.SubtaskUtils;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Provides services related to marshaling and persisting data files, and transporting them between
 * the datastore and the task directories. These services include:
 * <ol>
 * <li>Creating inputs and outputs subdirectories in the task directory for a given
 * {@link PipelineTask}.
 * <li>Identifying the datastore directories that contain files to use as inputs for a task.
 * <li>Copying or linking the input files for a task to a subtask directory of the task directory.
 * <li>Copying either all files or all newly-created files, depending on whether the use-case is
 * reprocessing or forward processing.
 * <li>Copying or moving the output files from the subtasks of the task directory to the datastore.
 * <li>Managing file permissions for the datastore: the files in the datastore are write-protected
 * except when being deliberately overwritten with newer results.
 * <ol>
 *
 * @author PT
 */

public class DatastoreFileManager {

    private static final Logger log = LoggerFactory.getLogger(DatastoreFileManager.class);

    private static final Predicate<? super File> WITH_OUTPUTS = AlgorithmStateFiles::hasOutputs;
    private static final Predicate<? super File> WITHOUT_OUTPUTS = WITH_OUTPUTS.negate();
    public static final String FILE_NAME_DELIMITER = "\\.";
    public static final String SINGLE_SUBTASK_BASE_NAME = "Single Subtask";

    private final PipelineTask pipelineTask;
    private AlertService alertService = new AlertService();
    private DatastoreWalker datastoreWalker;
    private final Path taskDirectory;
    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud = new DatastoreProducerConsumerCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

    public DatastoreFileManager(PipelineTask pipelineTask, Path taskDirectory) {
        this.pipelineTask = pipelineTask;
        this.taskDirectory = taskDirectory;
    }

    /**
     * Constructs the collection of {@link Path}s for each subtask.
     * <p>
     * All subtasks must have one data file from each file-per-subtask data file type. Any subtask
     * that is missing one or more files is omitted from the returned {@link List}.
     */
    public Map<String, Set<Path>> filesForSubtasks() {

        // Obtain the data file types that the module requires
        Set<DataFileType> dataFileTypes = pipelineTask.pipelineDefinitionNode()
            .getInputDataFileTypes();
        // Construct a List of data file types that expect 1 file per subtask.
        List<DataFileType> filePerSubtaskDataFileTypes = dataFileTypes.stream()
            .filter(s -> !s.isIncludeAllFilesInAllSubtasks())
            .collect(Collectors.toList());

        // Construct a list of data file types for which all files need to be provided
        // to all subtasks.
        List<DataFileType> allFilesAllSubtasksDataFileTypes = new ArrayList<>(dataFileTypes);
        allFilesAllSubtasksDataFileTypes.removeAll(filePerSubtaskDataFileTypes);

        UnitOfWork uow = pipelineTask.uowTaskInstance();
        // Generate a Map from each file-per-subtask data file type to all the data files for
        // that type; then the same for the all-files-all-subtask types.
        Map<DataFileType, Set<Path>> pathsByPerSubtaskDataType = pathsByDataFileType(uow,
            filePerSubtaskDataFileTypes);
        Map<DataFileType, Set<Path>> pathsByAllSubtasksDataType = pathsByDataFileType(uow,
            allFilesAllSubtasksDataFileTypes);

        // If the user wants new-data processing only, filter the data files to remove
        // any that were processed already by the pipeline module that's assigned to
        // this pipeline task.
        if (!singleSubtask() && pipelineDefinitionCrud()
            .retrieveProcessingMode(
                pipelineTask.getPipelineInstance().getPipelineDefinition().getName())
            .equals(ProcessingMode.PROCESS_NEW)) {
            filterOutDataFilesAlreadyProcessed(pathsByPerSubtaskDataType);
        }

        // Produce the List using just the file-per-subtask data files.
        Map<String, Set<Path>> filesForSubtasks = filePerSubtaskFilesForSubtasks(
            pathsByPerSubtaskDataType);

        // if this task will use a single subtask, it's possible that it has
        // no input types that are in the one-file-per-subtask category. Handle
        // that corner case now.
        if (singleSubtask() && filesForSubtasks.isEmpty()) {
            filesForSubtasks.put(SINGLE_SUBTASK_BASE_NAME, new HashSet<>());
        }

        // Add the all-files-all-subtasks paths to all the subtasks.
        Set<Path> allFilesAllSubtasks = new HashSet<>();
        for (Set<Path> files : pathsByAllSubtasksDataType.values()) {
            allFilesAllSubtasks.addAll(files);
        }
        for (Set<Path> files : filesForSubtasks.values()) {
            files.addAll(allFilesAllSubtasks);
        }
        return filesForSubtasks;
    }

    /**
     * Produces a {@link Map} from a given {@link DataFileType} to the data files for that type,
     * based on the unit of work.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private Map<DataFileType, Set<Path>> pathsByDataFileType(UnitOfWork uow,
        List<DataFileType> dataFileTypes) {
        Map<DataFileType, Set<Path>> pathsByDataFileType = new HashMap<>();

        // Obtain the Map from data file type names to UOW paths
        Map<String, String> pathsByDataTypeName = DirectoryUnitOfWorkGenerator
            .directoriesByDataFileType(uow);
        for (DataFileType dataFileType : dataFileTypes) {
            Path datastoreDirectory = Paths.get(pathsByDataTypeName.get(dataFileType.getName()));
            pathsByDataFileType.put(dataFileType,
                FileUtil.listFiles(datastoreDirectory, dataFileType.getFileNameRegexp()));
        }
        return pathsByDataFileType;
    }

    /**
     * Filters out data files that have already been processed for situations in which the user only
     * wants to process new data files (i.e., files that have not yet been processed).
     * <p>
     * The method works by obtaining the {@link DatastoreProducerConsumer} records for all the files
     * in the datastore that are going to be processed by this task. It then finds the intersection
     * of the consumer task IDs for the files and IDs for tasks that share the same pipeline
     * definition node. Any file that has a consumer in that intersection set must be omitted from
     * processing.
     */
    private void filterOutDataFilesAlreadyProcessed(
        Map<DataFileType, Set<Path>> pathsByPerSubtaskDataType) {

        for (Set<Path> paths : pathsByPerSubtaskDataType.values()) {

            // The names in the producer-consumer table are relative to the datastore
            // root, while the values in pathsByPerSubtaskDataType are absolute.
            // Generate a relativized Set now.
            Set<String> relativizedFilePaths = paths.stream()
                .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
                .map(Path::toString)
                .collect(Collectors.toSet());

            // Find the consumers that correspond to the definition node of the current task.
            List<Long> consumersWithMatchingPipelineNode = pipelineTaskCrud()
                .retrieveIdsForPipelineDefinitionNode(pipelineTask.pipelineDefinitionNode(), null);

            // Obtain the Set of datastore files that are in the relativizedFilePaths collection
            // and which have a consumer that matches the pipeline definition node of the current
            // pipeline task.
            Set<String> namesOfFilesAlreadyProcessed = datastoreProducerConsumerCrud()
                .retrieveFilesConsumedByTasks(consumersWithMatchingPipelineNode,
                    relativizedFilePaths);

            if (CollectionUtils.isEmpty(namesOfFilesAlreadyProcessed)) {
                continue;
            }

            // Convert the strings back to absolute paths.
            Set<Path> filesAlreadyProcessed = namesOfFilesAlreadyProcessed.stream()
                .map(Paths::get)
                .map(t -> DirectoryProperties.datastoreRootDir().toAbsolutePath().resolve(t))
                .collect(Collectors.toSet());

            // Remove the files already processed from the set of paths.
            paths.removeAll(filesAlreadyProcessed);
        }
    }

    /**
     * Generates the portion of the {@link List} of {@link Set}s of {@link Paths} for each subtask
     * that comes from file-per-subtask data types. Returns a {@link Map} from the data file base
     * name (i.e., everything before the first "." in its name) to all the data files that have that
     * base name. Each Map entry's value are the set of input files needed for a given subtask.
     */
    private Map<String, Set<Path>> filePerSubtaskFilesForSubtasks(
        Map<DataFileType, Set<Path>> pathsByPerSubtaskDataType) {

        if (singleSubtask()) {
            Set<Path> allDataFiles = new HashSet<>();
            for (Set<Path> paths : pathsByPerSubtaskDataType.values()) {
                allDataFiles.addAll(paths);
            }
            return Map.of(SINGLE_SUBTASK_BASE_NAME, allDataFiles);
        }

        // Generate the mapping from regexp group values to sets of files.
        Map<String, Set<Path>> filePerSubtaskFilesForSubtasks = new HashMap<>();
        for (Map.Entry<DataFileType, Set<Path>> entry : pathsByPerSubtaskDataType.entrySet()) {
            addPathsByRegexpGroupValues(filePerSubtaskFilesForSubtasks, entry);
        }

        // Check for cases that have insufficient files. These are cases in which one or more
        // data file type has no file for the given subtasks, which means that these are subtasks
        // that cannot run. Note that the logic of regular expressions guarantees that each data
        // file type can produce no more than one file that matches a given data file type regexp.
        int subtaskCount = filePerSubtaskFilesForSubtasks.size();
        int dataFileTypeCount = pathsByPerSubtaskDataType.size();
        Set<String> regexpGroupValuesForInvalidSubtasks = new HashSet<>();
        for (Map.Entry<String, Set<Path>> subtaskMapEntry : filePerSubtaskFilesForSubtasks
            .entrySet()) {
            if (subtaskMapEntry.getValue().size() < dataFileTypeCount) {
                regexpGroupValuesForInvalidSubtasks.add(subtaskMapEntry.getKey());
            }
        }
        if (!regexpGroupValuesForInvalidSubtasks.isEmpty()) {
            log.warn("{} subtasks out of {} missing files and will not be processed",
                regexpGroupValuesForInvalidSubtasks.size(), subtaskCount);
            for (String regexpGroupValuesForInvalidSubtask : regexpGroupValuesForInvalidSubtasks) {
                filePerSubtaskFilesForSubtasks.remove(regexpGroupValuesForInvalidSubtask);
            }
        }
        return filePerSubtaskFilesForSubtasks;
    }

    /**
     * Adds the {@link Path}s for a given {@link DataFileType} to the overall {@link Map} of paths
     * by concatenated regexp group values. Each entry in the map represents a subtask in which all
     * of the data files for the subtask have matching values for their regexp groups.
     */
    private void addPathsByRegexpGroupValues(Map<String, Set<Path>> pathsByRegexpGroupValue,
        Map.Entry<DataFileType, Set<Path>> pathsByDataFileType) {
        Pattern dataFileTypePattern = Pattern
            .compile(pathsByDataFileType.getKey().getFileNameRegexp());

        for (Path path : pathsByDataFileType.getValue()) {
            String concatenatedRegexpGroups = concatenatedRegexpGroups(dataFileTypePattern, path);
            if (StringUtils.isBlank(concatenatedRegexpGroups)) {
                continue;
            }
            if (pathsByRegexpGroupValue.get(concatenatedRegexpGroups) == null) {
                pathsByRegexpGroupValue.put(concatenatedRegexpGroups, new HashSet<>());
            }
            pathsByRegexpGroupValue.get(concatenatedRegexpGroups).add(path);
        }
    }

    /**
     * Applies a {@link Pattern} to the file name element of a {@link Path}, and returns the
     * concatenation of the values of the regexp groups. For example, if the pattern is
     * "(\\S+)-bauhaus-(\\S+).nc" and the file name is "foo-bauhaus-baz.nc", then this method
     * returns "foobaz".
     */
    private String concatenatedRegexpGroups(Pattern dataFileTypePattern, Path file) {
        Matcher matcher = dataFileTypePattern.matcher(file.getFileName().toString());
        if (!matcher.matches()) {
            log.warn("File {} does not match regexp {}", file.getFileName().toString(),
                dataFileTypePattern.pattern());
            return null;
        }
        StringBuilder groupValueConcatenator = new StringBuilder();
        for (int groupIndex = 1; groupIndex <= matcher.groupCount(); groupIndex++) {
            groupValueConcatenator.append(matcher.group(groupIndex));
        }
        return groupValueConcatenator.toString();
    }

    /**
     * Returns the model files for the task. The return is in the form of a {@link Map} in which the
     * datastore paths of the current models are the keys and the names of the files in the task
     * directory are the values.
     */
    public Map<Path, String> modelFilesForTask() {
        Map<Path, String> modelFilesForTask = new HashMap<>();

        // Get the model registry and the model types from the pipeline task.
        ModelRegistry modelRegistry = pipelineTask.getPipelineInstance().getModelRegistry();
        Set<ModelType> modelTypes = pipelineTask.pipelineDefinitionNode().getModelTypes();

        // Put the model location in the datastore, and its original file name, into the Map.
        for (ModelType modelType : modelTypes) {
            ModelMetadata metadata = modelRegistry.getModels().get(modelType);
            modelFilesForTask.put(metadata.datastoreModelPath(), metadata.getOriginalFileName());
        }
        return modelFilesForTask;
    }

    /** Copies datastore files to the subtask directories. Both data files and models are copied. */
    public Map<Path, Set<Path>> copyDatastoreFilesToTaskDirectory(
        Collection<Set<Path>> subtaskFiles, Map<Path, String> modelFilesForTask) {

        List<Set<Path>> subtaskFilesCopy = new ArrayList<>(subtaskFiles);
        log.info("Generating subtasks...");
        // The algorithm may want one subtask per task. Handle that case now.
        if (pipelineTask.getPipelineInstanceNode().getPipelineDefinitionNode().getSingleSubtask()) {
            Set<Path> filesForSingleSubtask = new HashSet<>();
            for (Set<Path> files : subtaskFiles) {
                filesForSingleSubtask.addAll(files);
            }
            subtaskFilesCopy.clear();
            subtaskFilesCopy.add(filesForSingleSubtask);
        }

        Map<Path, Set<Path>> pathsBySubtaskDirectory = new HashMap<>();

        // Loop over subtasks.
        int subtaskIndex = 0;
        int loggingIndex = Math.max(1, subtaskFilesCopy.size() / 20);
        for (Set<Path> files : subtaskFilesCopy) {
            Path subtaskDirectory = SubtaskUtils.createSubtaskDirectory(taskDirectory(),
                subtaskIndex);

            // Copy or link the data files.
            for (Path file : files) {
                Path destination = subtaskDirectory.resolve(file.getFileName());
                copyOrLink(file, destination);
            }
            if (modelFilesForTask == null) {
                continue;
            }

            // Copy or link the models.
            for (Map.Entry<Path, String> modelEntry : modelFilesForTask.entrySet()) {
                Path destination = subtaskDirectory.resolve(modelEntry.getValue());
                copyOrLink(modelEntry.getKey(), destination);
            }
            if (subtaskIndex++ % loggingIndex == 0) {
                log.info("Subtask {} of {} generated", subtaskIndex, subtaskFilesCopy.size());
            }
            pathsBySubtaskDirectory.put(subtaskDirectory, files);
        }
        log.info("Generating subtasks...done");
        return pathsBySubtaskDirectory;
    }

    /**
     * Copies output files from the task directory to the datastore, returning the Set of datastore
     * Paths that result from the copy operations.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Set<Path> copyTaskDirectoryFilesToDatastore() {

        log.info("Copying output files to datastore...");
        Set<DataFileType> outputDataFileTypes = pipelineTask.pipelineDefinitionNode()
            .getOutputDataFileTypes();
        Map<String, String> regexpValues = DatastoreDirectoryUnitOfWorkGenerator
            .regexpValues(pipelineTask.uowTaskInstance());
        Map<DataFileType, Path> datastorePathByDataFileType = new HashMap<>();

        // Get a Map from each data file type to its location in the datastore. Here
        // we use the regexp values captured in the UOW, which in turn is captured
        // in the PipelineTask, to perform the mapping.
        for (DataFileType dataFileType : outputDataFileTypes) {
            datastorePathByDataFileType.put(dataFileType, datastoreWalker()
                .pathFromLocationAndRegexpValues(regexpValues, dataFileType.getLocation()));
        }

        // Generate the paths of all subtask directories.
        Set<Path> subtaskDirs = FileUtil.listFiles(taskDirectory(),
            Set.of(SubtaskUtils.SUBTASK_DIR_PATTERN), null);

        // Construct a Map from the data file type to the set of output files of that type.
        Map<DataFileType, Set<Path>> outputFilesByDataFileType = new HashMap<>();
        for (DataFileType dataFileType : outputDataFileTypes) {
            Set<Path> outputFiles = new HashSet<>();
            for (Path subtaskDir : subtaskDirs) {
                outputFiles
                    .addAll(FileUtil.listFiles(subtaskDir, dataFileType.getFileNameRegexp()));
            }
            outputFilesByDataFileType.put(dataFileType, outputFiles);
        }

        // Copy the files from the subtask directories to the correct datastore location.
        Set<Path> outputFiles = new HashSet<>();
        for (Map.Entry<DataFileType, Set<Path>> outputFilesEntry : outputFilesByDataFileType
            .entrySet()) {
            Path datastoreLocation = datastorePathByDataFileType.get(outputFilesEntry.getKey());
            try {
                Files.createDirectories(datastoreLocation);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            FileUtil.prepareDirectoryTreeForOverwrites(datastoreLocation);
            for (Path outputFile : outputFilesEntry.getValue()) {
                Path destination = datastoreLocation.resolve(outputFile.getFileName());
                copyOrLink(outputFile, destination);
                outputFiles.add(destination);
            }
            FileUtil.writeProtectDirectoryTree(datastoreLocation);
        }
        log.info("Copying output files to datastore...done");
        return outputFiles;
    }

    /** Returns the number of subtasks for a given task. */
    public int subtaskCount() {
        return filesForSubtasks().size();
    }

    /**
     * Determines the input files that are associated with outputs (i.e., they are used in a task
     * that produced outputs) and the files that are not associated with outputs. Returns an object
     * that provides both sets of information for the caller.
     */
    public InputFiles inputFilesByOutputStatus() {

        // Identify the subtasks that have, or fail to have, outputs.
        Set<Path> subtasksWithOutputs = subtaskDirectoriesWithOutputStatus(WITH_OUTPUTS);
        Set<Path> subtasksWithoutOutputs = subtaskDirectoriesWithOutputStatus(WITHOUT_OUTPUTS);

        // Construct the paths for each kind of subdirectory
        Set<Path> filesWithOutputs = inputsFilesInSubtaskDirectories(subtasksWithOutputs);
        Set<Path> filesWithoutOutputs = inputsFilesInSubtaskDirectories(subtasksWithoutOutputs);

        // If a file produced outputs in some subdirectories but not others, we need to count it
        // as producing outputs on this task, so remove any entries in filesWithOutputs from
        // the set of filesWithoutOutputs.
        filesWithoutOutputs.removeAll(filesWithOutputs);

        return new InputFiles(filesWithOutputs, filesWithoutOutputs);
    }

    /**
     * Returns the {@link Set} of subtask directory {@link Path}s that represent completed subtasks
     * with a given outputs status (either with or without outputs).
     */
    private Set<Path> subtaskDirectoriesWithOutputStatus(Predicate<? super File> outputsStatus) {
        return SubtaskUtils.subtaskDirectories(taskDirectory)
            .stream()
            .map(Path::toFile)
            .filter(AlgorithmStateFiles::isComplete)
            .filter(outputsStatus)
            .map(File::toPath)
            .collect(Collectors.toSet());
    }

    /** Returns the {@link Set} of input file {@link Path}s from a set of subdirectory Paths. */
    private Set<Path> inputsFilesInSubtaskDirectories(Set<Path> subtaskDirectories) {
        Set<Path> inputsFiles = new HashSet<>();
        Set<DataFileType> inputDataFileTypes = pipelineTask.pipelineDefinitionNode()
            .getInputDataFileTypes();
        for (DataFileType fileType : inputDataFileTypes) {
            inputsFiles.addAll(filesInSubtaskDirsOfType(fileType, subtaskDirectories));
        }
        return inputsFiles;
    }

    /**
     * Returns the {@link Set} of file {@link Path}s for a given {@link DataFileType}, across a
     * collection of subtask directory Paths.
     */
    private Set<Path> filesInSubtaskDirsOfType(DataFileType dataFileType,
        Set<Path> subtaskDirectories) {
        Set<Path> filesInSubtaskDirsOfType = new HashSet<>();
        for (Path subtaskDirectory : subtaskDirectories) {
            filesInSubtaskDirsOfType
                .addAll(FileUtil.listFiles(subtaskDirectory, dataFileType.getFileNameRegexp()));
        }

        // Convert the files back to their datastore names so that we can use this information
        // to track the producer-consumer relationships for the files.
        Path datastorePath = datastoreWalker().pathFromLocationAndRegexpValues(
            DatastoreDirectoryUnitOfWorkGenerator.regexpValues(pipelineTask.uowTaskInstance()),
            dataFileType.getLocation());
        return filesInSubtaskDirsOfType.stream()
            .map(s -> datastorePath.resolve(s.getFileName()))
            .collect(Collectors.toSet());
    }

    boolean singleSubtask() {
        return pipelineTask.pipelineDefinitionNode().getSingleSubtask();
    }

    AlertService alertService() {
        return alertService;
    }

    DatastoreWalker datastoreWalker() {
        if (datastoreWalker == null) {
            datastoreWalker = DatastoreWalker.newInstance();
        }
        return datastoreWalker;
    }

    public Path taskDirectory() {
        return taskDirectory;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }

    DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
        return datastoreProducerConsumerCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    /**
     * Performs a hard-link or a copy of a file. Hard links can generally be created only from a
     * target file to another location on the same file system, but Java doesn't appear to give us
     * any way to determine the latter. Thus: we try to link, and if an exception occurs we execute
     * a copy operation.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public static void copyOrLink(Path src, Path dest) {
        try {
            FileUtil.CopyType.LINK.copy(src, dest);
        } catch (Exception unableToLinkException) {
            FileUtil.CopyType.COPY.copy(src, dest);
        }
    }

    /** Container class for files that either have outputs associated with them, or not. */
    public static class InputFiles {
        private final Set<Path> filesWithOutputs;
        private final Set<Path> filesWithoutOutputs;

        public InputFiles(Set<Path> filesWithOutputs, Set<Path> filesWithoutOutputs) {
            this.filesWithOutputs = filesWithOutputs;
            this.filesWithoutOutputs = filesWithoutOutputs;
        }

        public Set<Path> getFilesWithOutputs() {
            return filesWithOutputs;
        }

        public Set<Path> getFilesWithoutOutputs() {
            return filesWithoutOutputs;
        }
    }
}
