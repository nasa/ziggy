package gov.nasa.ziggy.data.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerOperations;
import gov.nasa.ziggy.module.AlgorithmStateFiles;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.SubtaskUtils;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

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
 * <p>
 * By default, the copies between the datastore and the task directories both attempt to generate
 * hard links and resort to actual copying if links cannot be constructed. The user can specify a
 * different behavior by using {@link #setDatastoreToTaskDirCopier(DatastoreCopier)} and
 * {@link #setTaskDirToDatastoreCopier(DatastoreCopier)}, and supplying implementations of the
 * {@link DatastoreCopier} interface as arguments to the setters.
 *
 * @author PT
 */

public class DatastoreFileManager {

    private static final Logger log = LoggerFactory.getLogger(DatastoreFileManager.class);

    private static final Predicate<? super File> WITH_OUTPUTS = AlgorithmStateFiles::hasOutputs;
    private static final Predicate<? super File> WITHOUT_OUTPUTS = WITH_OUTPUTS.negate();
    public static final String FILE_NAME_DELIMITER = "\\.";
    public static final String SINGLE_SUBTASK_BASE_NAME = "Single Subtask";
    private static final String SERIALIZED_REGEXP_VALUE_FILE_NAME = ".regexp-values.ser";

    private final PipelineTask pipelineTask;
    private AlertService alertService = new AlertService();
    private DatastoreWalker datastoreWalker;
    private final Path taskDirectory;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations = new DatastoreProducerConsumerOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private DatastoreCopier datastoreToTaskDirCopier = DatastoreFileManager::copyOrLink;
    private DatastoreCopier taskDirToDatastoreCopier = DatastoreFileManager::copyOrLink;
    private boolean singleSubtask;

    public DatastoreFileManager(PipelineTask pipelineTask, Path taskDirectory) {
        this.pipelineTask = pipelineTask;
        this.taskDirectory = taskDirectory;
    }

    /**
     * Constructs the collection of {@link SubtaskDefinition}s for each subtask.
     * <p>
     * All subtasks must have one data file from each file-per-subtask data file type. Any subtask
     * that is missing one or more files is omitted from the returned {@link Set}.
     */
    public Set<SubtaskDefinition> subtaskDefinitions() {
        return subtaskDefinitions(pipelineTaskOperations().pipelineDefinitionNode(pipelineTask));
    }

    public Set<SubtaskDefinition> subtaskDefinitions(
        PipelineDefinitionNode pipelineDefinitionNode) {

        singleSubtask = pipelineDefinitionNode.getSingleSubtask();
        // Obtain the data file types that the module requires
        Set<DataFileType> dataFileTypes = pipelineDefinitionNodeOperations()
            .inputDataFileTypes(pipelineDefinitionNode);
        // Construct a List of data file types that expect 1 file per subtask.
        List<DataFileType> filePerSubtaskDataFileTypes = dataFileTypes.stream()
            .filter(s -> !s.isIncludeAllFilesInAllSubtasks())
            .collect(Collectors.toList());
        // Construct a list of data file types for which all files need to be provided
        // to all subtasks.
        List<DataFileType> allFilesAllSubtasksDataFileTypes = new ArrayList<>(dataFileTypes);
        allFilesAllSubtasksDataFileTypes.removeAll(filePerSubtaskDataFileTypes);

        UnitOfWork uow = pipelineTask.getUnitOfWork();

        // Generate sets of DataFilesForDataFileType instances. These provide the necessary
        // information for mapping files in the datastore into the files needed by each
        // subtask.
        Set<DataFilesForDataFileType> filesForPerSubtaskDataType = dataFilesForDataFileTypes(uow,
            filePerSubtaskDataFileTypes);
        Set<DataFilesForDataFileType> filesForAllSubtasksDataType = dataFilesForDataFileTypes(uow,
            allFilesAllSubtasksDataFileTypes);

        // If the user wants new-data processing only, filter the data files to remove
        // any that were processed already by the pipeline module that's assigned to
        // this pipeline task.
        if (!singleSubtask && pipelineDefinitionOperations()
            .processingMode(pipelineDefinitionNode.getPipelineName())
            .equals(ProcessingMode.PROCESS_NEW)) {
            filterOutDataFilesAlreadyProcessed(filesForPerSubtaskDataType);
        }
        for (DataFilesForDataFileType dataFilesForDataFileType : filesForPerSubtaskDataType) {
            log.debug("Data file type {} file count {}",
                dataFilesForDataFileType.getDataFileType().getName(),
                dataFilesForDataFileType.allPaths().size());
        }

        // Generate subtask information using just the file-per-subtask data file types.
        Set<SubtaskDefinition> subtaskDefinitions = generateSubtaskDefinitions(
            filesForPerSubtaskDataType);
        // Add the all-files-all-subtasks paths to all the subtasks.
        Set<Path> allFilesAllSubtasks = new HashSet<>();
        for (DataFilesForDataFileType dataFilesForDataFileType : filesForAllSubtasksDataType) {
            allFilesAllSubtasks.addAll(dataFilesForDataFileType.allPaths());
        }
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            subtaskDefinition.addAll(allFilesAllSubtasks);
        }

        // if this task will use a single subtask, it's possible that it has
        // no input types that are in the one-file-per-subtask category. Handle
        // that corner case now.
        if (pipelineDefinitionNode.getSingleSubtask() && subtaskDefinitions.isEmpty()) {
            subtaskDefinitions = generateSubtaskDefinitions(filesForAllSubtasksDataType);
        }

        log.info("Number of subtasks {}", subtaskDefinitions.size());
        return subtaskDefinitions;
    }

    /**
     * Produces a {@link Map} from a given {@link DataFileType} to the data files for that type,
     * based on the unit of work.
     * <p>
     * In cases where the current unit of work includes subdirectories that must be searched for
     * input files, we need an additional layer of bookkeeping. Specifically, we need to track which
     * input files come from which subdirectory, so that later on we can use this information to
     * construct subtasks (i.e., we need all the inputs in a subtask to come from subdirs with the
     * same names as one another). Thus we use the {@link DataFilesInSubdir} to provide this
     * additional bookkeeping.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private Set<DataFilesForDataFileType> dataFilesForDataFileTypes(UnitOfWork uow,
        List<DataFileType> dataFileTypes) {
        Set<DataFilesForDataFileType> dataFilesForDataFileTypes = new HashSet<>();

        // Get the specific directories for this UOW (one per data file type).
        Map<String, String> pathsByDataTypeName = DirectoryUnitOfWorkGenerator
            .directoriesByDataFileType(uow);
        for (DataFileType dataFileType : dataFileTypes) {

            Path datastoreDirectory = Paths.get(pathsByDataTypeName.get(dataFileType.getName()));
            DataFilesForDataFileType dataFilesForDataFileType = new DataFilesForDataFileType(
                dataFileType, datastoreDirectory, datastoreWalker());

            // Determine all the paths under the datastore directory that match the data file type
            // full location.
            List<Path> datastoreDirectories = datastoreWalker().pathsForLocation(
                datastoreWalker().specificLocation(dataFileType, datastoreDirectory));
            for (Path datastoreSubdirectory : datastoreDirectories) {
                if (!Files.exists(datastoreDirectory) || !Files.isDirectory(datastoreDirectory)) {
                    continue;
                }
                dataFilesForDataFileType.addSublocation(datastoreSubdirectory);
                dataFilesForDataFileType.addPathsToSublocation(datastoreSubdirectory,
                    ZiggyFileUtils.listFiles(datastoreSubdirectory,
                        DatastoreWalker.fileNameRegexpBaseName(dataFileType)));
            }
            log.debug("Data file type {} file count {}", dataFileType.getName(),
                dataFilesForDataFileType.allPaths().size());
            dataFilesForDataFileTypes.add(dataFilesForDataFileType);
        }
        return dataFilesForDataFileTypes;
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
        Set<DataFilesForDataFileType> dataFilesForDataFileTypes) {

        for (DataFilesForDataFileType dataFilesForDataFileType : dataFilesForDataFileTypes) {
            for (Map.Entry<String, Set<Path>> entry : dataFilesForDataFileType
                .getDataFilesBySublocation()
                .entrySet()) {
                Set<Path> paths = entry.getValue();

                // The names in the producer-consumer table are relative to the datastore
                // root, while the values in pathsByPerSubtaskDataType are absolute.
                // Generate a relativized Set now.
                Set<String> relativizedFilePaths = paths.stream()
                    .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
                    .map(Path::toString)
                    .collect(Collectors.toSet());

                // Find the consumers that correspond to the definition node of the current task.
                List<PipelineTask> consumersWithMatchingPipelineNode = pipelineTaskOperations()
                    .tasksForPipelineDefinitionNode(pipelineTask);

                // Obtain the Set of datastore files that are in the relativizedFilePaths collection
                // and which have a consumer that matches the pipeline definition node of the
                // current pipeline task.
                Set<String> namesOfFilesAlreadyProcessed = datastoreProducerConsumerOperations()
                    .filesConsumedByTasks(consumersWithMatchingPipelineNode, relativizedFilePaths);

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
    }

    /**
     * Generates the portion of the {@link List} of {@link Set}s of {@link Paths} for each subtask
     * that comes from file-per-subtask data types. Returns a {@link Map} from the data file base
     * name (i.e., everything before the first "." in its name) to all the data files that have that
     * base name. Each Map entry's value are the set of input files needed for a given subtask.
     */
    private Set<SubtaskDefinition> generateSubtaskDefinitions(
        Set<DataFilesForDataFileType> pathsByPerSubtaskDataType) {

        // Generate the mapping from regexp group values SubtaskDefinition instances.
        Map<String, SubtaskDefinition> subtaskDefinitionByRegexpGroupValuesHash = new HashMap<>();
        for (DataFilesForDataFileType dataFilesForDataFileType : pathsByPerSubtaskDataType) {
            addPathsToSubtaskDefinitions(dataFilesForDataFileType,
                subtaskDefinitionByRegexpGroupValuesHash);
        }
        log.debug("Files for subtask entries {}", subtaskDefinitionByRegexpGroupValuesHash.size());

        // Check for cases that have insufficient files. These are cases in which one or more
        // data file type has no file for the given subtasks, which means that these are subtasks
        // that cannot run. Note that the logic of regular expressions guarantees that each data
        // file type can produce no more than one file that matches a given data file type regexp.
        int subtaskCount = subtaskDefinitionByRegexpGroupValuesHash.size();
        int dataFileTypeCount = pathsByPerSubtaskDataType.size();
        Set<String> regexpGroupValuesForInvalidSubtasks = new HashSet<>();
        for (Map.Entry<String, SubtaskDefinition> subtaskMapEntry : subtaskDefinitionByRegexpGroupValuesHash
            .entrySet()) {
            if (subtaskMapEntry.getValue().getSubtaskFiles().size() < dataFileTypeCount) {
                regexpGroupValuesForInvalidSubtasks.add(subtaskMapEntry.getKey());
            }
        }
        if (!regexpGroupValuesForInvalidSubtasks.isEmpty()) {
            log.warn("{} subtasks out of {} missing files and will not be processed",
                regexpGroupValuesForInvalidSubtasks.size(), subtaskCount);
            for (String regexpGroupValuesForInvalidSubtask : regexpGroupValuesForInvalidSubtasks) {
                subtaskDefinitionByRegexpGroupValuesHash.remove(regexpGroupValuesForInvalidSubtask);
            }
        }
        return new HashSet<>(subtaskDefinitionByRegexpGroupValuesHash.values());
    }

    /**
     * Adds the {@link Path}s for a given {@link DataFileType} to the overall {@link Map} of paths
     * by concatenated regexp group values. Each entry in the map represents a subtask in which all
     * of the data files for the subtask have matching values for their regexp groups.
     */
    private void addPathsToSubtaskDefinitions(DataFilesForDataFileType dataFilesForDataFileType,
        Map<String, SubtaskDefinition> subtaskDefinitionByRegexpGroupValuesHash) {
        for (Map.Entry<String, Set<Path>> entry : dataFilesForDataFileType
            .getDataFilesBySublocation()
            .entrySet()) {
            for (Path path : entry.getValue()) {
                String regexpGroupValuesHash = singleSubtask ? SINGLE_SUBTASK_BASE_NAME
                    : regexpGroupValuesHash(entry.getKey(),
                        dataFilesForDataFileType.getFileNameRegexpPattern(), path);
                if (!subtaskDefinitionByRegexpGroupValuesHash.containsKey(regexpGroupValuesHash)) {
                    subtaskDefinitionByRegexpGroupValuesHash
                        .put(regexpGroupValuesHash,
                            SubtaskDefinition.of(
                                datastoreWalker(), regexpGroupValuesHash, DatastoreWalker
                                    .fullLocation(dataFilesForDataFileType.getDataFileType()),
                                path));
                }
                subtaskDefinitionByRegexpGroupValuesHash.get(regexpGroupValuesHash).add(path);
            }
        }
    }

    /**
     * Applies a {@link Pattern} to the file name element of a {@link Path}, and returns the hash of
     * the values of the regexp groups. The hash is formed by combining the values of the regexp
     * groups. For example, if the pattern is "(\\S+)-bauhaus-(\\S+).nc" and the file name is
     * "foo-bauhaus-baz.nc", then this method returns "foobaz".
     */
    private static String regexpGroupValuesHash(String sublocation, Pattern dataFileTypePattern,
        Path file) {
        log.debug("File type pattern {}, file {}", dataFileTypePattern.pattern(), file.toString());
        Matcher matcher = dataFileTypePattern.matcher(file.getFileName().toString());
        if (!matcher.matches()) {
            log.warn("File {} does not match regexp {}", file.getFileName().toString(),
                dataFileTypePattern.pattern());
            return null;
        }
        StringBuilder regexpGroupValuesHash = new StringBuilder(sublocation);
        for (int groupIndex = 1; groupIndex <= matcher.groupCount(); groupIndex++) {
            regexpGroupValuesHash.append(matcher.group(groupIndex));
        }
        return regexpGroupValuesHash.toString();
    }

    /**
     * Returns the model files for the task. The return is in the form of a {@link Map} in which the
     * datastore paths of the current models are the keys and the names of the files in the task
     * directory are the values.
     */
    public Map<Path, String> modelTaskFilesByDatastorePath() {
        Map<Path, String> modelTaskFilesByDatastorePath = new HashMap<>();

        // Get the model registry and the model types from the pipeline task.
        ModelRegistry modelRegistry = pipelineTaskOperations().modelRegistry(pipelineTask);
        Set<ModelType> modelTypes = pipelineTaskOperations().modelTypes(pipelineTask);

        // Put the model location in the datastore, and its original file name, into the Map.
        for (ModelType modelType : modelTypes) {
            ModelMetadata metadata = modelRegistry.getModels().get(modelType);
            modelTaskFilesByDatastorePath.put(metadata.datastoreModelPath(),
                metadata.getOriginalFileName());
        }
        return modelTaskFilesByDatastorePath;
    }

    /**
     * Copies datastore files to the subtask directories. Both data files and models are copied. Any
     * datastore regexp values for the given subtask are also copied to the subtask directory.
     */
    public Map<Path, Set<Path>> copyDatastoreFilesToTaskDirectory(
        Set<SubtaskDefinition> subtaskDefinitions, Map<Path, String> modelFilesForTask) {

        Map<Path, Set<Path>> pathsBySubtaskDirectory = new HashMap<>();

        // Loop over subtasks.
        int subtaskIndex = 0;
        int loggingIndex = Math.max(1, subtaskDefinitions.size() / 20);
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            Path subtaskDirectory = SubtaskUtils.createSubtaskDirectory(taskDirectory(),
                subtaskIndex);

            // Copy or link the data files.
            for (Path file : subtaskDefinition.getSubtaskFiles()) {
                Path destination = subtaskDirectory.resolve(file.getFileName());
                datastoreToTaskDirCopier.copy(file, destination);
            }

            // Put the regexp map into the subtask directory.
            copyDatastoreRegexpValuesToSubtaskDir(subtaskDirectory,
                subtaskDefinition.regexpGroupValuesByRegexpName);

            if (modelFilesForTask == null) {
                continue;
            }

            // Copy or link the models.
            for (Map.Entry<Path, String> modelEntry : modelFilesForTask.entrySet()) {
                Path destination = subtaskDirectory.resolve(modelEntry.getValue());
                copyOrLink(modelEntry.getKey(), destination);
            }
            if (subtaskIndex++ % loggingIndex == 0) {
                log.info("Subtask {} of {} generated", subtaskIndex, subtaskDefinitions.size());
            }
            pathsBySubtaskDirectory.put(subtaskDirectory, subtaskDefinition.getSubtaskFiles());
        }
        log.info("Generating subtasks...done");
        return pathsBySubtaskDirectory;
    }

    // Package scoped for unit testing.
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    void copyDatastoreRegexpValuesToSubtaskDir(Path subtaskDirectory,
        Map<String, String> regexpValuesByName) {
        if (regexpValuesByName == null) {
            return;
        }
        if (regexpValuesByName.isEmpty()) {
        }
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(
            subtaskDirectory.resolve(SERIALIZED_REGEXP_VALUE_FILE_NAME).toString()))) {
            outputStream.writeObject(regexpValuesByName);
        } catch (FileNotFoundException e) {
            throw new PipelineException("FileNotFound exception occurred", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Copies output files from the task directory to the datastore, returning the Set of datastore
     * Paths that result from the copy operations.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Set<Path> copyTaskDirectoryFilesToDatastore() {

        log.info("Copying output files to datastore...");
        Set<DataFileType> outputDataFileTypes = pipelineTaskOperations()
            .outputDataFileTypes(pipelineTask);

        // Construct a Map from data file type to the Pattern for its regexp.
        Map<DataFileType, Pattern> regexpPatternsByDataFileType = new HashMap<>();
        for (DataFileType dataFileType : outputDataFileTypes) {
            regexpPatternsByDataFileType.put(dataFileType,
                Pattern.compile(DatastoreWalker.fileNameRegexpBaseName(dataFileType)));
        }

        // Generate the paths of all subtask directories.
        Set<Path> subtaskDirs = ZiggyFileUtils.listFiles(taskDirectory(),
            Set.of(SubtaskUtils.SUBTASK_DIR_PATTERN), null);

        Set<Path> outputFiles = new HashSet<>();

        // Prepare a Map from datastore directories to files that go into that directory.
        Map<Path, Set<Path>> outputFilesByDestinationDir = new HashMap<>();

        // Populate the outputFilesByDestinationDir map.
        for (Path subtaskDir : subtaskDirs) {
            Map<String, String> datastoreRegexpValues = copyDatastoreRegexpValuesFromSubtaskDir(
                subtaskDir);
            for (DataFileType dataFileType : outputDataFileTypes) {
                Path destinationDirectory = datastoreWalker().pathFromLocationAndRegexpValues(
                    datastoreRegexpValues, DatastoreWalker.fullLocation(dataFileType));
                Set<Path> subtaskOutputFiles = ZiggyFileUtils.listFiles(subtaskDir,
                    Set.of(regexpPatternsByDataFileType.get(dataFileType)), null);
                if (outputFilesByDestinationDir.get(destinationDirectory) == null) {
                    outputFilesByDestinationDir.put(destinationDirectory, new HashSet<>());
                }
                outputFilesByDestinationDir.get(destinationDirectory).addAll(subtaskOutputFiles);
            }
        }

        // Use the outputFilesByDestinationDir to copy files to their datastore locations.
        // Because the files are organized by destination directory, we only need to change
        // the write protect on each directory twice.
        for (Map.Entry<Path, Set<Path>> entry : outputFilesByDestinationDir.entrySet()) {
            try {
                Files.createDirectories(entry.getKey());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            ZiggyFileUtils.prepareDirectoryTreeForOverwrites(entry.getKey());
            for (Path outputFile : entry.getValue()) {
                Path destinationFile = entry.getKey().resolve(outputFile.getFileName());
                taskDirToDatastoreCopier.copy(outputFile, destinationFile);
                outputFiles.add(destinationFile);
            }
            ZiggyFileUtils.writeProtectDirectoryTree(entry.getKey());
        }

        log.info("Copying output files to datastore...done");
        return outputFiles;
    }

    /** Returns the number of subtasks for a given task. */
    public int subtaskCount() {
        return subtaskDefinitions().size();
    }

    public int subtaskCount(PipelineDefinitionNode pipelineDefinitionNode) {
        return subtaskDefinitions(pipelineDefinitionNode).size();
    }

    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    Map<String, String> copyDatastoreRegexpValuesFromSubtaskDir(Path subtaskDir) {
        if (!Files.exists(subtaskDir.resolve(SERIALIZED_REGEXP_VALUE_FILE_NAME))) {
            return new HashMap<>();
        }
        try (ObjectInputStream objectInputStream = new ObjectInputStream(
            new FileInputStream(subtaskDir.resolve(SERIALIZED_REGEXP_VALUE_FILE_NAME).toFile()))) {
            return (Map<String, String>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Class not found exception", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        Set<DataFileType> inputDataFileTypes = pipelineTaskOperations()
            .inputDataFileTypes(pipelineTask);
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
            Set<Path> filesInCurrentSubtask = ZiggyFileUtils.listFiles(subtaskDirectory,
                DatastoreWalker.fileNameRegexpBaseName(dataFileType));
            Map<String, String> regexpValues = copyDatastoreRegexpValuesFromSubtaskDir(
                subtaskDirectory);
            Path datastorePath = datastoreWalker().pathFromLocationAndRegexpValues(regexpValues,
                DatastoreWalker.fullLocation(dataFileType));
            filesInSubtaskDirsOfType.addAll(filesInCurrentSubtask.stream()
                .map(s -> datastorePath.resolve(s.getFileName()))
                .collect(Collectors.toSet()));
        }
        return filesInSubtaskDirsOfType;
    }

    public void setDatastoreToTaskDirCopier(DatastoreCopier copier) {
        if (copier != null) {
            datastoreToTaskDirCopier = copier;
        }
    }

    public void setTaskDirToDatastoreCopier(DatastoreCopier copier) {
        if (copier != null) {
            taskDirToDatastoreCopier = copier;
        }
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

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }

    DatastoreProducerConsumerOperations datastoreProducerConsumerOperations() {
        return datastoreProducerConsumerOperations;
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
            ZiggyFileUtils.CopyType.LINK.copy(src, dest);
        } catch (Exception unableToLinkException) {
            ZiggyFileUtils.CopyType.COPY.copy(src, dest);
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

    /**
     * Container class for subtask information.
     * <p>
     * The {@link SubtaskDefinition} class captures all the files that correspond to a given
     * subtask. The files of different types that are processed in a subtask must all have the same
     * values for the regexp groups in their full locations. For example if one data file type has a
     * full location of "foo/(bar|baz)/(duran|sisters)", and another has a full location of
     * "bauhaus/(bar|baz)/(duran|sisters)", one subtask will contain files from foo/bar/duran and
     * bauhaus/bar/duran; another from foo/bar/sisters and bauhaus/bar/sisters; another from
     * foo/baz/duran and bauhaus/baz/duran; and the final subtask will have files from
     * foo/baz/sisters and bauhaus/baz/sisters.
     * <p>
     * The {@link SubtaskDefinition} class thus contains the following fields:
     * <ol>
     * <li>{@link SubtaskDefinition#regexpGroupValuesByRegexpName}, which is the Map from the name
     * of a regexp to the value of the corresponding group. All files in a subtask will have the
     * regexp values in the regexpGroupValuesByRegexpName map.
     * <li>{@link SubtaskDefinition#regexpGroupValuesHash}, which is a hash of the regexp group
     * values for this subtask. In this case, the hash is generated by combining the strings. Thus
     * if the location for one data file type is "foo/(bar|baz)/(duran|sisters)", and the location
     * for another is "bauhaus/(bar|baz)/(duran|sisters)", the regexpValuesHash will be one of
     * "barduran", "barsisters", "bazduran", bazsisters".
     * <li>{@link SubtaskDefinition#subtaskFiles}, the actual files that will be processed in this
     * subtask.
     * </ol>
     *
     * @author PT
     */
    public static class SubtaskDefinition {

        private final String regexpGroupValuesHash;
        private final Map<String, String> regexpGroupValuesByRegexpName;
        private final Set<Path> subtaskFiles = new HashSet<>();

        public SubtaskDefinition(String regexpGroupValuesHash,
            Map<String, String> regexpGroupValuesByRegexpName) {
            this.regexpGroupValuesHash = regexpGroupValuesHash;
            this.regexpGroupValuesByRegexpName = regexpGroupValuesByRegexpName;
        }

        public static SubtaskDefinition of(DatastoreWalker datastoreWalker,
            String regexpGroupValuesHash, String location, Path path) {
            Map<String, String> regexpValuesByRegexpName = datastoreWalker
                .regexpValuesByRegexpName(location, path, false);
            return new SubtaskDefinition(regexpGroupValuesHash, regexpValuesByRegexpName);
        }

        public Map.Entry<String, Set<Path>> mapEntry() {
            return new AbstractMap.SimpleEntry<>(regexpGroupValuesHash, subtaskFiles);
        }

        public String getRegexpValuesHash() {
            return regexpGroupValuesHash;
        }

        public Map<String, String> regexpGroupValueByRegexpName() {
            return regexpGroupValuesByRegexpName;
        }

        public Set<Path> getSubtaskFiles() {
            return subtaskFiles;
        }

        public void add(Path path) {
            subtaskFiles.add(path);
        }

        public void addAll(Collection<Path> paths) {
            subtaskFiles.addAll(paths);
        }

        @Override
        public int hashCode() {
            return Objects.hash(regexpGroupValuesHash);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SubtaskDefinition other = (SubtaskDefinition) obj;
            return Objects.equals(regexpGroupValuesHash, other.regexpGroupValuesHash);
        }
    }

    /**
     * Container that organizes data files for a given data file type.
     * <p>
     * In order to support the use-case in which the {@link Path} for a given {@link DataFileType}
     * has subdirectories that need to be searched for data files, it is necessary to provide a file
     * accounting system that keeps track of which files were found in which subdirectory. This is
     * accomplished by concatenating the path name elements of each subdirectory into a
     * {@link String}, then establishing a {@link Map} between those Strings and the {@link Set} of
     * {@link Path}s that represent individual data files in the specified directory.
     * <p>
     * The concatenated String of the subdirectory is known here as a sublocation for want of a
     * better name.
     *
     * @author PT
     */
    private static class DataFilesForDataFileType {

        private final DataFileType dataFileType;
        private final Pattern fileNameRegexpPattern;
        private final Path dataFileTypeParentDir;
        private final Set<Integer> namePathsForSublocation;
        private final Map<Path, String> sublocationByPath = new HashMap<>();
        private final Map<String, Set<Path>> dataFilesBySublocation = new HashMap<>();

        public DataFilesForDataFileType(DataFileType dataFileType, Path dataFileTypeParentDir,
            DatastoreWalker datastoreWalker) {
            this.dataFileType = dataFileType;
            this.dataFileTypeParentDir = dataFileTypeParentDir;
            fileNameRegexpPattern = Pattern
                .compile(DatastoreWalker.fileNameRegexpBaseName(dataFileType));
            namePathsForSublocation = datastoreWalker
                .findLocationIndicesForSublocation(dataFileType);
        }

        public DataFileType getDataFileType() {
            return dataFileType;
        }

        public Map<String, Set<Path>> getDataFilesBySublocation() {
            return dataFilesBySublocation;
        }

        public Pattern getFileNameRegexpPattern() {
            return fileNameRegexpPattern;
        }

        /** Converts a {@link Path} into a sublocation. */
        public void addSublocation(Path path) {
            Path sublocationRelativeDirectory = dataFileTypeParentDir.toAbsolutePath()
                .toAbsolutePath()
                .relativize(path.toAbsolutePath());
            StringBuilder sublocation = new StringBuilder();
            for (int pathNameIndex : namePathsForSublocation) {
                sublocation.append(sublocationRelativeDirectory.getName(pathNameIndex));
            }
            sublocationByPath.put(path, sublocation.toString());
            dataFilesBySublocation.put(sublocation.toString(), new HashSet<>());
        }

        public void addPathsToSublocation(Path subdir, Set<Path> paths) {
            dataFilesBySublocation.get(sublocationByPath.get(subdir)).addAll(paths);
        }

        public Set<Path> allPaths() {
            Set<Path> allPaths = new HashSet<>();
            for (Map.Entry<String, Set<Path>> entry : dataFilesBySublocation.entrySet()) {
                allPaths.addAll(entry.getValue());
            }
            return allPaths;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataFileType);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DataFilesForDataFileType other = (DataFilesForDataFileType) obj;
            return Objects.equals(dataFileType, other.dataFileType);
        }
    }
}
