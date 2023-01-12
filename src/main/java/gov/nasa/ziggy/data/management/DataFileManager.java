package gov.nasa.ziggy.data.management;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;

import gov.nasa.ziggy.data.management.DataFileType.RegexType;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.AlgorithmStateFiles;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.TaskConfigurationManager;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.uow.TaskConfigurationParameters;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Provides functions that identify data files based on the subclass of DataFileInfo they correspond
 * to, and tools to copy such files between the datastore and a task directory; to move files from a
 * task directory to the datastore; and to delete unneeded data files from a task directory.
 * <p>
 * The class can be used in one of two ways.
 * <p>
 * The approach that involves less code development is to use DataFileType instances to define the
 * names and datastore locations of various types of data file. This allows all of the DataFileType
 * information to be specified in XML files that configure the pipeline. The main disadvantage of
 * this approach is that it is less flexible in terms of defining the organization of the datastore,
 * optionally moving just some files in a given data file type (rather than all of them), etc.
 * <p>
 * In the event that a greater degree of flexibility is desired, the DataFileManager can use
 * DataFileInfo classes and a DatastoreFileLocator instance to manage the file names, paths to the
 * datastore, etc. This requires additional code in the form of the DataFileInfo classes and the
 * DatastoreFileLocator instance.
 *
 * @author PT
 */
public class DataFileManager {

    private static final Predicate<? super File> WITH_RESULTS = AlgorithmStateFiles::hasResults;
    private static final Predicate<? super File> WITHOUT_RESULTS = WITH_RESULTS.negate();

    private DatastorePathLocator datastorePathLocator;
    private PipelineTask pipelineTask;
    private TaskConfigurationParameters taskConfigurationParameters;
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud;
    private PipelineTaskCrud pipelineTaskCrud;
    private Path taskDirectory;
    private Path datastoreRoot = DirectoryProperties.datastoreRootDir();
    private DatastoreCopyType taskDirCopyType;
    private DatastoreCopyType datastoreCopyType = DatastoreCopyType.MOVE;

    // =========================================================================
    //
    // Constructors
    //
    // =========================================================================

    public Path getDatastoreRoot() {
        return datastoreRoot;
    }

    /**
     * No-arg constructor. Used in the PipelineInputs and PipelineOutputs classes.
     */
    public DataFileManager() {
    }

    /**
     * Constructor with PipelineTask and DatastorePathLocator arguments. Used in pipeline modules
     * that use the DataFileInfo and DatastoreFileLocator classes to identify and manage files that
     * need to be moved between the task directory and the datastore.
     *
     * @param datastorePathLocator instance of a DatastorePathLocator subclass that is sufficient to
     * provide datastore paths for all subclasses of DataFileInfo used by the pipeline module that
     * instantiates the DataFileManager instance.
     * @param pipelineTask PipelineTask supported by this instance.
     * @param taskDirectory Path to the task directory.
     */
    public DataFileManager(DatastorePathLocator datastorePathLocator, PipelineTask pipelineTask,
        Path taskDirectory) {
        this.pipelineTask = pipelineTask;
        this.datastorePathLocator = datastorePathLocator;
        this.taskDirectory = taskDirectory;
        if (pipelineTask != null) {
            taskConfigurationParameters = pipelineTask
                .getParameters(TaskConfigurationParameters.class, false);
        }
        taskDirCopyType = taskDirCopyType();
    }

    /**
     * Constructor with PipelineTask and Paths to the task directory and the datastore root. Used in
     * pipeline modules that use the DefaultUnitOfWork and DataFileType instances to identify and
     * manage files that need to be moved between the task directory and the datastore.
     */
    public DataFileManager(Path datastoreRoot, Path taskDirectory, PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
        this.taskDirectory = taskDirectory;
        if (datastoreRoot != null) {
            this.datastoreRoot = datastoreRoot;
        }
        if (pipelineTask != null) {
            taskConfigurationParameters = pipelineTask
                .getParameters(TaskConfigurationParameters.class, false);
        }
        datastorePathLocator = null;
        taskDirCopyType = taskDirCopyType();
    }

    private DatastoreCopyType taskDirCopyType() {
        DatastoreCopyType copyType = null;
        boolean useSymlinks = ZiggyConfiguration.getInstance()
            .getBoolean(PropertyNames.USE_SYMLINKS_PROP_NAME, false);
        if (useSymlinks) {
            copyType = DatastoreCopyType.SYMLINK;
        } else {
            copyType = DatastoreCopyType.COPY;
        }
        return copyType;
    }

    // =========================================================================
    //
    // Public methods for use with DataFileType instances
    //
    // =========================================================================

    /**
     * Obtains a Map from DataFileType instances to files of each type in the task directory.
     *
     * @param dataFileTypes Set of DataFileType instances to be matched
     */
    public Map<DataFileType, Set<Path>> taskDirectoryDataFilesMap(Set<DataFileType> dataFileTypes) {

        return dataFilesMap(taskDirectory, dataFileTypes, RegexType.TASK_DIR);
    }

    /**
     * Obtains a Map from DataFileType instances to files of each type in a sub-directory of the
     * datastore.
     *
     * @param datastoreSubDir subdirectory of the datastore to be searched.
     * @param dataFileTypes Set of DataFileType instances to be matched.
     */
    public Map<DataFileType, Set<Path>> datastoreDataFilesMap(Path datastoreSubDir,
        Set<DataFileType> dataFileTypes) {
        return dataFilesMap(datastoreRoot.resolve(datastoreSubDir), dataFileTypes,
            RegexType.DATASTORE);
    }

    /**
     * Determines the number of files of a given type in a given subdirectory of the datastore. Used
     * for counting subtasks.
     */
    public int countDatastoreFilesOfType(DataFileType type, Path datastoreSubDir) {

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(type);
        Map<DataFileType, Set<Path>> datastoreFiles = datastoreDataFilesMap(datastoreSubDir,
            dataFileTypes);
        return datastoreFiles.get(type).size();
    }

    /**
     * Copies data files from the datastore to the task directory.
     *
     * @param datastoreSubDir subdirectory of datastore to use as the file source.
     * @param dataFileTypes Set of DataFileType instances to use in the copy.
     */
    public Map<DataFileType, Set<Path>> copyDataFilesByTypeToTaskDirectory(Path datastoreSubDir,
        Set<DataFileType> dataFileTypes) {
        return copyDataFilesByTypeToTaskDirectory(
            datastoreDataFilesMap(datastoreSubDir, dataFileTypes));
    }

    /**
     * Copies data files from the datastore to the task directory.
     *
     * @param datastoreDataFilesMap files to be copied, in the form of a {@link Map} that uses
     * {@link DataFileType} as its key and a {@link Set} of data file {@link Path} instances as the
     * map values.
     * @param taskConfig {@link TaskConfigurationParameters} instance.
     */
    public Map<DataFileType, Set<Path>> copyDataFilesByTypeToTaskDirectory(
        Map<DataFileType, Set<Path>> datastoreDataFilesMap) {

        Map<DataFileType, Set<Path>> datastoreFilesMap = copyDataFilesByTypeToDestination(
            datastoreDataFilesMap, RegexType.TASK_DIR, taskDirCopyType);
        Set<Path> datastoreFiles = new HashSet<>();
        for (Set<Path> paths : datastoreFilesMap.values()) {
            datastoreFiles.addAll(paths);
        }
        // obtain the originators for all datastore files and add them as producers to the
        // current pipeline task; also delete any existing ones so that in the event of a
        // reprocess the correct information is reflected.
        Set<Long> producerTaskIds = datastoreProducerConsumerCrud()
            .retrieveProducers(datastoreFiles);
        pipelineTask.clearProducerTaskIds();
        pipelineTask.setProducerTaskIds(producerTaskIds);

        return datastoreFilesMap;
    }

    /**
     * Identifies the files in the datastore that will be used as inputs for the current task.
     *
     * @param datastoreSubDir subdirectory of datastore to use as the file source
     * @param dataFileTypes set of DataFileType instances to use for the search
     * @param taskConfig {@link TaskConfigurationManager} instance that indicates whether full
     * processing or "keep ahead" processing is performed
     * @return non @code{null} set of {@link Path} instances for data files to be used as input
     */
    public Set<Path> dataFilesForInputs(Path datastoreSubDir, Set<DataFileType> dataFileTypes) {
        Map<DataFileType, Set<Path>> datastoreFilesMap = datastoreDataFilesMap(datastoreSubDir,
            dataFileTypes);
        Set<Path> datastoreFiles = new HashSet<>();
        for (Set<Path> paths : datastoreFilesMap.values()) {
            datastoreFiles.addAll(paths);
        }
        return datastoreFiles;
    }

    /**
     * Copies data files from the working directory to the task directory. This uses the copy type
     * that is appropriate for the task directory, so it can either copy files or produce symlinks
     * of files.
     */
    public void copyDataFilesByTypeFromWorkingDirToTaskDir(Set<DataFileType> dataFileTypes) {
        Path workingDirectory = DirectoryProperties.workingDir();
        Map<DataFileType, Set<Path>> sourceDataFiles = dataFilesMap(workingDirectory, dataFileTypes,
            RegexType.TASK_DIR);
        for (DataFileType dataFileType : sourceDataFiles.keySet()) {
            for (Path sourceFile : sourceDataFiles.get(dataFileType)) {
                taskDirCopyType.copy(workingDirectory.resolve(sourceFile),
                    taskDirectory.resolve(sourceFile));
            }
        }
    }

    /**
     * Determines whether the working directory has any files of the specified data types.
     */
    public boolean workingDirHasFilesOfTypes(Set<DataFileType> dataFileTypes) {
        Path workingDirectory = DirectoryProperties.workingDir();
        Map<DataFileType, Set<Path>> sourceDataFiles = dataFilesMap(workingDirectory, dataFileTypes,
            RegexType.TASK_DIR);
        return sourceDataFiles.values().stream().anyMatch(s -> !s.isEmpty());
    }

    /**
     * Copies model files from the datastore to the task directory using the selected copy mode
     * (true copies or symlinks). Returns the names of all files copied to the task directory.
     */
    public List<String> copyModelFilesToTaskDirectory(ModelRegistry modelRegistry,
        Set<ModelType> modelTypes, Logger log) {
        List<String> modelFilesCopied = new ArrayList<>();
        Map<ModelType, ModelMetadata> models = modelRegistry.getModels();
        for (ModelType modelType : modelTypes) {
            ModelMetadata modelMetadata = models.get(modelType);
            if (modelMetadata == null) {
                throw new PipelineException(
                    "Model " + modelType.getType() + " has no metadata entry");
            }
            if (modelMetadata.getDatastoreFileName() == null) {
                throw new PipelineException(
                    "Model " + modelType.getType() + " has no datastore filename");
            }
            Path datastoreModelFile = datastoreRoot
                .resolve(Paths.get(ModelImporter.DATASTORE_MODELS_SUBDIR_NAME, modelType.getType(),
                    modelMetadata.getDatastoreFileName()));
            Path taskDirectoryModelFile = taskDirectory
                .resolve(modelMetadata.getOriginalFileName());
            log.info("Copying file " + datastoreModelFile.getFileName().toString()
                + " to task directory");
            taskDirCopyType.copy(datastoreModelFile, taskDirectoryModelFile);
            modelFilesCopied.add(taskDirectoryModelFile.getFileName().toString());
        }
        return modelFilesCopied;
    }

    /**
     * Copies files by name from the task directory to the working directory. This uses the copy
     * type that is appropriate for the task directory, so it can either copy files or produce
     * symlinks of files.
     */
    public void copyFilesByNameFromTaskDirToWorkingDir(Collection<String> filenames) {
        Path workingDirectory = DirectoryProperties.workingDir();
        for (String filename : filenames) {
            taskDirCopyType.copy(taskDirectory.resolve(filename),
                workingDirectory.resolve(filename));
        }
    }

    /**
     * Deletes data files from the task directory given a Set of DataFileType instances. All data
     * files that belong to the specified types will be deleted.
     *
     * @param dataFileTypes Set of DataFileType instances to use in deletion.
     */
    public void deleteDataFilesByTypeFromTaskDirectory(Set<DataFileType> dataFileTypes) {

        Map<DataFileType, Set<Path>> dataFileTypesMap = taskDirectoryDataFilesMap(dataFileTypes);
        for (DataFileType dataFileType : dataFileTypesMap.keySet()) {
            for (Path dataFilePath : dataFileTypesMap.get(dataFileType)) {
                Path fullPath = taskDirectory.resolve(dataFilePath);
                try {
                    if (Files.isSymbolicLink(fullPath)) {
                        Files.delete(fullPath);
                    } else if (Files.isRegularFile(fullPath)) {
                        Files.deleteIfExists(fullPath);
                    } else {
                        FileUtils.deleteDirectory(fullPath.toFile());
                    }
                } catch (IOException e) {
                    throw new PipelineException(
                        "Unable to delete file " + fullPath.getFileName().toString()
                            + " from task directory " + taskDirectory.toString(),
                        e);
                }
            }
        }
    }

    /**
     * Moves data files from the task directory to the datastore given a set of DataFileType
     * instances. All data files that belong to the specified types will be moved.
     *
     * @param dataFileTypes Set of DataFileType instances to use in the move.
     */
    public void moveDataFilesByTypeToDatastore(Set<DataFileType> dataFileTypes) {
        Map<DataFileType, Set<Path>> datastoreFilesMap = copyDataFilesByTypeToDestination(
            taskDirectoryDataFilesMap(dataFileTypes), RegexType.DATASTORE, datastoreCopyType);

        Set<Path> datastoreFiles = new HashSet<>();
        for (Set<Path> paths : datastoreFilesMap.values()) {
            datastoreFiles.addAll(paths);
        }
        // Record the originator in the data accountability table in the database
        datastoreProducerConsumerCrud().createOrUpdateProducer(pipelineTask, datastoreFiles,
            DatastoreProducerConsumer.DataReceiptFileType.DATA);
    }

    private Set<String> datastoreFilesInCompletedSubtasks(Set<DataFileType> dataFileTypes,
        List<Path> subtaskDirectories) {

        // Get the data files of the assorted types from all the successful subtask directories
        Map<DataFileType, Set<Path>> dataFilesMap = dataFilesMap(subtaskDirectories, dataFileTypes,
            RegexType.TASK_DIR);

        // Convert the file names from task directory format to datastore format
        Set<String> inputFilesDatastoreFormatted = new HashSet<>();
        for (Map.Entry<DataFileType, Set<Path>> entry : dataFilesMap.entrySet()) {
            DataFileType type = entry.getKey();
            inputFilesDatastoreFormatted.addAll(entry.getValue()
                .stream()
                .map(s -> s.getFileName().toString())
                .map(s -> type.datastoreFileNameFromTaskDirFileName(s))
                .collect(Collectors.toSet()));
        }
        return inputFilesDatastoreFormatted;
    }

    /**
     * Identifies the completed subtasks within a task that produced results, and gets the names of
     * all files used by those subtasks based on a set of {@link DataFileType} instances. The file
     * names are returned.
     */
    public Set<String> datastoreFilesInCompletedSubtasksWithResults(
        Set<DataFileType> dataFileTypes) {
        return datastoreFilesInCompletedSubtasks(dataFileTypes,
            completedSubtaskDirectoriesWithResults());
    }

    /**
     * Identifies the completed subtasks within a task that failed to produce results, and gets the
     * names of all files used by those subtasks based on a set of {@link DataFileType} instances.
     * The file names are returned.
     */
    public Set<String> datastoreFilesInCompletedSubtasksWithoutResults(
        Set<DataFileType> dataFileTypes) {
        return datastoreFilesInCompletedSubtasks(dataFileTypes,
            completedSubtaskDirectoriesWithoutResults());
    }

    // =========================================================================
    //
    // Public methods for use with DataFileInfo classes
    //
    // =========================================================================

    /**
     * Identifies all the files in a given directory that belong to any of a set of DataFileInfo
     * subclasses and them as a single Set of DataFileInfo subclass instances.
     *
     * @param dataFileInfoClasses Set of DataFileInfo classes that are to be matched.
     * @param dir Path to directory that will be searched.
     * @return Set of DataFileInfo subclass instances that correspond to all the files in the
     * specified directory that can be matched by any of the DataFileInfo subclasses.
     */
    public Set<DataFileInfo> datastoreFiles(Path dir,
        Set<Class<? extends DataFileInfo>> dataFileInfoClasses) {
        Set<DataFileInfo> dataFiles = new TreeSet<>();
        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> dataFilesMap = dataFilesMap(
            dir, dataFileInfoClasses);
        for (Set<? extends DataFileInfo> s : dataFilesMap.values()) {
            dataFiles.addAll(s);
        }
        return dataFiles;
    }

    /**
     * Obtains a map from DataFileInfo subclasses to objects in each of the subclasses, where the
     * objects are generated from the files in a specified directory.
     */
    public Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> dataFilesMap(Path dir,
        Set<Class<? extends DataFileInfo>> dataFileInfoClasses) {
        return dataFilesMap(new ArrayList<>(Arrays.asList(dir)), dataFileInfoClasses);
    }

    /**
     * Obtains a map from DataFileInfo subclasses to objects in each of the subclasses, where the
     * objects are generated from the files in a specified directory. The search for the objects
     * runs through a collection of directories.
     */
    public Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> dataFilesMap(
        Collection<Path> dirs, Set<Class<? extends DataFileInfo>> dataFileInfoClasses) {

        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> datastoreMap = new HashMap<>();
        for (Path dir : dirs) {
            checkArgument(Files.isDirectory(dir), "File " + dir.toString() + " is not a directory");

            Set<Path> filesSet;
            try {
                filesSet = Files.list(dir).collect(Collectors.toCollection(TreeSet::new));
            } catch (IOException e) {
                throw new PipelineException("Unable to list files in " + dir.toString(), e);
            }

            for (Class<? extends DataFileInfo> clazz : dataFileInfoClasses) {
                Set<? extends DataFileInfo> dataFilesOfClass = dataFilesOfClass(clazz, filesSet);
                if (datastoreMap.containsKey(clazz)) {
                    @SuppressWarnings("unchecked")
                    Set<DataFileInfo> existingFilesOfClass = (Set<DataFileInfo>) datastoreMap
                        .get(clazz);
                    existingFilesOfClass.addAll(dataFilesOfClass);
                } else {
                    datastoreMap.put(clazz, dataFilesOfClass);
                }
            }

        }
        return datastoreMap;
    }

    /**
     * Copies a set of files from the datastore to the task directory. The originators of the files
     * are retrieved from the database; the pipeline task's existing set of producers is deleted and
     * replaced with the originators for the copied files.
     *
     * @param dataFiles {@link Set} of instances of {@link DataFileInfo} subclasses representing the
     * files to be copied.
     */
    public void copyToTaskDirectory(Set<? extends DataFileInfo> dataFiles) {
        Map<DataFileInfo, Path> dataFileInfoToPath = findInputFiles(dataFiles);
        for (DataFileInfo dataFileInfo : dataFileInfoToPath.keySet()) {
            taskDirCopyType.copy(dataFileInfoToPath.get(dataFileInfo),
                taskDirectory.resolve(dataFileInfo.getName()));
        }

        // obtain the originators for all datastore files and add them as producers to the
        // current pipeline task; also delete any existing ones so that in the event of a
        // reprocess the correct information is reflected.
        Set<Long> producerTaskIds = datastoreProducerConsumerCrud()
            .retrieveProducers(new HashSet<>(dataFileInfoToPath.values()));
        pipelineTask.clearProducerTaskIds();
        pipelineTask.setProducerTaskIds(producerTaskIds);
    }

    /**
     * Finds the set of files from the datastore that are needed as inputs for this task.
     *
     * @param dataFiles {@link Set} of instances of {@link DataFileInfo} subclasses representing the
     * files to be used as inputs
     * @return {@link Set} of {@link Path} instances for the files represented by the
     * {@link DataFileInfo} instances.
     */
    public Set<Path> dataFilesForInputs(Set<? extends DataFileInfo> dataFiles) {
        Set<Path> datastoreFiles = new HashSet<>();
        Map<DataFileInfo, Path> dataFileInfoToPath = findInputFiles(dataFiles);
        datastoreFiles.addAll(dataFileInfoToPath.values());
        return datastoreFiles;
    }

    private Map<DataFileInfo, Path> findInputFiles(Set<? extends DataFileInfo> dataFiles) {
        Map<DataFileInfo, Path> dataFileInfoToPath = new HashMap<>();
        for (DataFileInfo dataFileInfo : dataFiles) {
            dataFileInfoToPath.put(dataFileInfo, datastorePathLocator.datastorePath(dataFileInfo));
        }
        return dataFileInfoToPath;
    }

    /**
     * Deletes a set of files from the task directory.
     *
     * @param dataFiles Set of instances of DataFileInfo subclasses that represent the files to be
     * deleted.
     */
    public void deleteFromTaskDirectory(Set<? extends DataFileInfo> dataFiles) {
        for (DataFileInfo dataFileInfo : dataFiles) {
            Path taskDirLocation = taskDirectory.resolve(dataFileInfo.getName());
            try {
                if (Files.isRegularFile(taskDirLocation) || Files.isSymbolicLink(taskDirLocation)) {
                    Files.deleteIfExists(taskDirLocation);

                } else {
                    FileUtils.deleteDirectory(taskDirLocation.toFile());
                }
            } catch (IOException e) {
                throw new PipelineException(
                    "Unable to delete file " + dataFileInfo.getName().toString()
                        + " from task directory " + taskDirectory.toString(),
                    e);
            }
        }

    }

    /**
     * Moves a set of files from the task directory to the datastore.
     *
     * @param dataFiles Set of instances of DataFileInfo subclasses that represent the files to be
     * moved.
     */
    public void moveToDatastore(Set<? extends DataFileInfo> dataFiles) {
        Set<Path> datastoreFiles = new HashSet<>();
        for (DataFileInfo dataFileInfo : dataFiles) {
            Path taskDirLocation = taskDirectory.resolve(dataFileInfo.getName());
            Path datastoreLocation = datastorePathLocator.datastorePath(dataFileInfo);
            Path datastoreLocationParent = datastoreLocation.getParent();
            try {
                Files.createDirectories(datastoreLocationParent);
            } catch (IOException e) {
                throw new PipelineException("Unable to create directory " + datastoreLocationParent,
                    e);
            }
            datastoreCopyType.copy(taskDirLocation, datastoreLocation);
            datastoreFiles.add(datastoreRoot.relativize(datastoreLocation));
        }

        // Record the originator in the data accountability table in the database
        datastoreProducerConsumerCrud().createOrUpdateProducer(pipelineTask, datastoreFiles,
            DatastoreProducerConsumer.DataReceiptFileType.DATA);
    }

    private Set<String> filesInCompletedSubtasks(Set<Class<? extends DataFileInfo>> dataFileTypes,
        List<Path> completedSubtaskDirectories) {

        // Get the data files of the assorted types from all the successful subtask directories
        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> dataFilesMap = dataFilesMap(
            completedSubtaskDirectories, dataFileTypes);

        // Convert the file names from task directory format to datastore format
        Set<String> inputFiles = new HashSet<>();
        for (Set<? extends DataFileInfo> dataFileInfoSet : dataFilesMap.values()) {
            inputFiles.addAll(dataFileInfoSet.stream()
                .map(s -> datastorePathLocator.datastorePath(s).toString())
                .collect(Collectors.toSet()));
        }
        return inputFiles;
    }

    /**
     * Identifies the completed subtasks within a task that produced results and gets the names of
     * all files used by those subtasks based on a set of {@link DataFileInfo} instances. The file
     * names are returned.
     */
    public Set<String> filesInCompletedSubtasksWithResults(
        Set<Class<? extends DataFileInfo>> dataFileTypes) {
        return filesInCompletedSubtasks(dataFileTypes, completedSubtaskDirectoriesWithResults());
    }

    /**
     * Identifies the completed subtasks within a task that failed to produce results and gets the
     * names of all files used by those subtasks based on a set of {@link DataFileInfo} instances.
     * The file names are returned.
     */
    public Set<String> filesInCompletedSubtasksWithoutResults(
        Set<Class<? extends DataFileInfo>> dataFileTypes) {
        return filesInCompletedSubtasks(dataFileTypes, completedSubtaskDirectoriesWithoutResults());
    }

    // =========================================================================
    //
    // Private and package-private methods that perform services for the public methods
    //
    // =========================================================================

    /**
     * Returns the DatastoreProducerConsumerCrud instance, constructing if necessary. Public to
     * allow mocking in unit tests.
     */
    public DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
        if (datastoreProducerConsumerCrud == null) {
            datastoreProducerConsumerCrud = new DatastoreProducerConsumerCrud();
        }
        return datastoreProducerConsumerCrud;
    }

    /**
     * Returns the PipelineTaskCrud instance, constructing if necessary. Public to allow mocking in
     * unit tests.
     */
    public PipelineTaskCrud pipelineTaskCrud() {
        if (pipelineTaskCrud == null) {
            pipelineTaskCrud = new PipelineTaskCrud();
        }
        return pipelineTaskCrud;
    }

    /**
     * Helper function that performs the general process of searching a directory for files that
     * match DataFileType regular expressions, and returns a Map from the DataFileType instances to
     * the identified files.
     *
     * @param directory Directory to be searched.
     * @param dataFileTypes Set of DataFileType instances to search for.
     * @param regexType Regex to use (datastore or task dir)
     */
    private Map<DataFileType, Set<Path>> dataFilesMap(Path directory,
        Set<DataFileType> dataFileTypes, DataFileType.RegexType regexType) {
        return dataFilesMap(new ArrayList<>(Arrays.asList(directory)), dataFileTypes, regexType);
    }

    /**
     * Helper function that performs the general process of searching a collection of directories
     * for files that match DataFileType regular expressions, and returns a Map from the
     * DataFileType instances to the identified files.
     *
     * @param directories Collection of directories to be searched.
     * @param dataFileTypes Set of DataFileType instances to search for.
     * @param regexType Regex to use (datastore or task dir)
     */
    private Map<DataFileType, Set<Path>> dataFilesMap(Collection<Path> directories,
        Set<DataFileType> dataFileTypes, DataFileType.RegexType regexType) {

        Map<DataFileType, Set<Path>> dataFilesMap = new HashMap<>();
        Set<Path> filesSet;
        Stream<Path> pathStream = null;
        Path pathToRelativize = null;
        for (Path directory : directories) {
            try {
                pathStream = regexType.pathStream(directory);
                pathToRelativize = regexType.pathToRelativize(directory, datastoreRoot);
                final Path finalPathToRelativize = pathToRelativize;
                filesSet = pathStream.map(s -> finalPathToRelativize.relativize(s))
                    .collect(Collectors.toCollection(TreeSet::new));
            } catch (IOException e) {
                throw new PipelineException("Unable to list files in " + directory.toString(), e);
            } finally {
                if (pathStream != null) {
                    pathStream.close();
                }
            }

            for (DataFileType dataFileType : dataFileTypes) {
                Pattern pattern = regexType.getPattern(dataFileType);
                Set<Path> filesOfType = new TreeSet<>();
                for (Path filePath : filesSet) {
                    if (pattern.matcher(filePath.toString()).matches()) {
                        filesOfType.add(filePath);
                    }
                }
                filesOfType = filterDataFiles(filesOfType, regexType);
                if (dataFilesMap.containsKey(dataFileType)) {
                    dataFilesMap.get(dataFileType).addAll(filesOfType);
                } else {
                    dataFilesMap.put(dataFileType, filesOfType);
                }
            }
        }
        return dataFilesMap;

    }

    /**
     * Helper method that, for a given subclass of DataFileInfo, finds all the files that correspond
     * to that subclass and constructs DataFileInfo instances for them. The DataFileInfo instances
     * are returned as a Set.
     *
     * @param clazz Class object of DataFileInfo subclass
     * @param files Set of Path instances for files to be searched
     * @return Set of all Paths in files argument that correspond to the specified subclass of
     * DataFileInfo.
     */
    private <T extends DataFileInfo> Set<T> dataFilesOfClass(Class<T> clazz, Set<Path> files) {
        Set<T> dataFiles = new TreeSet<>();
        Set<Path> foundFiles = new HashSet<>();
        T dataFileInfoForPatternCheck;
        Constructor<T> stringArgConstructor;
        try {
            Constructor<T> noArgConstructor = clazz.getDeclaredConstructor();
            stringArgConstructor = clazz.getDeclaredConstructor(String.class);
            dataFileInfoForPatternCheck = noArgConstructor.newInstance();
        } catch (NoSuchMethodException | SecurityException | InstantiationException
            | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new PipelineException(
                "Unable to perform reflection on " + clazz.getCanonicalName(), e);
        }
        for (Path file : files) {

            // Check each file against the pattern for this DatastoreId subclass, and if
            // validity is indicated, add it to the dataFiles set.
            if (dataFileInfoForPatternCheck.pathValid(file)) {
                try {
                    dataFiles.add(stringArgConstructor.newInstance(file.getFileName().toString()));
                } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                    throw new PipelineException("Unable to instantiate class "
                        + clazz.getCanonicalName() + " with file " + file.toString(), e);
                }
                foundFiles.add(file);
            }

        }

        return dataFiles;
    }

    /**
     * Helper function that performs general copying of the contents of a Map of data file types and
     * their data files from one directory to another: either datastore-to-task-dir or
     * task-dir-to-datastore.
     *
     * @param dataFileTypesMap Map from DataFileType instances to data files.
     * @param destination RegexType that indicates the destination for the files.
     * @param performCopy if true, copy the files, otherwise just find them and return the Map.
     * @return Datastore locations of all copied files. For copy from datastore to task dir, the
     * returned files are relative to the task directory (i.e., just filenames); for copy from the
     * task dir to the datastore, the returned files are relative to the datastore root (i.e., they
     * contain the path within the datastore to the directory that contains the file, as well as the
     * filename).
     */
    private Map<DataFileType, Set<Path>> copyDataFilesByTypeToDestination(
        Map<DataFileType, Set<Path>> dataFileTypesMap, RegexType destination,
        DatastoreCopyType copyType) {

        Map<DataFileType, Set<Path>> copiedFiles = new HashMap<>();
        for (DataFileType dataFileType : dataFileTypesMap.keySet()) {
            Set<Path> dataFiles = dataFileTypesMap.get(dataFileType);
            if (destination.equals(RegexType.TASK_DIR)) {
                copiedFiles.put(dataFileType, dataFiles);
            }
            Set<Path> datastoreFiles = new HashSet<>();
            for (Path dataFile : dataFiles) {
                DataFilePaths dataFilePaths = destination.dataFilePaths(datastoreRoot,
                    taskDirectory, dataFileType, dataFile);
                datastoreFiles.add(datastoreRoot.relativize(dataFilePaths.getDatastorePath()));
                copyType.copy(dataFilePaths.getSourcePath(), dataFilePaths.getDestinationPath());

            }
            if (destination.equals(RegexType.DATASTORE)) {
                copiedFiles.put(dataFileType, datastoreFiles);
            }
        }
        return copiedFiles;
    }

    private List<Path> completedSubtaskDirectoriesWithResults() {
        return completedSubtaskDirectories(WITH_RESULTS);
    }

    private List<Path> completedSubtaskDirectoriesWithoutResults() {
        return completedSubtaskDirectories(WITHOUT_RESULTS);
    }

    private List<Path> completedSubtaskDirectories(Predicate<? super File> predicate) {

        // Reconstitute the TaskConfigurationManager from the task directory
        TaskConfigurationManager configManager = TaskConfigurationManager
            .restore(taskDirectory.toFile());

        // Identify the subtask directories that correspond to successful executions of an algorithm
        return configManager.allSubTaskDirectories()
            .stream()
            .filter(AlgorithmStateFiles::isComplete)
            .filter(predicate)
            .map(File::toPath)
            .collect(Collectors.toList());

    }

    private Set<Path> filterDataFiles(Set<Path> allDataFiles, RegexType destination) {

        // we never filter files that are in the task directory.
        // We also never filter files if the TaskConfigurationParameters isn't defined.
        if (destination.equals(RegexType.TASK_DIR) || taskConfigurationParameters == null) {
            return allDataFiles;
        }

        // if we're doing keep-up reprocessing, that's one case
        if (!taskConfigurationParameters.isReprocess()) {
            return filterDataFilesForKeepUpProcessing(allDataFiles);
        }

        // if there are excluded tasks (i.e., tasks that produced results that should not
        // be reprocessed), that's another case
        if (taskConfigurationParameters.getReprocessingTasksExclude() != null
            && taskConfigurationParameters.getReprocessingTasksExclude().length > 0) {
            return filterDataFilesForBugfixProcessing(allDataFiles,
                taskConfigurationParameters.getReprocessingTasksExclude());
        }

        // If we got this far then it's just garden variety reprocess-everything, so no
        // filtering
        return allDataFiles;
    }

    /**
     * Filters data files for "keep-up" processing. This is processing in which the user only needs
     * to process input files that have never been successfully processed by the selected pipeline
     * module. All data files that have been successfully processed in the pipeline by a node that
     * matches the pipeline task's {@link PipelineDefinitionNode} are filtered out.
     */
    private Set<Path> filterDataFilesForKeepUpProcessing(Set<Path> allDataFiles) {

        // collect all the consumers of the files in question -- note that we want both the
        // consumers that produced output with the files AND consumers that failed to produce
        // output but which recorded successful processing. The latter are stored with
        // negative consumer IDs. This is necessary because for any file that ran successfully
        // but produced no output, we don't want to process it again during "keep-up" processing.
        List<DatastoreProducerConsumer> dpcs = datastoreProducerConsumerCrud()
            .retrieveByFilename(allDataFiles);
        Set<Long> allConsumers = new HashSet<>();
        dpcs.stream().forEach(s -> allConsumers.addAll(s.getAllConsumers()));

        // Determine the consumers that have the same pipeline definition node as the
        // current task
        List<Long> consumersWithMatchingNode = pipelineTaskCrud()
            .retrieveIdsForPipelineDefinitionNode(allConsumers,
                pipelineTask.getPipelineDefinitionNode());

        // Return the data files that don't have any consumers that have the specified
        // pipeline definition node
        return dpcs.stream()
            .filter(s -> Collections.disjoint(s.getAllConsumers(), consumersWithMatchingNode))
            .map(s -> Paths.get(s.getFilename()))
            .collect(Collectors.toSet());

    }

    /**
     * Filters data files for "bugfix" reprocessing. This is reprocessing in which the user doesn't
     * want to process all files, but just the ones that have failed in a prior processing run. This
     * is accomplished by taking the prior run or runs (in the form of pipeline task IDs) and
     * excluding all inputs that were successfully processed in any of those prior tasks.
     */
    private Set<Path> filterDataFilesForBugfixProcessing(Set<Path> allDataFiles,
        long[] reprocessingTasksExclude) {

        List<DatastoreProducerConsumer> dpcs = datastoreProducerConsumerCrud()
            .retrieveByFilename(allDataFiles);

        // Figure out if any of the excluded tasks use the same pipeline definition node
        // as the current one, if not we can walk away
        List<Long> reprocessingTasksExcludeList = Arrays
            .asList(ArrayUtils.toObject(reprocessingTasksExclude));
        List<Long> tasksWithMatchingNode = pipelineTaskCrud().retrieveIdsForPipelineDefinitionNode(
            reprocessingTasksExcludeList, pipelineTask.getPipelineDefinitionNode());
        if (tasksWithMatchingNode.isEmpty()) {
            return allDataFiles;
        }

        // Otherwise, any input that DOES NOT have one of the excluded tasks as
        // a past consumer should be returned
        return dpcs.stream()
            .filter(s -> Collections.disjoint(s.getConsumers(), tasksWithMatchingNode))
            .map(s -> Paths.get(s.getFilename()))
            .collect(Collectors.toSet());
    }

    /**
     * Finds the actual source file for a given source file. If the source file is not a symbolic
     * link, then that file is the actual source file. If not, the symbolic link is read to find the
     * actual source file. The reading of symbolic links runs iteratively, so it produces the
     * correct result even in the case of a link to a link to a link... etc. The process of
     * following symbolic links stops at the first such link that is a child of the datastore root
     * path. Thus the "actual source" is either a non-symlink file that the src file is a link to,
     * or it's a file (symlink or regular file) that lies inside the datastore.
     */
    public static Path realSourceFile(Path src) throws IOException {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Path trueSrc = src;
        if (Files.isSymbolicLink(src) && !src.startsWith(datastoreRoot)) {
            trueSrc = realSourceFile(Files.readSymbolicLink(src));
        }
        return trueSrc;
    }

    /**
     * Enum-with-behavior that supports multiple different copy mechanisms that are specialized for
     * use with moving files between the datastore and a working directory. The following options
     * are provided:
     * <ol>
     * <li>{@link DatastoreCopyType#COPY} performs a traditional file copy operation. The copy is
     * recursive, so directories are supported as well as individual files.
     * <li>{@link DatastoreCopyType#SYMLINK} makes the destination a symbolic link to the true
     * source file, as defined by the {@link DataFileManager#realSourceFile(Path)} method.
     * Symlinking can be faster than copying and can consume less disk space (assuming the datastore
     * and working directories are on the same file system).
     * <li>{@link DatastoreCopyType#MOVE} will move the true source file to the destination; that
     * is, it will follow symlinks via the {@link DataFileManager#realSourceFile(Path)} method and
     * move the file that is found in this way. In addition, if the source file is a symlink, the
     * true source file will be changed to a symlink to the moved file in its new location. In this
     * way, the source file symlink remains valid and unchanged, but the file it points to is now
     * itself a symlink to the moved file.
     * </ol>
     * In addition to all the foregoing, {@link DatastoreCopyType} manages file permissions. After
     * execution of any move / copy / symlink operation, the new file's permissions are set to make
     * it write-protected and world-readable. If the copy / move / symlink operation is required to
     * overwrite the destination file, that file's permissions will be set to allow the overwrite
     * prior to execution.
     * <p>
     * IFor copying files from the datastore to the task directory, or from the task directory to
     * the subtask directory, {@link DatastoreCopyType#COPY}, and {@link DatastoreCopyType#SYMLINK}
     * options are available. For copies from the task directory to the datastore, only one option
     * is provided: {@link DatastoreCopyType#MOVE}.
     *
     * @author PT
     */
    private enum DatastoreCopyType {
        COPY {
            @Override
            protected void copyInternal(Path src, Path dest) throws IOException {
                checkout(src, dest);
                if (Files.isRegularFile(src)) {
                    Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    FileUtils.copyDirectory(src.toFile(), dest.toFile());
                }
            }

            @Override
            protected String pipelineExceptionFormat() {
                return "Unable to copy file %s to %s\n";
            }
        },
        MOVE {
            @Override
            protected void copyInternal(Path src, Path dest) throws IOException {
                checkout(src, dest);
                Path trueSrc = DataFileManager.realSourceFile(src);
                if (Files.exists(dest)) {
                    FileUtil.prepareDirectoryTreeForOverwrites(dest);
                }
                Files.move(trueSrc, dest, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
                FileUtil.writeProtectDirectoryTree(dest);
                if (src != trueSrc) {
                    Files.delete(src);
                    Files.createSymbolicLink(trueSrc, dest);
                }
            }

            @Override
            protected String pipelineExceptionFormat() {
                return "Unable to move file %s to %s\n";
            }
        },
        SYMLINK {
            @Override
            protected void copyInternal(Path src, Path dest) throws IOException {
                checkout(src, dest);
                Path trueSrc = DataFileManager.realSourceFile(src);
                if (Files.exists(dest)) {
                    Files.delete(dest);
                }
                Files.createSymbolicLink(dest, trueSrc);
            }

            @Override
            protected String pipelineExceptionFormat() {
                return "Unable to create symlink %s from %s\n";
            }
        };

        /**
         * Copy operation that allows / forces the caller to manage any {@link IOException} that
         * occurs.
         */
        protected abstract void copyInternal(Path src, Path dest) throws IOException;

        /**
         * Provides a formatting string for the {@link PipelineException} thrown by
         * {@link #copy(Path, Path)}.
         */
        protected abstract String pipelineExceptionFormat();

        /**
         * Copy operation that manages any resulting {@link IOException}}. In this event, a
         * {@link PipelineException} is thrown, which terminates execution of the datastore
         * operations.
         */
        public void copy(Path src, Path dest) {
            try {
                copyInternal(src, dest);
            } catch (IOException e) {
                throw new PipelineException(
                    String.format(pipelineExceptionFormat(), src.toString(), dest.toString()), e);
            }
        }

        private static void checkout(Path src, Path dest) {
            checkNotNull(src, "src");
            checkNotNull(dest, "dest");
            checkArgument(Files.exists(src, LinkOption.NOFOLLOW_LINKS),
                "Source file " + src + " does not exist");
        }

    }

    /**
     * Selects a {@link DatastoreCopyType} based on the type of the source file. Source files that
     * are symbolic links will use the {@link DatastoreCopyType#SYMLINK} operation, resulting in a
     * symbolic link at the destination that links to the true source file and removal of the
     * symbolic link that was used as the source file. Sources that are not symbolic links will use
     * the {@link DatastoreCopyType#MOVE} operation.
     *
     * @throws IOException if any such occurs during the underlying file operations.
     */
    public static void moveOrSymlink(Path src, Path dest) throws IOException {

        if (Files.isSymbolicLink(src)) {
            DatastoreCopyType.SYMLINK.copy(src, dest);
            Files.delete(src);
        } else {
            DatastoreCopyType.MOVE.copy(src, dest);
        }
    }

}
