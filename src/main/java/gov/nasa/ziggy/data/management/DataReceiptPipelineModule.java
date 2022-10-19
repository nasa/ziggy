package gov.nasa.ziggy.data.management;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer.DataReceiptFileType;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.ProcessingStatePipelineModule;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.io.FileUtil;
import gov.nasa.ziggy.worker.WorkerTaskRequestDispatcher;
import jakarta.xml.bind.JAXBException;

/**
 * Pipeline module that performs data receipt, defined as the process that brings science data and
 * instrument models into the datastore from the outside world.
 * <p>
 * This class requires an instance of an implementation of the {@link DataImporter} interface, which
 * provides validation for the overall delivery and for each individual data file. The
 * {@link DataImporter} subclass is specified in the properties file. If no such specification is
 * provided, the {@link DefaultDataImporter} class will be used, which performs no validations.
 * <p>
 * This class also uses an instance of {@link ModelImporter} to import the models.
 * <p>
 * In order to determine the regular expressions for data and model files for import, one or more
 * {@link DataFileType} instances and one or more {@link ModelType} instances must be provided for
 * the data receipt node in the pipeline definition. All files that are successfully imported will
 * have a database entry that shows which pipeline task was used to perform the import. Any failures
 * will be recorded in a separate database table.
 * <p>
 * The importer uses an import directory that is specified in the properties file. Data files in
 * this directory must use their task directory name format in order to be located and imported.
 * Files that are regular files will be moved to their specified locations in the datastore; files
 * that are symlinks will be unlinked, and a new symlink will be created at the specified location
 * in the datastore.
 *
 * @author PT
 */
public class DataReceiptPipelineModule extends PipelineModule
    implements ProcessingStatePipelineModule {

    private static final Logger log = LoggerFactory.getLogger(DataReceiptPipelineModule.class);

    private static final String DEFAULT_DATA_RECEIPT_CLASS = "gov.nasa.ziggy.data.management.DefaultDataImporter";
    public static final String DATA_RECEIPT_MODULE_NAME = "data-receipt";

    private DataImporter dataImporter;
    protected ModelImporter modelImporter;
    private ManifestCrud manifestCrud;
    private List<String> namesOfFilesToImport;
    private Path dataReceiptTopLevelPath;
    private Path dataImportPathForTask;
    private Manifest manifest;
    private Acknowledgement ack;
    private String dataReceiptDir;
    private boolean processingComplete = false;
    private Path datastoreRoot;
    private boolean allFilesImported = true;

    public DataReceiptPipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        super(pipelineTask, runMode);
    }

    @Override
    public boolean processTask() throws PipelineException {

        // Get the top-level DR directory and the datastore root directory
        Configuration config = ZiggyConfiguration.getInstance();
        dataReceiptDir = config.getString(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME);
        checkState(dataReceiptDir != null,
            PropertyNames.DATA_RECEIPT_DIR_PROP_NAME + " missing or empty");
        datastoreRoot = DirectoryProperties.datastoreRootDir();
        UnitOfWork uow = pipelineTask.getUowTask().getInstance();
        dataReceiptTopLevelPath = Paths.get(dataReceiptDir);
        dataImportPathForTask = dataReceiptTopLevelPath
            .resolve(DirectoryUnitOfWorkGenerator.directory(uow));
        checkState(Files.isDirectory(dataImportPathForTask),
            dataImportPathForTask.toString() + " not a directory");

        boolean containsNonHiddenFiles = false;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dataImportPathForTask)) {
            for (Path filePath : dirStream) {
                if (!Files.isHidden(filePath)) {
                    containsNonHiddenFiles = true;
                    break;
                }
            }
        } catch (IOException e) {
            throw new PipelineException(
                "Unable to list directory " + dataImportPathForTask.toString());
        }

        if (!containsNonHiddenFiles) {
            log.warn("Directory " + dataImportPathForTask.toString()
                + " contains no files, skipping DR");
            alertService().generateAndBroadcastAlert("DR", pipelineTask.getId(), Severity.WARNING,
                "Directory " + dataImportPathForTask.toString() + " contains no files");
            return true;
        }

        // Read the manifest
        readManifest();

        // Get and execute the run mode
        runMode.run(this);

        return true;
    }

    /**
     * Defines the valid processing states for DR. During
     * {@link ProcessingState#ALGORITHM_EXECUTING}, the manifest is read, acknowledgement written,
     * and validation steps are taken. During {@link ProcessingState#STORING}, the files in the DR
     * directory are imported into the datastore.
     */
    @Override
    public List<ProcessingState> processingStates() {
        return Arrays.asList(ProcessingState.INITIALIZING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.STORING, ProcessingState.COMPLETE);
    }

    /**
     * Performs the processing based on the current value of the task processing state. In this
     * case, the "loop" loops over the valid states until the {@link ProcessingState#COMPLETE} is
     * reached, at which time the loop exits.
     */
    @Override
    public void processingMainLoop() {

        while (!processingComplete) {
            if (WorkerTaskRequestDispatcher.isTaskDeleted(pipelineTask.getId())) {
                log.error("Exiting Data Receipt pipeline module due to task deletion");
                return;
            }
            getProcessingState().taskAction(this);
        }
    }

    /**
     * If the task is in the initializing state, simply advance it to the next state, which will be
     * {@link ProcessingState#ALGORITHM_EXECUTING}.
     */
    @Override
    public void initializingTaskAction() {
        incrementProcessingState();
    }

    /**
     * Performs the algorithm portion of DR, which is reading the manifest, validating the delivered
     * files, and generating an acknowledgement.
     */
    @Override
    public void executingTaskAction() {

        manifest.setImportTime(new Date());
        manifest.setImportTaskId(pipelineTask.getId());

        // Check the uniqueness of the dataset ID, unless the ID value is <= 0
        if (manifest.getDatasetId() > 0
            && manifestCrud().datasetIdExists(manifest.getDatasetId())) {
            throw new PipelineException(
                "Dataset ID " + manifest.getDatasetId() + " has already been used");
        }

        // Generate the acknowledgement object -- note that this also performs the
        // transfer validation and size / checksum validation for all files in the
        // manifest. If the manifest contains files with problems, the import will
        // terminate with an exception.
        acknowledgeManifest();

        // Save the manifest information in the database.
        manifest.setAcknowledged(true);
        manifestCrud().create(manifest);

        // Make sure that all the regular files in the directory tree have been validated
        // (i.e., there are no files in the directory tree that are absent from the
        // manifest).
        checkForFilesNotInManifest();

        incrementProcessingState();
    }

    private void readManifest() {

        try {
            manifest = Manifest.readManifest(dataImportPathForTask);
            if (manifest == null) {
                throw new PipelineException(
                    "No manifest file present in directory " + dataImportPathForTask.toString());
            }
        } catch (InstantiationException | IllegalAccessException | IOException | SAXException
				| JAXBException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException e) {
            throw new PipelineException("Unable to read manifest from directory " + dataReceiptDir,
                e);
        }
        log.info("Read manifest from file " + manifest.getName());
    }

    private void acknowledgeManifest() {

        ack = Acknowledgement.of(manifest, dataImportPathForTask, pipelineTask.getId());

        // Write the acknowledgement to the directory.
        try {
            ack.write(dataImportPathForTask);
            log.info("Acknowledgement file written: " + ack.getName());
		} catch (InstantiationException | IllegalAccessException | SAXException | JAXBException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException("Unable to write manifest acknowledgement", e);
        }

        // If the acknowledgement has bad status, throw an exception now.
        if (ack.getTransferStatus().equals(DataReceiptStatus.INVALID)) {
            log.error("Validation of files against the manifest status == INVALID");
            throw new PipelineException(
                "Data Receipt terminated due to manifest validation failure");
        }
    }

    private void checkForFilesNotInManifest() {

        // Get the names of the files that passed validation (which at this point should
        // be the set of all names in the manifest)
        List<String> namesOfValidFiles = ack.namesOfValidFiles();

        try {
            Map<Path, Path> regularFilesInDirTree = FileUtil
                .regularFilesInDirTree(dataImportPathForTask);
            List<String> filenamesInDirTree = regularFilesInDirTree.keySet()
                .stream()
                .map(s -> s.toString())
                .collect(Collectors.toList());
            filenamesInDirTree.removeAll(namesOfValidFiles);
            filenamesInDirTree.remove(manifest.getName());
            filenamesInDirTree.remove(ack.getName());
            if (filenamesInDirTree.size() != 0) {
                log.error("Data receipt directory " + dataImportPathForTask.toString()
                    + " contains files not listed in manifest ");
                for (String filename : filenamesInDirTree) {
                    log.error("File missing from manifest: " + filename);
                }
                ack.write(dataImportPathForTask);
                manifest.setAcknowledged(true);
                log.info("Acknowledgement file written: " + ack.getName());
                throw new PipelineException("Unable to import files from data receipt directory "
                    + dataImportPathForTask.toString()
                    + " due to presence of files not listed in manifest");
            }
        } catch (IOException e1) {
            throw new PipelineException(
                "Unable to find regular files in directory " + dataImportPathForTask.toString(),
                e1);
		} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | SAXException
				| InvocationTargetException | JAXBException e) {
            throw new PipelineException("Unable to write manifest acknowledgement", e);
        }
    }

    /**
     * Imports files from the DR directory into the datastore.
     * <p>
     * Note that once storing has begun, any attempt to re-run the algorithm step will fail because
     * some of the files in the manifest will no longer be present in the DR directory. For this
     * reason, DR only supports the "Resume Current Step" restart option.
     */
    @Override
    public void storingTaskAction() {

        // Make a list of files for import. This is the list of non-hidden files in the
        // data receipt directory. Note that in the steps above we have implicitly ensured that
        // all regular files that are about to get imported are present in the manifest and are
        // valid based on the checksum and the size of the files. Hence, it is now safe to simply
        // walk through the contents of the data receipt directory and import everything.
        generateFilenamesForImport();

        importDataFiles(datastoreRoot);

        importModels(datastoreRoot);

        if (!allFilesImported) {
            throw new PipelineException("File import failures detected");
        }

        performDirectoryCleanup();

        incrementProcessingState();
    }

    private void generateFilenamesForImport() {
        try (Stream<Path> filestream = Files.list(dataImportPathForTask)) {
            namesOfFilesToImport = filestream.filter(t -> {

                // Seriously, JDK? You're going to force me to have a try-catch block
                // inside a stream if I ever want to use any java.nio.file methods within
                // the stream? You can't simply manage it on the basis of the entire stream
                // operation sitting inside a try-catch block?
                try {
                    return !Files.isHidden(t);
                } catch (IOException e) {
                    throw new PipelineException(
                        "IOException when checking hidden status of file " + t.toString(), e);
                }
            })
                .map(s -> dataImportPathForTask.relativize(s))
                .map(Path::toString)
                .collect(Collectors.toList());
        } catch (IOException e1) {
            throw new PipelineException(
                "Unable to stream file list in directory " + dataImportPathForTask.toString(), e1);
        }
    }

    /**
     * Performs import of mission data files into the datatore.
     */
    private void importDataFiles(Path datastoreRootPath) {

        final DataImporter dataImporter = dataImporter(Paths.get(dataReceiptDir),
            datastoreRootPath);

        dataImporter.importFilesToDatastore(namesOfFilesToImport);
        if (dataImporter.getInvalidFilesCount() > 0) {
            allFilesImported = false;
            log.warn("Detected " + dataImporter.getInvalidFilesCount()
                + " data files that failed validation");
            alertService().generateAndBroadcastAlert("Data Receipt (DR)", pipelineTask.getId(),
                AlertService.Severity.WARNING,
                "Failed to import " + dataImporter.getInvalidFilesCount() + " data files (out of "
                    + dataImporter.getTotalDataFileCount() + ")");
        }

        if (dataImporter.getFailedImportsCount() > 0) {
            allFilesImported = false;
            log.warn("Detected " + dataImporter.getFailedImportsCount()
                + " data files that were not imported");
        }

        // Generate data accountability records.
        persistProducerConsumerRecords(dataImporter.getSuccessfulImports(),
            dataImporter.getFailedImports(), DataReceiptFileType.DATA);
    }

    /**
     * Create and persist the data accountability records for successful and failed imports.
     */
    protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
        Collection<Path> failedImports, DataReceiptFileType fileType) {

        // Persist successful file records to the datastore producer-consumer table
        if (!successfulImports.isEmpty()) {
            new DatastoreProducerConsumerCrud().createOrUpdateProducer(pipelineTask,
                successfulImports, fileType);
        }

        // Save the failure cases to the FailedImport database table
        if (!failedImports.isEmpty()) {
            new FailedImportCrud().create(pipelineTask, failedImports, fileType);
        }
    }

    /**
     * Performs import of instrument models to the datastore. This method must be synchronized in
     * order to ensure that the model registry is updated by only one task at a time. This ensures
     * that imports that run across multiple tasks, with model imports in each task, will not result
     * in a corrupted model registry.
     */
    private void importModels(Path datastoreRootPath) {
        synchronized (DataReceiptPipelineModule.class) {

            Path dataReceiptPath = Paths.get(dataReceiptDir);
            // get the unit of work from the pipeline task
            UnitOfWork uow = pipelineTask.getUowTask().getInstance();
            Path importDirectory = dataReceiptPath
                .resolve(DirectoryUnitOfWorkGenerator.directory(uow));
            log.info("Importing models from directory: " + importDirectory.toString());

            // Obtain the model types from the pipeline task
            Set<ModelType> modelTypes = pipelineTask.getPipelineDefinitionNode().getModelTypes();

            ModelImporter importer = modelImporter(importDirectory,
                "Model imports performed at time " + new Date().toString());

            // Set up and perform imports
            importer.setDataReceiptTaskId(pipelineTask.getId());
            importer.setModelTypesToImport(modelTypes);
            boolean modelFilesLocated = importer.importModels(namesOfFilesToImport);

            if (modelFilesLocated) {

                // The pipeline instance is supposed to have a model registry with all the current
                // models in it. Unfortunately, the instance that contains this importer can't have
                // that registry because the models were just imported. Add the registry to the
                // instance now.
                updateModelRegistryForPipelineInstance();

                // If there are any failed imports, we need to save that information now
                List<Path> successfulImports = importer.getSuccessfulImports();
                List<Path> failedImports = importer.getFailedImports();
                persistProducerConsumerRecords(successfulImports, failedImports,
                    DataReceiptFileType.MODEL);
                if (!failedImports.isEmpty()) {
                    allFilesImported = false;
                    log.warn(failedImports.size() + " out of "
                        + (successfulImports.size() + failedImports.size())
                        + " model files failed to import");
                    alertService().generateAndBroadcastAlert("Data Receipt (DR)",
                        pipelineTask.getId(), AlertService.Severity.WARNING,
                        "Failed to import " + failedImports.size() + " model files (out of "
                            + (successfulImports.size() + failedImports.size()) + ")");

                }
                // Flush the session so that as soon as the next task starts importing model files
                // it already has an up to date registry
                flushDatabase();
            }
        }
    }

    /**
     * Flushes the database. Broken out to facilitate testing.
     */
    protected void flushDatabase() {
        DatabaseService.getInstance().getSession().flush();
    }

    // Allows a caller to supply a data receipt instance for test purposes
    DataImporter dataImporter(Path dataReceiptPath, Path datastoreRootPath) {
        if (dataImporter == null) {

            // Get the data importer implementation
            Configuration config = ZiggyConfiguration.getInstance();
            String classname = config.getString(PropertyNames.DATA_RECEIPT_CLASS_PROP_NAME,
                DEFAULT_DATA_RECEIPT_CLASS);
            Class<?> dataReceiptClass = null;
            try {
                dataReceiptClass = Class.forName(classname);
            } catch (ClassNotFoundException e) {
                throw new PipelineException("Class " + classname + " not found", e);
            }
            if (!DataImporter.class.isAssignableFrom(dataReceiptClass)) {
                throw new PipelineException(
                    "Class" + classname + " not implementation of DataReceipt interface");
            }

            // Instantiate the appropriate class
            try {
                dataImporter = (DataImporter) dataReceiptClass
                    .getDeclaredConstructor(PipelineTask.class, Path.class, Path.class)
                    .newInstance(pipelineTask, dataReceiptPath, datastoreRootPath);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new PipelineException("Unable to instantiate object of class " + classname,
                    e);
            }

        }
        return dataImporter;
    }

    /**
     * Performs cleanup on the directory used as the file source for this data receipt unit of work.
     * During cleanup the directory is checked to make sure that the only non-hidden files present
     * are the manifest and acknowledgement; these are then moved to the master manifest /
     * acknowledgement directory. If the UOW used a subdirectory of the main DR directory, that
     * directory is deleted.
     */
    public void performDirectoryCleanup() {

        try {
            // Create the manifests directory if it doesn't yet exist
            Path manifestDir = DirectoryProperties.manifestsDir();
            Files.createDirectories(manifestDir);

            // Move the manifest and the acknowledgement to the hidden manifests directory.
            Files.move(dataImportPathForTask.resolve(manifest.getName()),
                manifestDir.resolve(manifest.getName()), StandardCopyOption.REPLACE_EXISTING);
            String ackName = Acknowledgement.nameFromManifestName(manifest);
            Files.move(dataImportPathForTask.resolve(ackName), manifestDir.resolve(ackName),
                StandardCopyOption.REPLACE_EXISTING);

            Path realPath = DataFileManager.realSourceFile(dataImportPathForTask);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(realPath)) {
                for (Path file : stream) {
                    // Ignore hidden files
                    if (Files.isHidden(file)) {
                        continue;
                    }
                    // If we got here we have non-hidden, non manifest, non-ack files, so we can't
                    // delete this directory
                    throw new PipelineException(
                        "Directory " + dataImportPathForTask.getFileName().toString()
                            + " not empty, file " + file.getFileName().toString() + " detected");
                }
            }

            // Delete the directory unless it's the main DR directory
            if (!dataImportPathForTask.equals(dataReceiptTopLevelPath)) {
                FileUtils.deleteDirectory(dataImportPathForTask.toFile());
            }

        } catch (IOException e) {
            throw new PipelineException(
                "Unable to perform post-import cleanup of directory " + dataImportPathForTask, e);
        }
    }

    /**
     * Notifies the processing loop that processing is complete because the task processing state is
     * {@link ProcessingState#COMPLETE}.
     */
    @Override
    public void processingCompleteTaskAction() {
        processingComplete = true;
    }

    // Allows a caller to supply an alert service instance for test purposes
    AlertService alertService() {
        return AlertService.getInstance();
    }

    // Allows a caller to supply a model importer for test purposes.
    ModelImporter modelImporter(Path importDirectory, String description) {
        if (modelImporter == null) {
            modelImporter = new ModelImporter(importDirectory.toString(), description);
        }
        return modelImporter;
    }

    // Updates the model registry in the current pipeline instance. Package scope
    // for testing purposes.
    void updateModelRegistryForPipelineInstance() {
        ModelCrud modelCrud = new ModelCrud();
        modelCrud.lockCurrentRegistry();
        ModelRegistry modelRegistry = modelCrud.retrieveCurrentRegistry();
        PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
        PipelineInstance dbInstance = pipelineInstanceCrud
            .retrieve(pipelineTask.getPipelineInstance().getId());
        dbInstance.setModelRegistry(modelRegistry);
        pipelineInstanceCrud.update(dbInstance);
        pipelineTask.getPipelineInstance().setModelRegistry(modelRegistry);
    }

    ManifestCrud manifestCrud() {
        if (manifestCrud == null) {
            manifestCrud = new ManifestCrud();
        }
        return manifestCrud;
    }

    @Override
    public String getModuleName() {
        return "data receipt";
    }

    /**
     * The only supported restart mode for {@link DataReceiptPipelineModule} is
     * {@link RunMode#RESUME_CURRENT_STEP}. This is because the storing step removes files from the
     * DR directory. Thus, if a task is interrupted during storage, and is restarted at an earlier
     * step, the re-run will fail because the content of the DR directory no longer matches the
     * manifest (i.e., files in the manifest have been moved out of the directory).
     */
    @Override
    protected List<RunMode> restartModes() {
        return defaultRestartModes();
    }

    @Override
    protected List<RunMode> defaultRestartModes() {
        return Arrays.asList(RunMode.RESUME_CURRENT_STEP);

    }

    @Override
    protected void restartFromBeginning() {
    }

    @Override
    protected void resumeCurrentStep() throws PipelineException {
        processingMainLoop();
    }

    @Override
    protected void resubmit() {
    }

    @Override
    protected void resumeMonitoring() {
    }

    @Override
    protected void runStandard() throws PipelineException {
        processingMainLoop();
    }

    @Override
    public long pipelineTaskId() {
        return taskId();
    }

    @Override
    public void marshalingTaskAction() {
    }

    @Override
    public void submittingTaskAction() {
    }

    @Override
    public void queuedTaskAction() {
    }

    @Override
    public void algorithmCompleteTaskAction() {
    }

    /**
     * Creates the {@link DataReceiptPipelineModule} for import into the database the database.
     */
    public static PipelineModuleDefinition createDataReceiptPipelineForDb() {

        // Create the data receipt pipeline module
        PipelineModuleDefinition dataReceiptModule = new PipelineModuleDefinition(
            DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME);
        ClassWrapper<PipelineModule> moduleClassWrapper = new ClassWrapper<>(
            DataReceiptPipelineModule.class);
        dataReceiptModule.setPipelineModuleClass(moduleClassWrapper);
        return dataReceiptModule;

    }

}
