package gov.nasa.ziggy.data.management;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Pipeline module that performs data receipt, defined as the process that brings science data and
 * instrument models into the datastore from the outside world.
 * <p>
 * This class requires an instance of an implementation of the {@link DataReceiptDefinition}
 * interface, which provides an overall definition of the requirements and conventions of the data
 * receipt implementation for a given pipeline. The {@link DataReceiptDefinition} implementing class
 * is specified in the properties file. If no such specification is provided, the
 * {@link DatastoreDirectoryDataReceiptDefinition} class will be used.
 * <p>
 * The importer uses an import directory that is specified in the properties file. Files that are
 * regular files will be moved to their specified locations in the data storage system; files that
 * are symlinks will be unlinked, and a new symlink will be created at the specified location in the
 * data storage system.
 *
 * @author PT
 */
public class DataReceiptPipelineModule extends PipelineModule {

    private static final Logger log = LoggerFactory.getLogger(DataReceiptPipelineModule.class);

    private static final String DEFAULT_DATA_RECEIPT_CLASS = DatastoreDirectoryDataReceiptDefinition.class
        .getCanonicalName();
    public static final String DATA_RECEIPT_MODULE_NAME = "data-receipt";
    public static final String DEFAULT_DATA_RECEIPT_UOW_GENERATOR_CLASS = DataReceiptUnitOfWorkGenerator.class
        .getCanonicalName();

    /**
     * Value chosen to match the chunk size used by ZiggyQuery, and then preserved because it was
     * emprically acceptable.
     */
    private static final int FILE_CHUNK_SIZE = 50_000;

    private DataReceiptDefinition dataReceiptDefinition;
    protected ModelImporter modelImporter;
    private Path dataReceiptTopLevelPath;
    private Path dataImportPathForTask;
    private String dataReceiptDir;
    private boolean processingComplete = false;
    private DataReceiptOperations dataReceiptOperations = new DataReceiptOperations();
    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations = new DatastoreProducerConsumerOperations();

    public DataReceiptPipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        super(pipelineTask, runMode);

        // Get the top-level DR directory and the datastore root directory
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        dataReceiptDir = config.getString(PropertyName.DATA_RECEIPT_DIR.property());
        UnitOfWork uow = pipelineTask.uowTaskInstance();
        dataReceiptTopLevelPath = Paths.get(dataReceiptDir).toAbsolutePath();
        dataImportPathForTask = dataImportPathForTask(uow);
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public boolean processTask() {

        boolean containsNonHiddenFiles = false;
        Path filePathForException = null;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dataImportPathForTask)) {
            for (Path filePath : dirStream) {
                filePathForException = filePath;
                if (!Files.isHidden(filePath)) {
                    containsNonHiddenFiles = true;
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to check hidden status of " + filePathForException.toString(), e);
        }

        if (!containsNonHiddenFiles) {
            log.warn("Directory " + dataImportPathForTask.toString()
                + " contains no files, skipping DR");
            alertService().generateAndBroadcastAlert("DR", pipelineTask.getId(), Severity.WARNING,
                "Directory " + dataImportPathForTask.toString() + " contains no files");
            return true;
        }

        // Get and execute the run mode
        runMode.run(this);

        return true;
    }

    /**
     * Defines the valid processing steps for DR. During {@link ProcessingStep#EXECUTING}, the
     * manifest is read, acknowledgement written, and validation steps are taken. During
     * {@link ProcessingStep#STORING}, the files in the DR directory are imported into the
     * datastore.
     */
    @Override
    public List<ProcessingStep> processingSteps() {
        return List.of(ProcessingStep.EXECUTING, ProcessingStep.STORING);
    }

    /**
     * Performs the processing based on the current value of the task processing state. In this
     * case, the "loop" loops over the valid states until the {@link ProcessingStep#COMPLETE} is
     * reached, at which time the loop exits.
     */
    private void processingMainLoop() {
        while (!processingComplete) {
            currentProcessingStep().taskAction(this);
        }
    }

    /**
     * Performs the algorithm portion of DR: validation of the delivery and of the delivered files.
     */
    @Override
    public void executingTaskAction() {

        // Validate the delivery.
        dataReceiptDefinition = dataReceiptDefinition();
        dataReceiptDefinition().setDataImportDirectory(dataImportPathForTask);
        dataReceiptDefinition().setPipelineTask(pipelineTask);
        boolean deliveryValid = dataReceiptDefinition().isConformingDelivery();
        if (!deliveryValid) {
            throw new PipelineException("Delivery validation failed");
        }

        List<Path> filesToImport = dataReceiptDefinition().filesForImport();
        List<String> invalidFiles = filesToImport.stream()
            .filter(s -> !dataReceiptDefinition().isConformingFile(s))
            .map(Path::toString)
            .collect(Collectors.toList());
        if (invalidFiles.size() != 0) {
            for (String invalidFile : invalidFiles) {
                log.error("File failed data receipt validation: {}", invalidFile);
            }
            throw new PipelineException("File validation failed, see task log for details");
        }

        // If we made it this far, we can proceed to the storing state, which performs the actual
        // import.
        incrementProcessingStep();
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

        dataReceiptDefinition().importFiles();
        persistProducerConsumerRecords(dataReceiptDefinition().successfulImports(),
            dataReceiptDefinition().failedImports());

        if (dataReceiptDefinition().failedImports().size() > 0) {
            throw new PipelineException("File import failures detected");
        }

        if (dataReceiptDefinition().cleanDataReceiptDirectories()) {
            performDirectoryCleanup();
        }
        processingComplete = true;
    }

    /**
     * Create and persist the data accountability records for successful and failed imports.
     */
    protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
        Collection<Path> failedImports) {

        // Persist successful file records to the datastore producer-consumer table.
        // Note that we chunk the files here even though the createOrUpdateProducer
        // method uses ZiggyQuery's chunked query internally. This allows progress in
        // persisting the records to be logged. It also allows the actual database
        // transactions to occur chunk-by-chunk, which ensures that Hibernate does
        // not become overwhelmed by the need to persist an enormous number of rows
        // all at once.
        if (!successfulImports.isEmpty()) {
            log.info("Updating {} producer-consumer records ...", successfulImports.size());
            int updatedFiles = 0;
            for (List<Path> fileChunk : Lists.partition(new ArrayList<>(successfulImports),
                FILE_CHUNK_SIZE)) {
                datastoreProducerConsumerOperations().createOrUpdateProducer(pipelineTask,
                    fileChunk);
                updatedFiles += fileChunk.size();
                log.info("Updated {} producer-consumer records", updatedFiles);
            }
            log.info("Updating {} producer-consumer records ...done", successfulImports.size());
        }

        // Save the failure cases to the FailedImport database table
        if (!failedImports.isEmpty()) {
            log.info("Recording {} failed imports...", failedImports.size());
            dataReceiptOperations().createFailedImportRecords(pipelineTask, failedImports);
            log.info("Recording {} failed imports...done", failedImports.size());
        }
    }

    /**
     * Instantiates a {@link DataReceiptDefinition} instance of the user-specified implementing
     * class.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    DataReceiptDefinition dataReceiptDefinition() {
        if (dataReceiptDefinition == null) {

            // Get the data importer implementation
            ImmutableConfiguration config = ZiggyConfiguration.getInstance();
            String classname = config.getString(PropertyName.DATA_IMPORTER_CLASS.property(),
                DEFAULT_DATA_RECEIPT_CLASS);
            Class<?> dataReceiptClass = null;
            try {
                dataReceiptClass = Class.forName(classname);
            } catch (ClassNotFoundException e) {
                throw new PipelineException("Class " + classname + " not found", e);
            }
            if (!DataReceiptDefinition.class.isAssignableFrom(dataReceiptClass)) {
                throw new PipelineException(
                    "Class" + classname + " not implementation of DataReceipt interface");
            }

            // Instantiate the appropriate class
            try {
                Constructor<?> ctor = dataReceiptClass.getConstructor();
                dataReceiptDefinition = (DataReceiptDefinition) ctor.newInstance();
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new PipelineException(
                    "Class " + dataReceiptClass.getName() + " has no zero-argument constructor");
            }
        }
        return dataReceiptDefinition;
    }

    /**
     * Performs cleanup on the directory used as the file source for this data receipt unit of work.
     * Specifically: if the UOW used a subdirectory of the main DR directory, that directory is
     * deleted; other directories within the UOW directory are deleted if they are empty, otherwise
     * an exception occurs.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void performDirectoryCleanup() {
        cleanUpSpecifiedDirectory(dataImportPathForTask);
    }

    /**
     * Recursively loops through all directories and subdirectories; if they contain only hidden
     * files, they can be deleted.
     */
    private void cleanUpSpecifiedDirectory(Path directory) {
        try {
            Path realPath = ZiggyFileUtils.realSourceFile(directory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(realPath)) {
                for (Path file : stream) {
                    // Ignore hidden files.
                    if (Files.isHidden(file)) {
                        continue;
                    }
                    if (Files.isDirectory(file)) {
                        cleanUpSpecifiedDirectory(file);
                        continue;
                    }
                    // If we got here we have non-hidden, non-directory files, so we can't
                    // delete this directory
                    throw new PipelineException(
                        "Directory " + dataImportPathForTask.getFileName().toString()
                            + " not empty, file " + file.getFileName().toString() + " detected");
                }
            }

            // Delete the directory unless it's the main DR directory
            if (!directory.equals(dataReceiptTopLevelPath)) {
                FileUtils.deleteDirectory(directory.toFile());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "IOException occurred cleaning up directory " + dataImportPathForTask.toString(),
                e);
        }
    }

    // Allows a caller to supply an alert service instance for test purposes.
    AlertService alertService() {
        return AlertService.getInstance();
    }

    DataReceiptOperations dataReceiptOperations() {
        return dataReceiptOperations;
    }

    DatastoreProducerConsumerOperations datastoreProducerConsumerOperations() {
        return datastoreProducerConsumerOperations;
    }

    @Override
    public String getModuleName() {
        return "data receipt";
    }

    Path dataImportPathForTask(UnitOfWork uow) {
        Path importPathForTask = dataReceiptTopLevelPath
            .resolve(DirectoryUnitOfWorkGenerator.directory(uow));
        checkState(Files.isDirectory(importPathForTask),
            importPathForTask.toString() + " not a directory");
        return importPathForTask;
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
        return List.of(RunMode.RESUME_CURRENT_STEP);
    }

    @Override
    protected void resumeCurrentStep() {
        processingMainLoop();
    }

    @Override
    protected void runStandard() {
        processingMainLoop();
    }
}
