package gov.nasa.ziggy.models;

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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Imports models of all types from a specified directory.
 * <p>
 * The model import process includes the following steps:
 * <ol>
 * <li>If the model filename includes a model ID, this is compared to the model ID in the current
 * model registry. If the ID is &#60;= the model registry ID, an exception is thrown.
 * <li>If the model name does not include an ID, an ID number is prepended that is 1 larger than the
 * ID of the model in the registry.
 * <li>If the model name does not include a timestamp, the current time in ISO 8601 format is
 * prepended onto the file name.
 * <li>The model is imported into the correct directory in the datastore and, if necessary, its name
 * is changed to match the datastore name.
 * <li>The original name, datastore name, and ID number are added to the model metadata in the
 * database.
 * <li>A new model registry is created with all the latest models.
 * </ol>
 *
 * @author PT
 */
public class ModelImporter {

    private static final Logger log = LoggerFactory.getLogger(ModelImporter.class);

    private Path datastoreRoot;
    private ModelCrud modelCrud = new ModelCrud();
    private Path datastoreModelsRoot;
    private Path dataImportPath;
    String modelDescription;
    private Set<ModelType> modelTypesToImport = new HashSet<>();
    private long dataReceiptTaskId;
    private List<Path> successfulImports = new ArrayList<>();
    private List<Path> failedImports = new ArrayList<>();

    public static final String DATASTORE_MODELS_SUBDIR_NAME = "models";

    public ModelImporter(Path dataImportPath, String modelDescription) {
        this.modelDescription = modelDescription;
        this.dataImportPath = dataImportPath;
        datastoreRoot = DirectoryProperties.datastoreRootDir().toAbsolutePath();
        datastoreModelsRoot = datastoreRoot
            .resolve(Paths.get(ModelImporter.DATASTORE_MODELS_SUBDIR_NAME));
    }

    /**
     * Performs the top level work of the model import process:
     * <ol>
     * <li>Identify the files that are of each model type.
     * <li>Add the models to the datastore and the model registry.
     * </ol>
     * import.
     */
    public void importModels(List<Path> files) {

        log.info("Importing models...");
        if (modelTypesToImport.isEmpty()) {
            modelTypesToImport.addAll(modelTypes());
            log.info("Retrieved " + modelTypesToImport.size() + " model types from database");
        }

        Map<ModelType, Map<String, Path>> modelFilesByModelType = new HashMap<>();

        // build the set of file names for each model type
        int importFileCount = 0;
        for (ModelType modelType : modelTypesToImport) {
            Map<String, Path> filenamesForModelType = findFilenamesForModelType(files, modelType);
            importFileCount += filenamesForModelType.size();
            modelFilesByModelType.put(modelType, filenamesForModelType);
        }

        if (importFileCount == 0) {
            log.info("No models to be imported, exiting");
            return;
        }

        // perform the database portion of the process
        ModelRegistry modelRegistry = unlockedRegistry();
        for (ModelType modelType : modelFilesByModelType.keySet()) {
            addModels(modelRegistry, modelType, modelFilesByModelType.get(modelType));
        }
        long unlockedModelRegistryId = mergeRegistryAndReturnUnlockedId(modelRegistry);
        log.info("Update of model registry complete");
        log.info("Importing models...done");
        log.info("Current unlocked model registry ID == " + unlockedModelRegistryId);
    }

    /**
     * Uses the regular expression for a given model type to identify the files that are of that
     * type.
     *
     * @return A Map from the version number of the new files to their names. If the model type in
     * question does not include a version number in its name, there can be only one file in the
     * Map.
     */
    public Map<String, Path> findFilenamesForModelType(List<Path> files, ModelType modelType) {

        // Get all the file names for this model type
        Map<String, Path> versionNumberFileNamesMap = new TreeMap<>();
        Pattern pattern = modelType.pattern();
        for (Path file : files) {
            String filename = file.getFileName().toString();
            Matcher matcher = pattern.matcher(filename);
            if (matcher.matches()) {
                String versionNumber;
                if (modelType.getVersionNumberGroup() > 0) {
                    versionNumber = matcher.group(modelType.getVersionNumberGroup());
                } else {
                    versionNumber = filename;
                }
                versionNumberFileNamesMap.put(versionNumber, file);
            }
        }

        // If the model type doesn't carry its own version number, then there had better not
        // be more than 1 model of that version because we don't know which is the most recent

        if (modelType.getVersionNumberGroup() <= 0 && versionNumberFileNamesMap.size() > 1) {
            throw new IllegalArgumentException(
                "Unable to import multiple models of an unversioned model type");
        }
        return versionNumberFileNamesMap;
    }

    /**
     * Add models of a given type to the registry. If there is more than one model of the type, the
     * models will be added in version number order. This will result in the most recent version
     * being committed to the registry, but all versions getting imported and copied to the
     * datastore.
     *
     * @param modelRegistry Current model registry.
     * @param modelType Type of model to be imported.
     * @param modelFilesByVersionId Map from version numbers to file names.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void addModels(ModelRegistry modelRegistry, ModelType modelType,
        Map<String, Path> modelFilesByVersionId) {

        // find or make the directory for this type of model
        Path modelDir = datastoreModelsRoot.resolve(modelType.getType()).toAbsolutePath();
        if (!Files.isDirectory(modelDir)) {
            try {
                Files.createDirectories(modelDir);
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create directory " + modelDir.toString(),
                    e);
            }
        }

        Set<String> modelVersions = new TreeSet<>(modelFilesByVersionId.keySet());
        for (String version : modelVersions) {
            createModel(modelRegistry, modelType, modelDir, modelFilesByVersionId.get(version));
            log.info(modelFilesByVersionId.size() + " models of type " + modelType.getType()
                + " added to datastore");
        }
    }

    /**
     * Creates a version of a model and adds it to the model registry. If the model file name does
     * not include a version, one will be added based on the current existing version number. If the
     * model file name does not include a timestamp, one will be added. The model, with these
     * potential additions to the file name, will then be copied to the correct directory in the
     * datastore and added to the current model registry.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void createModel(ModelRegistry modelRegistry, ModelType modelType, Path modelDir,
        Path modelFile) {

        // The update of the model registry and the move of the file must be done atomically,
        // and can only be done if the model metadata was successfully created. Thus we do this
        // in steps. First, create the model metadata, if we can't do so record the failure and
        // return.
        ModelMetadata modelMetadata = null;
        String modelFilename = modelFile.getFileName().toString();
        try {
            ModelMetadata currentModelRegistryMetadata = modelRegistry
                .getMetadataForType(modelType);
            modelMetadata = modelMetadata(modelType, modelFilename, modelDescription,
                currentModelRegistryMetadata);
            modelMetadata.setDataReceiptTaskId(dataReceiptTaskId);
        } catch (Exception e) {
            log.error("Unable to create model metadata for file " + modelFile);
            failedImports.add(dataImportPath.relativize(modelFile));
            return;
        }

        // Next we move the file, if we can't do so record the failure and return.
        Path destinationFile = modelDir.resolve(modelMetadata.getDatastoreFileName());
        try {
            move(modelFile, destinationFile);
        } catch (Exception e) {
            log.error("Unable to import file " + modelFile + " into datastore");
            failedImports.add(dataImportPath.relativize(modelFile));
            return;
        }

        // If all that worked, then we can update the model registry
        persistModelMetadata(modelMetadata);
        modelRegistry.updateModelMetadata(modelMetadata);
        log.info("Imported file " + modelFile + " to models directory as "
            + modelMetadata.getDatastoreFileName() + " of type " + modelType.getType());
        successfulImports.add(datastoreRoot.relativize(destinationFile));
    }

    // The DataFileManager method is broken out in this fashion to facilitate testing.
    public void move(Path src, Path dest) throws IOException {
        FileUtil.CopyType.MOVE.copy(src, dest);
    }

    // The ModelMetadata constructor is broken out in this fashion to facilitate testing.
    protected ModelMetadata modelMetadata(ModelType modelType, String modelName,
        String modelDescription, ModelMetadata currentRegistryMetadata) {
        return new ModelMetadata(modelType, modelName, modelDescription, currentRegistryMetadata);
    }

    public Set<ModelType> getModelTypesToImport() {
        return modelTypesToImport;
    }

    public void setModelTypesToImport(Collection<ModelType> modelTypes) {
        modelTypesToImport.clear();
        modelTypesToImport.addAll(modelTypes);
    }

    public long getDataReceiptTaskId() {
        return dataReceiptTaskId;
    }

    public void setDataReceiptTaskId(long dataReceiptTaskId) {
        this.dataReceiptTaskId = dataReceiptTaskId;
    }

    public List<Path> getFailedImports() {
        return failedImports;
    }

    public List<Path> getSuccessfulImports() {
        return successfulImports;
    }

    public ModelRegistry unlockedRegistry() {
        return (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> modelCrud.retrieveUnlockedRegistry());
    }

    public void persistModelMetadata(ModelMetadata modelMetadata) {
        DatabaseTransactionFactory.performTransaction(() -> {
            modelCrud.persist(modelMetadata);
            return null;
        });
    }

    public long mergeRegistryAndReturnUnlockedId(ModelRegistry modelRegistry) {
        return (long) DatabaseTransactionFactory.performTransaction(() -> {
            modelCrud.merge(modelRegistry);
            return modelCrud.retrieveUnlockedRegistryId();
        });
    }

    @SuppressWarnings("unchecked")
    public List<ModelType> modelTypes() {
        return (List<ModelType>) DatabaseTransactionFactory
            .performTransaction(() -> modelCrud.retrieveAllModelTypes());
    }

}
