package gov.nasa.ziggy.models;

import java.io.File;
import java.io.IOException;
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

import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;

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

    private String directory;
    private Path datastoreRoot;
    private ModelCrud modelCrud;
    private Path modelsRoot;
    String modelDescription;
    private Set<ModelType> modelTypesToImport = new HashSet<>();
    private long dataReceiptTaskId;
    private List<Path> successfulImports = new ArrayList<>();
    private List<Path> failedImports = new ArrayList<>();

    public static final String DATASTORE_MODELS_SUBDIR_NAME = "models";

    public ModelImporter(String directory, String modelDescription) {
        this.directory = directory;
        File directoryFile = new File(directory);
        if (!directoryFile.exists() || !directoryFile.isDirectory()) {
            throw new IllegalArgumentException(
                "Argument " + directory + " is not a directory or does not exist");
        }
        this.modelDescription = modelDescription;
        datastoreRoot = DirectoryProperties.datastoreRootDir();
        modelsRoot = datastoreRoot.resolve(Paths.get(ModelImporter.DATASTORE_MODELS_SUBDIR_NAME));
    }

    /**
     * Performs the top level work of the model import process:
     * <ol>
     * <li>Identify the files in the directory that are of each model type.
     * <li>Add the models to the datastore and the model registry.
     * </ol>
     *
     * @param filenames list of all validated files in the import directory.
     * @return true if models were found that required import, false if no models were found to
     * import.
     */
    public boolean importModels(List<String> filenames) {

        log.info("Starting model imports from directory " + directory);
        final ModelCrud modelMetadataCrud = modelCrud();
        if (modelTypesToImport.isEmpty()) {
            modelTypesToImport.addAll(modelMetadataCrud.retrieveAllModelTypes());
            log.info("Retrieved " + modelTypesToImport.size() + " model types from database");
        }

        Map<ModelType, Map<String, String>> modelTypeFileNamesMap = new HashMap<>();

        // build the set of file names for each model type
        int importFileCount = 0;
        for (ModelType modelType : modelTypesToImport) {
            Map<String, String> filenamesForModelType = findFilenamesForModelType(filenames,
                modelType);
            importFileCount += filenamesForModelType.keySet().size();
            modelTypeFileNamesMap.put(modelType, filenamesForModelType);
        }

        if (importFileCount == 0) {
            log.info("No models to be imported, exiting");
            return false;
        }

        // perform the database portion of the process
        ModelRegistry modelRegistry = modelCrud().retrieveUnlockedRegistry();
        for (ModelType modelType : modelTypeFileNamesMap.keySet()) {
            addModels(modelRegistry, modelType, modelTypeFileNamesMap.get(modelType));
        }
        modelCrud().createOrUpdate(modelRegistry);
        log.info("Update of model registry complete");
        long unlockedModelRegistryId = modelCrud().retrieveUnlockedRegistryId();
        log.info("Current unlocked model registry ID == " + unlockedModelRegistryId);
        return true;
    }

    /**
     * Uses the regular expression for a given model type to identify the files that are of that
     * type.
     *
     * @param filenames List of files in the import directory.
     * @param modelType ModelType instance to be used in the search.
     * @return A Map from the version number of the new files to their names. If the model type in
     * question does not include a version number in its name, there can be only one file in the
     * Map.
     */
    public Map<String, String> findFilenamesForModelType(List<String> filenames,
        ModelType modelType) {

        // Get all the file names for this model type
        Map<String, String> versionNumberFileNamesMap = new TreeMap<>();
        Pattern pattern = modelType.pattern();
        for (String filename : filenames) {
            Matcher matcher = pattern.matcher(filename);
            if (matcher.matches()) {
                String versionNumber;
                if (modelType.getVersionNumberGroup() > 0) {
                    versionNumber = matcher.group(modelType.getVersionNumberGroup());
                } else {
                    versionNumber = filename;
                }
                versionNumberFileNamesMap.put(versionNumber, filename);
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
     * @param versionNumberFileNamesMap Map from version numbers to file names.
     */
    private void addModels(ModelRegistry modelRegistry, ModelType modelType,
        Map<String, String> versionNumberFileNamesMap) {

        // find or make the directory for this type of model
        Path modelDir = modelsRoot.resolve(modelType.getType());
        if (!Files.isDirectory(modelDir)) {
            try {
                Files.createDirectories(modelDir);
            } catch (IOException e) {
                throw new PipelineException(
                    "Unable to create models directory " + modelDir.toString(), e);
            }
        }

        Set<String> modelVersions = new TreeSet<>(versionNumberFileNamesMap.keySet());
        for (String version : modelVersions) {
            createModel(modelRegistry, modelType, modelDir, versionNumberFileNamesMap.get(version));
            log.info(versionNumberFileNamesMap.size() + " models of type " + modelType.getType()
                + " added to datastore");
        }
    }

    /**
     * Creates a version of a model and adds it to the model registry. If the model file name does
     * not include a version, one will be added based on the current existing version number. If the
     * model file name does not include a timestamp, one will be added. The model, with these
     * potential additions to the file name, will then be copied to the correct directory in the
     * datastore and added to the current model registry.
     *
     * @param modelRegistry Current version of the registy.
     * @param modelType Type of model to add.
     * @param modelDir Directory for models of this type in the datastore.
     * @param modelName File name for the model in the import directory.
     */
    private void createModel(ModelRegistry modelRegistry, ModelType modelType, Path modelDir,
        String modelName) {

        // The update of the model registry and the move of the file must be done atomically,
        // and can only be done if the model metadata was successfully created. Thus we do this
        // in steps. First, create the model metadata, if we can't do so record the failure and
        // return.
        ModelMetadata modelMetadata = null;
        try {
            ModelMetadata currentModelRegistryMetadata = modelRegistry
                .getMetadataForType(modelType);
            modelMetadata = modelMetadata(modelType, modelName, modelDescription,
                currentModelRegistryMetadata);
            modelMetadata.setDataReceiptTaskId(dataReceiptTaskId);
        } catch (Exception e) {
            log.error("Unable to create model metadata for file " + modelName);
            failedImports.add(Paths.get(modelName));
            return;
        }

        // Next we move the file, if we can't do so record the failure and return.
        Path sourceFile = Paths.get(directory, modelName);
        Path destinationFile = modelDir.resolve(modelMetadata.getDatastoreFileName());
        try {
            moveOrSymlink(sourceFile, destinationFile);
        } catch (Exception e) {
            log.error("Unable to import file " + modelName + " into datastore");
            failedImports.add(Paths.get(modelName));
            return;
        }

        // If all that worked, then we can update the model registry
        modelCrud().create(modelMetadata);
        modelRegistry.updateModelMetadata(modelMetadata);
        log.info("Imported file " + modelName + " to models directory as "
            + modelMetadata.getDatastoreFileName() + " of type " + modelType.getType());
        successfulImports.add(datastoreRoot.relativize(destinationFile));
    }

    // The DataFileManager method is broken out in this fashion to facilitate testing.
    public void moveOrSymlink(Path src, Path dest) throws IOException {
        DataFileManager.moveOrSymlink(src, dest);
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

    protected ModelCrud modelCrud() {
        if (modelCrud == null) {
            modelCrud = new ModelCrud();
        }
        return modelCrud;
    }
}
