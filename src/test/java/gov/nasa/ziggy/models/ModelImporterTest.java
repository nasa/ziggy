package gov.nasa.ziggy.models;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class ModelImporterTest {

    private ModelType modelType1;
    private ModelType modelType2;
    private ModelType modelType3;
    private File datastoreRoot;
    private File modelImportDirectory;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyDatastorePropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, directoryRule, "datastore");

    @Before
    public void setup() throws IOException {

        datastoreRoot = new File(ziggyDatastorePropertyRule.getProperty());
        // Set up the model type 1 to have a model ID in its name, which is a simple integer,
        // and a timestamp in its name
        modelType1 = new ModelType();
        modelType1.setFileNameRegex("tess([0-9]{13})-([0-9]{5})_([0-9]{3})-geometry.xml");
        modelType1.setType("geometry");
        modelType1.setVersionNumberGroup(3);
        modelType1.setTimestampGroup(1);
        modelType1.setSemanticVersionNumber(false);

        // Set up the model type 2 to have a semantic model ID in its name but no timestamp
        modelType2 = new ModelType();
        modelType2.setFileNameRegex("calibration-([0-9]+\\.[0-9]+\\.[0-9]+).h5");
        modelType2.setTimestampGroup(-1);
        modelType2.setType("calibration");
        modelType2.setVersionNumberGroup(1);
        modelType2.setSemanticVersionNumber(true);

        // Set up the model type 3 to have neither ID nor timestamp
        modelType3 = new ModelType();
        modelType3.setFileNameRegex("simple-text.h5");
        modelType3.setType("ravenswood");
        modelType3.setTimestampGroup(-1);
        modelType3.setVersionNumberGroup(-1);

        // Create metadata
        String filename1 = "tess2020321141516-12345_022-geometry.xml";
        ModelMetadata modelMetadata1 = new ModelMetadata(modelType1, filename1, "desc", null);
        String filename2 = "calibration-4.12.9.h5";
        ModelMetadata modelMetadata2 = new ModelMetadata(modelType2, filename2, "blabla", null);
        String filename3 = "simple-text.h5";
        ModelMetadata modelMetadata3 = new ModelMetadata(modelType3, filename3, "zinfandel", null);

        // Initialize the datastore
        modelImportDirectory = directoryRule.directory().resolve("modelImportDirectory").toFile();
        modelImportDirectory.mkdirs();

        // Create the database objects
        DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            modelCrud.create(modelType1);
            modelCrud.create(modelType2);
            modelCrud.create(modelType3);
            modelCrud.create(modelMetadata1);
            modelCrud.create(modelMetadata2);
            modelCrud.create(modelMetadata3);
            ModelRegistry modelRegistry = modelCrud.retrieveUnlockedRegistry();
            modelRegistry.updateModelMetadata(modelMetadata1);
            modelRegistry.updateModelMetadata(modelMetadata2);
            modelRegistry.updateModelMetadata(modelMetadata3);
            return null;
        });

        // create the new files to be imported
        new File(modelImportDirectory, "tess2020321141517-12345_024-geometry.xml").createNewFile();
        new File(modelImportDirectory, "tess2020321141517-12345_025-geometry.xml").createNewFile();
        new File(modelImportDirectory, "calibration-4.12.19.h5").createNewFile();
        new File(modelImportDirectory, "simple-text.h5").createNewFile();
    }

    @Test
    public void testImportWithCurrentRegistryUnlocked() {

        // Import the models
        ModelImporter modelImporter = new ModelImporter(modelImportDirectory.getAbsolutePath(),
            "unit test");
        DatabaseTransactionFactory.performTransaction(() -> {
            modelImporter.importModels(filenamesInDirectory());
            return null;
        });

        // Retrieve the current model registry
        ModelRegistry modelRegistry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                ModelCrud modelCrud = new ModelCrud();
                return modelCrud.retrieveCurrentRegistry();
            });

        // Check the registry's properties
        assertFalse(modelRegistry.isLocked());
        assertEquals(1L, modelRegistry.getId());
        Map<ModelType, ModelMetadata> models = modelRegistry.getModels();
        assertEquals(3, models.size());

        // Check that all the models are removed from the import dir
        assertEquals(0, modelImportDirectory.listFiles().length);

        // Check that there are no files logged with the importer as failed
        assertEquals(0, modelImporter.getFailedImports().size());

        // Extract the listing of successful imports
        List<Path> successfulImports = modelImporter.getSuccessfulImports();
        assertEquals(4, successfulImports.size());
        List<String> importFilenames = successfulImports.stream()
            .map(Path::toString)
            .collect(Collectors.toList());

        // Check the modelType1 models in the registry and the datastore
        ModelMetadata m = models.get(modelType1);
        assertEquals("025", m.getModelRevision());
        assertEquals("tess2020321141517-12345_025-geometry.xml", m.getDatastoreFileName());
        assertEquals(modelType1, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        File geometryModelsDirectory = new File(datastoreRoot, "models/geometry");
        File[] geometryModels = geometryModelsDirectory.listFiles();
        assertEquals(2, geometryModels.length);
        List<String> geometryModelFileNames = new ArrayList<>();
        geometryModelFileNames.add(geometryModels[0].getName());
        geometryModelFileNames.add(geometryModels[1].getName());
        assertTrue(geometryModelFileNames.contains("tess2020321141517-12345_025-geometry.xml"));
        assertTrue(geometryModelFileNames.contains("tess2020321141517-12345_024-geometry.xml"));
        assertTrue(
            importFilenames.contains("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertTrue(
            importFilenames.contains("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        // Check the modelType2 models in the registry and the datastore
        m = models.get(modelType2);
        assertEquals("4.12.19", m.getModelRevision());
        assertEquals(modelType2, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        String modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "calibration", modelFilename).toString()));
        String truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".calibration-4.12.19.h5", truncatedModelFilename);
        File calibrationModelsDirectory = new File(datastoreRoot, "models/calibration");
        File[] calibrationModels = calibrationModelsDirectory.listFiles();
        assertEquals(1, calibrationModels.length);
        assertEquals(modelFilename, calibrationModels[0].getName());

        // Check the modelType3 models in the registry and the datastore
        m = models.get(modelType3);
        assertEquals("2", m.getModelRevision());
        assertEquals("unit test", m.getModelDescription());
        assertEquals(modelType3, m.getModelType());
        modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "ravenswood", modelFilename).toString()));
        truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".0002-simple-text.h5", truncatedModelFilename);
        File ravenswoodModelsDirectory = new File(datastoreRoot, "models/ravenswood");
        File[] ravenswoodModels = ravenswoodModelsDirectory.listFiles();
        assertEquals(1, ravenswoodModels.length);
        assertEquals(modelFilename, ravenswoodModels[0].getName());

    }

    @Test
    public void testImportWithCurrentRegistryLocked() {

        // lock the current registry
        DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            modelCrud.lockCurrentRegistry();
            return null;
        });

        // Import the models
        ModelImporter modelImporter = new ModelImporter(modelImportDirectory.getAbsolutePath(),
            "unit test");
        DatabaseTransactionFactory.performTransaction(() -> {
            modelImporter.importModels(filenamesInDirectory());
            return null;
        });

        // Retrieve the current model registry
        ModelRegistry modelRegistry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                ModelCrud modelCrud = new ModelCrud();
                return modelCrud.retrieveCurrentRegistry();
            });

        // Check the registry's properties
        assertFalse(modelRegistry.isLocked());
        assertEquals(2L, modelRegistry.getId());
        Map<ModelType, ModelMetadata> models = modelRegistry.getModels();
        assertEquals(3, models.size());

        // Check that all the models are removed from the import dir
        assertEquals(0, modelImportDirectory.listFiles().length);

        // Check that there are no files logged with the importer as failed
        assertEquals(0, modelImporter.getFailedImports().size());

        // Extract the listing of successful imports
        List<Path> successfulImports = modelImporter.getSuccessfulImports();
        assertEquals(4, successfulImports.size());
        List<String> importFilenames = successfulImports.stream()
            .map(Path::toString)
            .collect(Collectors.toList());

        // Check the modelType1 models in the registry and the datastore
        ModelMetadata m = models.get(modelType1);
        assertEquals("025", m.getModelRevision());
        assertEquals("tess2020321141517-12345_025-geometry.xml", m.getDatastoreFileName());
        assertEquals(modelType1, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        File geometryModelsDirectory = new File(datastoreRoot, "models/geometry");
        File[] geometryModels = geometryModelsDirectory.listFiles();
        assertEquals(2, geometryModels.length);
        List<String> geometryModelFileNames = new ArrayList<>();
        geometryModelFileNames.add(geometryModels[0].getName());
        geometryModelFileNames.add(geometryModels[1].getName());
        assertTrue(geometryModelFileNames.contains("tess2020321141517-12345_025-geometry.xml"));
        assertTrue(geometryModelFileNames.contains("tess2020321141517-12345_024-geometry.xml"));
        assertTrue(
            importFilenames.contains("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertTrue(
            importFilenames.contains("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        // Check the modelType2 models in the registry and the datastore
        m = models.get(modelType2);
        assertEquals("4.12.19", m.getModelRevision());
        assertEquals(modelType2, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        String modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "calibration", modelFilename).toString()));
        String truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".calibration-4.12.19.h5", truncatedModelFilename);
        File calibrationModelsDirectory = new File(datastoreRoot, "models/calibration");
        File[] calibrationModels = calibrationModelsDirectory.listFiles();
        assertEquals(1, calibrationModels.length);
        assertEquals(modelFilename, calibrationModels[0].getName());

        // Check the modelType3 models in the registry and the datastore
        m = models.get(modelType3);
        assertEquals("2", m.getModelRevision());
        assertEquals("unit test", m.getModelDescription());
        assertEquals(modelType3, m.getModelType());
        modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "ravenswood", modelFilename).toString()));
        truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".0002-simple-text.h5", truncatedModelFilename);
        File ravenswoodModelsDirectory = new File(datastoreRoot, "models/ravenswood");
        File[] ravenswoodModels = ravenswoodModelsDirectory.listFiles();
        assertEquals(1, ravenswoodModels.length);
        assertEquals(modelFilename, ravenswoodModels[0].getName());

    }

    @Test
    public void testImportWithFailures() throws IOException {

        // For this exercise we need a spy for the importer
        ModelImporter importer = new ModelImporter(modelImportDirectory.getAbsolutePath(),
            "unit test");
        final ModelImporter modelImporter = Mockito.spy(importer);
        Path destFileToFlunk = Paths.get(datastoreRoot.toString(), "models", "geometry",
            "tess2020321141517-12345_025-geometry.xml");
        Path srcFileToFlunk = Paths.get(modelImportDirectory.toString(),
            "tess2020321141517-12345_025-geometry.xml");
        Mockito.doThrow(IOException.class)
            .when(modelImporter)
            .moveOrSymlink(srcFileToFlunk.toAbsolutePath(), destFileToFlunk);

        // Perform the import
        DatabaseTransactionFactory.performTransaction(() -> {
            modelImporter.importModels(filenamesInDirectory());
            return null;
        });

        // Retrieve the current model registry
        ModelRegistry modelRegistry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                ModelCrud modelCrud = new ModelCrud();
                return modelCrud.retrieveCurrentRegistry();
            });

        // Check the registry's properties
        assertFalse(modelRegistry.isLocked());
        assertEquals(1L, modelRegistry.getId());
        Map<ModelType, ModelMetadata> models = modelRegistry.getModels();
        assertEquals(3, models.size());

        // Check that there is one file logged with the importer as failed
        List<Path> failedImports = modelImporter.getFailedImports();
        assertEquals(1, failedImports.size());
        assertEquals("tess2020321141517-12345_025-geometry.xml", failedImports.get(0).toString());

        // Check that the failed import is still in the source directory
        File[] remainingFiles = modelImportDirectory.listFiles();
        assertEquals(1, remainingFiles.length);
        assertEquals("tess2020321141517-12345_025-geometry.xml", remainingFiles[0].getName());

        // Extract the listing of successful imports
        List<Path> successfulImports = modelImporter.getSuccessfulImports();
        assertEquals(3, successfulImports.size());
        List<String> importFilenames = successfulImports.stream()
            .map(Path::toString)
            .collect(Collectors.toList());

        // Check the modelType1 models in the registry and the datastore
        ModelMetadata m = models.get(modelType1);
        assertEquals("024", m.getModelRevision());
        assertEquals("tess2020321141517-12345_024-geometry.xml", m.getDatastoreFileName());
        assertEquals(modelType1, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        File geometryModelsDirectory = new File(datastoreRoot, "models/geometry");
        File[] geometryModels = geometryModelsDirectory.listFiles();
        assertEquals(1, geometryModels.length);
        List<String> geometryModelFileNames = new ArrayList<>();
        geometryModelFileNames.add(geometryModels[0].getName());
        assertTrue(geometryModelFileNames.contains("tess2020321141517-12345_024-geometry.xml"));
        assertTrue(
            importFilenames.contains("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        // Check the modelType2 models in the registry and the datastore
        m = models.get(modelType2);
        assertEquals("4.12.19", m.getModelRevision());
        assertEquals(modelType2, m.getModelType());
        assertEquals("unit test", m.getModelDescription());
        String modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "calibration", modelFilename).toString()));
        String truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".calibration-4.12.19.h5", truncatedModelFilename);
        File calibrationModelsDirectory = new File(datastoreRoot, "models/calibration");
        File[] calibrationModels = calibrationModelsDirectory.listFiles();
        assertEquals(1, calibrationModels.length);
        assertEquals(modelFilename, calibrationModels[0].getName());

        // Check the modelType3 models in the registry and the datastore
        m = models.get(modelType3);
        assertEquals("2", m.getModelRevision());
        assertEquals("unit test", m.getModelDescription());
        assertEquals(modelType3, m.getModelType());
        modelFilename = m.getDatastoreFileName();
        assertTrue(
            importFilenames.contains(Paths.get("models", "ravenswood", modelFilename).toString()));
        truncatedModelFilename = modelFilename.substring(10);
        assertEquals(".0002-simple-text.h5", truncatedModelFilename);
        File ravenswoodModelsDirectory = new File(datastoreRoot, "models/ravenswood");
        File[] ravenswoodModels = ravenswoodModelsDirectory.listFiles();
        assertEquals(1, ravenswoodModels.length);
        assertEquals(modelFilename, ravenswoodModels[0].getName());

    }

    private List<String> filenamesInDirectory() throws IOException {
        List<String> filenamesInDirectory = new ArrayList<>();
        try (DirectoryStream<Path> stream = java.nio.file.Files
            .newDirectoryStream(modelImportDirectory.toPath())) {
            for (Path path : stream) {
                filenamesInDirectory.add(path.getFileName().toString());
            }
        }
        return filenamesInDirectory;
    }

}
