package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Unit tests for {@link DatastoreDirectoryDataReceiptDefinition} class.
 *
 * @author PT
 */
public class DatastoreDirectoryDataReceiptDefinitionTest {

    private Path testDirectory;
    private Path dataImporterPath;
    private Path datastoreRootPath;
    private DatastoreDirectoryDataReceiptDefinition dataReceiptDefinition;
    private ModelType modelType1, modelType2, modelType3;
    private ManifestCrud manifestCrud;
    private ModelCrud modelCrud;
    private Path dataFile1, dataFile2, dataFile3;
    private Path modelFile1, modelFile2, modelFile3, modelFile4;
    private DatastoreWalker datastoreWalker;
    private ModelImporter modelImporter;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR.property(), directoryRule, "datastore");

    public ZiggyPropertyRule pipelineRootDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.RESULTS_DIR.property(), directoryRule, "pipeline-results");

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootDirPropertyRule)
        .around(pipelineRootDirPropertyRule);

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR.property(), DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() throws IOException {

        // Construct the necessary directories.
        testDirectory = directoryRule.directory();
        dataImporterPath = testDirectory.resolve("data-import").toAbsolutePath();
        dataImporterPath.toFile().mkdirs();
        datastoreRootPath = testDirectory.resolve("datastore").toAbsolutePath();
        datastoreRootPath.toFile().mkdirs();

        // construct the files for import
        constructFilesForImport();

        // Construct a Spy of the definition instance.
        dataReceiptDefinition = Mockito.spy(DatastoreDirectoryDataReceiptDefinition.class);
        dataReceiptDefinition.setDataImportDirectory(dataImporterPath);
        PipelineTask pipelineTask = new PipelineTask();
        pipelineTask.setId(1L);
        PipelineInstance pipelineInstance = new PipelineInstance();
        pipelineInstance.setId(2L);
        pipelineTask.setPipelineInstance(pipelineInstance);
        dataReceiptDefinition.setPipelineTask(pipelineTask);
        setUpModelTypes();
        manifestCrud = Mockito.mock(ManifestCrud.class);
        Mockito.doReturn(manifestCrud).when(dataReceiptDefinition).manifestCrud();
        Mockito.doReturn(List.of(modelType1, modelType2, modelType3))
            .when(dataReceiptDefinition)
            .modelTypes();
        datastoreWalker = new DatastoreWalker(DatastoreTestUtils.regexpsByName(),
            DatastoreTestUtils.datastoreNodesByFullPath());
        Mockito.doReturn(datastoreWalker).when(dataReceiptDefinition).datastoreWalker();

        modelCrud = Mockito.mock(ModelCrud.class);
        Mockito.doReturn(modelCrud).when(dataReceiptDefinition).modelCrud();

        modelImporter = Mockito.spy(new ModelImporter(dataImporterPath, "importerForTest"));
        Mockito.doReturn(modelImporter).when(dataReceiptDefinition).modelImporter();
        Mockito.doReturn(Mockito.mock(AlertService.class))
            .when(dataReceiptDefinition)
            .alertService();
        Mockito.doNothing().when(dataReceiptDefinition).updateModelRegistryForPipelineInstance();

        PipelineInstanceCrud pipelineInstanceCrud = Mockito.mock(PipelineInstanceCrud.class);
        Mockito.doReturn(pipelineInstanceCrud).when(dataReceiptDefinition).pipelineInstanceCrud();
        Mockito.when(pipelineInstanceCrud.retrieve(ArgumentMatchers.anyLong()))
            .thenReturn(pipelineInstance);
    }

    @Test
    public void testIsConformingDirectory() {
        assertTrue(dataReceiptDefinition.isConformingDelivery());
        Path manifestDir = Paths
            .get(ZiggyConfiguration.getInstance().getString(PropertyName.RESULTS_DIR.property()))
            .resolve("logs")
            .resolve("manifests");
        assertTrue(Files.isDirectory(manifestDir));
        assertTrue(Files
            .isRegularFile(manifestDir.resolve("datastore-directory-definition-manifest.xml")));
        assertTrue(Files
            .isRegularFile(manifestDir.resolve("datastore-directory-definition-manifest-ack.xml")));
    }

    /** Tests that isConformingDelivery is false if there is no manifest. */
    @Test
    public void testMissingManifest() throws IOException {
        Files.delete(dataImporterPath.resolve("datastore-directory-definition-manifest.xml"));
        assertFalse(dataReceiptDefinition.isConformingDelivery());
    }

    /** Tests that isConformingDelivery is false if the acknowledgement has invalid status. */
    @Test
    public void testAckInvalid() {
        Mockito.doReturn(false).when(dataReceiptDefinition).acknowledgementTransferStatus();
        assertFalse(dataReceiptDefinition.isConformingDelivery());
    }

    /**
     * Tests that isConformingDelivery is false if there are files in the directory that are not in
     * the manifest.
     */
    @Test
    public void testFilesNotInManifest() throws IOException {
        Files.createFile(dataImporterPath.resolve("foo.txt"));
        assertFalse(dataReceiptDefinition.isConformingDelivery());
    }

    /** Tests that isConformingDelivery is false if the dataset ID has already been used. */
    @Test
    public void testManifestIdInvalid() {
        Mockito.doReturn(true).when(manifestCrud).datasetIdExists(1L);
        assertFalse(dataReceiptDefinition.isConformingDelivery());
    }

    /** Tests that isConformingFile performs as expected. */
    @Test
    public void testIsConformingFile() {

        // Files that conform to the design.
        assertTrue(dataReceiptDefinition.isConformingFile(dataFile1));
        assertTrue(dataReceiptDefinition.isConformingFile(dataFile2));
        assertTrue(dataReceiptDefinition.isConformingFile(dataFile3));
        assertTrue(dataReceiptDefinition.isConformingFile(modelFile1));
        assertTrue(dataReceiptDefinition.isConformingFile(modelFile2));
        assertTrue(dataReceiptDefinition.isConformingFile(modelFile3));
        assertTrue(dataReceiptDefinition.isConformingFile(modelFile4));

        // Files that do not conform to the design.
        assertFalse(dataReceiptDefinition.isConformingFile(
            dataImporterPath.resolve("tess2020321141517-12345_024-geometry.xml")));
        assertFalse(dataReceiptDefinition.isConformingFile(
            dataImporterPath.resolve("models").resolve(dataImporterPath.relativize(dataFile1))));
    }

    /** Tests that use of a relative path for the import directory throws exception. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetDataImportDirectoryRelativePath() {
        dataReceiptDefinition.setDataImportDirectory(testDirectory);
    }

    /** Tests that use of a relative path in isConformingFile throws exception. */
    @Test(expected = IllegalArgumentException.class)
    public void testIsConformingFileRelativePath() {
        dataReceiptDefinition.isConformingFile(testDirectory);
    }

    /** Tests that filesForImport finds all files that are to be imported. */
    @Test
    public void testFilesForImport() throws IOException {

        // Delete the manifest to emulate the behavior of the methods of DataReceiptDefinition
        // that are called in the pipeline module prior to the filesForImport() call.
        Files.delete(dataImporterPath.resolve("datastore-directory-definition-manifest.xml"));
        List<Path> filesForImport = dataReceiptDefinition.filesForImport();
        assertTrue(filesForImport.contains(dataFile1));
        assertTrue(filesForImport.contains(dataFile2));
        assertTrue(filesForImport.contains(dataFile3));
        assertTrue(filesForImport.contains(modelFile1));
        assertTrue(filesForImport.contains(modelFile2));
        assertTrue(filesForImport.contains(modelFile3));
        assertTrue(filesForImport.contains(modelFile4));
        assertEquals(7, filesForImport.size());
    }

    /** Tests that dataFilesForImport finds all data files for import. */
    @Test
    public void testDataFilesForImport() throws IOException {

        // Delete the manifest to emulate the behavior of the methods of DataReceiptDefinition
        // that are called in the pipeline module prior to the filesForImport() call.
        Files.delete(dataImporterPath.resolve("datastore-directory-definition-manifest.xml"));
        List<Path> filesForImport = dataReceiptDefinition.dataFilesForImport();
        assertTrue(filesForImport.contains(dataFile1));
        assertTrue(filesForImport.contains(dataFile2));
        assertTrue(filesForImport.contains(dataFile3));
        assertEquals(3, filesForImport.size());
    }

    /** Tests that modelFilesForImport finds all model files for import. */
    @Test
    public void testModelFilesForImport() throws IOException {

        // Delete the manifest to emulate the behavior of the methods of DataReceiptDefinition
        // that are called in the pipeline module prior to the filesForImport() call.
        Files.delete(dataImporterPath.resolve("datastore-directory-definition-manifest.xml"));
        List<Path> filesForImport = dataReceiptDefinition.modelFilesForImport();
        assertTrue(filesForImport.contains(modelFile1));
        assertTrue(filesForImport.contains(modelFile2));
        assertTrue(filesForImport.contains(modelFile3));
        assertTrue(filesForImport.contains(modelFile4));
        assertEquals(4, filesForImport.size());
    }

    /** Tests the actual import of files. */
    @Test
    public void testImportFiles() {
        ModelRegistry registry = new ModelRegistry();
        Mockito.doReturn(registry).when(modelImporter).unlockedRegistry();
        Mockito.doNothing()
            .when(modelImporter)
            .persistModelMetadata(ArgumentMatchers.any(ModelMetadata.class));
        Mockito.doReturn(1L)
            .when(modelImporter)
            .mergeRegistryAndReturnUnlockedId(ArgumentMatchers.any(ModelRegistry.class));
        Mockito.when(modelCrud.retrieveCurrentRegistry()).thenReturn(registry);
        assertTrue(dataReceiptDefinition.isConformingDelivery());

        // All of the original files should still be in the data import directory.
        assertTrue(Files.exists(dataFile1));
        assertTrue(Files.exists(dataFile2));
        assertTrue(Files.exists(dataFile3));
        assertTrue(Files.exists(modelFile1));
        assertTrue(Files.exists(modelFile2));
        assertTrue(Files.exists(modelFile3));
        assertTrue(Files.exists(modelFile4));

        dataReceiptDefinition.importFiles();

        // Data files should be in the datastore, in directories that match the data import
        // directories but with the datastore as root rather than data import.
        assertTrue(Files.exists(datastoreRootPath.resolve(dataImporterPath.relativize(dataFile1))));
        assertTrue(Files.exists(datastoreRootPath.resolve(dataImporterPath.relativize(dataFile2))));
        assertTrue(Files.exists(datastoreRootPath.resolve(dataImporterPath.relativize(dataFile3))));

        // There should be a datastore models directory and 3 subdirs unter that.
        assertTrue(Files.isDirectory(datastoreRootPath.resolve("models")));
        assertTrue(Files.isDirectory(datastoreRootPath.resolve("models").resolve("geometry")));
        assertTrue(Files.isDirectory(datastoreRootPath.resolve("models").resolve("calibration")));
        assertTrue(Files.isDirectory(datastoreRootPath.resolve("models").resolve("ravenswood")));

        assertNotNull(registry.getModels());
        registry.populateXmlFields();

        // The geometry model should be imported to the geometry directory with no name change.
        ModelMetadata metadata = registry.getModels().get(modelType1);
        assertEquals("tess2020321141517-12345_025-geometry.xml", metadata.getDatastoreFileName());
        assertEquals("tess2020321141517-12345_025-geometry.xml", metadata.getOriginalFileName());
        assertTrue(Files.isRegularFile(datastoreRootPath.resolve("models")
            .resolve(modelType1.getType())
            .resolve(metadata.getDatastoreFileName())));

        // The calibration model should be in the right place with a different name.
        metadata = registry.getModels().get(modelType2);
        assertEquals(modelFile3.getFileName().toString(), metadata.getOriginalFileName());
        assertNotEquals(metadata.getOriginalFileName(), metadata.getDatastoreFileName());
        assertTrue(Files.isRegularFile(datastoreRootPath.resolve("models")
            .resolve(modelType2.getType())
            .resolve(metadata.getDatastoreFileName())));

        // The "ravenswood" model should be in the right place with a different name.
        metadata = registry.getModels().get(modelType3);
        assertEquals(modelFile4.getFileName().toString(), metadata.getOriginalFileName());
        assertNotEquals(metadata.getOriginalFileName(), metadata.getDatastoreFileName());
        assertTrue(Files.isRegularFile(datastoreRootPath.resolve("models")
            .resolve(modelType3.getType())
            .resolve(metadata.getDatastoreFileName())));

        assertEquals(3, registry.getModels().size());

        // None of the original files should still be in the data import directory.
        assertFalse(Files.exists(dataFile1));
        assertFalse(Files.exists(dataFile2));
        assertFalse(Files.exists(dataFile3));
        assertFalse(Files.exists(modelFile1));
        assertFalse(Files.exists(modelFile2));
        assertFalse(Files.exists(modelFile3));
        assertFalse(Files.exists(modelFile4));
    }

    /**
     * Creates test files for import in the data receipt directory
     */
    private void constructFilesForImport() throws IOException {

        Path sample1 = dataImporterPath.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("1:1:A.nc");
        Files.createDirectories(sample1.getParent());
        Files.createFile(sample1);
        dataFile1 = sample1;

        sample1 = dataImporterPath.resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A")
            .resolve("1:1:A.nc");
        Files.createDirectories(sample1.getParent());
        Files.createFile(sample1);
        dataFile2 = sample1;

        sample1 = dataImporterPath.resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B")
            .resolve("1:1:B.nc");
        Files.createDirectories(sample1.getParent());
        Files.createFile(sample1);
        dataFile3 = sample1;

        // Create model files
        setUpModelsForImport();

        // Create a manifest in the data receipt directory.
        Manifest manifest = Manifest.generateManifest(dataImporterPath, 1);
        manifest.setName("datastore-directory-definition-manifest.xml");
        if (manifest.getFileCount() > 0) {
            manifest.write(dataImporterPath);
        }
    }

    private void setUpModelTypes() {
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
    }

    private void setUpModelsForImport() throws IOException {
        Path modelImportPath = dataImporterPath.resolve("models");
        Files.createDirectories(modelImportPath);
        // create the new files to be imported
        modelFile1 = modelImportPath.resolve("tess2020321141517-12345_024-geometry.xml");
        Files.createFile(modelFile1);
        modelFile2 = modelImportPath.resolve("tess2020321141517-12345_025-geometry.xml");
        Files.createFile(modelFile2);
        modelFile3 = modelImportPath.resolve("calibration-4.12.19.h5");
        Files.createFile(modelFile3);
        modelFile4 = modelImportPath.resolve("simple-text.h5");
        Files.createFile(modelFile4);
    }
}
