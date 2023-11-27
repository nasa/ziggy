package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.DATA_RECEIPT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.USE_SYMLINKS;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumer.DataReceiptFileType;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelMetadataTest.ModelMetadataFixedDate;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import jakarta.xml.bind.JAXBException;

/**
 * Unit tests for the {@link DataReceiptPipelineModule} class.
 *
 * @author PT
 */
public class DataReceiptPipelineModuleTest {

    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private Path dataImporterPath;
    private Path dataImporterSubdirPath;
    private Path modelImporterSubdirPath;
    private Path datastoreRootPath;
    private UnitOfWork singleUow = new UnitOfWork();
    private UnitOfWork dataSubdirUow = new UnitOfWork();
    private UnitOfWork modelSubdirUow = new UnitOfWork();
    private PipelineDefinitionNode node = new PipelineDefinitionNode();
    private ModelType modelType1, modelType2, modelType3;

    private ExecutorService execThread;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule dataReceiptDirPropertyRule = new ZiggyPropertyRule(DATA_RECEIPT_DIR,
        directoryRule, "data-import");

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, directoryRule, "datastore");

    @Rule
    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        (String) null);

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public ZiggyPropertyRule useSymlinksPropertyRule = new ZiggyPropertyRule(USE_SYMLINKS,
        (String) null);

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirPropertyRule)
        .around(datastoreRootDirPropertyRule)
        .around(resultsDirPropertyRule);

    @Before
    public void setUp() throws IOException {

        // set up model types
        setUpModelTypes();

        // Initialize the data type samples
        DataFileTestUtils.initializeDataFileTypeSamples();

        // Construct the necessary directories.
        dataImporterPath = Paths.get(dataReceiptDirPropertyRule.getProperty());
        dataImporterPath.toFile().mkdirs();
        datastoreRootPath = Paths.get(datastoreRootDirPropertyRule.getProperty());
        datastoreRootPath.toFile().mkdirs();
        dataImporterSubdirPath = dataImporterPath.resolve("sub-dir");
        dataImporterSubdirPath.toFile().mkdirs();
        dataSubdirUow.addParameter(new TypedParameter(
            UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        modelSubdirUow.addParameter(new TypedParameter(
            UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        singleUow.addParameter(new TypedParameter(
            UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        dataSubdirUow
            .addParameter(new TypedParameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME,
                "sub-dir", ZiggyDataType.ZIGGY_STRING));
        modelSubdirUow
            .addParameter(new TypedParameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME,
                "models-sub-dir", ZiggyDataType.ZIGGY_STRING));
        singleUow.addParameter(new TypedParameter(
            DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME, "", ZiggyDataType.ZIGGY_STRING));
        modelImporterSubdirPath = dataImporterPath.resolve("models-sub-dir");
        modelImporterSubdirPath.toFile().mkdirs();

        // construct the files for import
        constructFilesForImport();

        // Construct the data file type information
        node.setInputDataFileTypes(ImmutableSet.of(DataFileTestUtils.dataFileTypeSample1,
            DataFileTestUtils.dataFileTypeSample2));

        // construct the model type information
        node.setModelTypes(ImmutableSet.of(modelType1, modelType2, modelType3));

        // Create the "database objects," these are actually an assortment of mocks
        // so we can test this without needing an actual database.
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        Mockito.when(pipelineTask.getId()).thenReturn(101L);

        // Put in a mocked AlertService instance.
        AlertService.setInstance(Mockito.mock(AlertService.class));

        // Set up the executor service
        execThread = Executors.newFixedThreadPool(1);
    }

    @After
    public void shutDown() throws InterruptedException, IOException {
        Thread.interrupted();
        execThread.shutdownNow();

        AlertService.setInstance(null);
    }

    @Test
    public void testImportFromDataReceiptDir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        setUpModelsForImport(dataImporterPath);
        constructManifests();

        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);

        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.disableDirectoryCleanup();
        module.processTask();

        // Obtain the producer-consumer records and check that all the files are listed with
        // the correct producer
        assertEquals(0, module.getFailedImportsDataAccountability().size());
        Set<DatastoreProducerConsumer> producerConsumerRecords = module
            .getSuccessfulImportsDataAccountability();
        assertEquals(6, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports.containsKey("pa/20/pa-001234567-20-results.h5"));
        assertEquals(Long.valueOf(101L), successfulImports.get("pa/20/pa-001234567-20-results.h5"));
        assertTrue(successfulImports.containsKey("cal/20/cal-1-1-A-20-results.h5"));
        assertEquals(Long.valueOf(101L), successfulImports.get("cal/20/cal-1-1-A-20-results.h5"));
        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertTrue(
            successfulImports.containsKey("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertTrue(
            successfulImports.containsKey("models/calibration/2020-12-29.calibration-4.12.19.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/calibration/2020-12-29.calibration-4.12.19.h5"));

        // check that all the files made it to their destinations
        assertTrue(datastoreRootPath.resolve(Paths.get("pa", "20", "pa-001234567-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-A-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_024-geometry.xml"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_025-geometry.xml"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "calibration", "2020-12-29.calibration-4.12.19.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "ravenswood", "2020-12-29.0001-simple-text.h5"))
            .toFile()
            .exists());

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertEquals(5, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve("sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("models-sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("pdc-1-1-22-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest.xml").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest-ack.xml").toFile().exists());
        assertEquals(3, dataImporterSubdirPath.toFile().listFiles().length);
        assertTrue(dataImporterSubdirPath.resolve("pa-765432100-20-results.h5").toFile().exists());
        assertTrue(dataImporterSubdirPath.resolve("cal-1-1-B-20-results.h5").toFile().exists());
        assertTrue(
            dataImporterSubdirPath.resolve("data-importer-subdir-manifest.xml").toFile().exists());
        assertEquals(0, modelImporterSubdirPath.toFile().listFiles().length);

        // Get the manifest out of the database
        Manifest dbManifest = module.getManifest();
        assertNotNull(dbManifest);
        assertEquals(1L, dbManifest.getDatasetId());
        assertTrue(dbManifest.isAcknowledged());
        assertEquals(DataReceiptStatus.VALID, dbManifest.getStatus());
        assertEquals("data-importer-manifest.xml", dbManifest.getName());
        assertNotNull(dbManifest.getImportTime());
    }

    @Test
    public void testImportFromDataSubdir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        setUpModelsForImport(modelImporterSubdirPath);
        constructManifests();

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(dataSubdirUow);

        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.processTask();

        // Obtain the producer-consumer records and check that only the data files are listed
        assertEquals(0, module.getFailedImportsDataAccountability().size());
        Set<DatastoreProducerConsumer> producerConsumerRecords = module
            .getSuccessfulImportsDataAccountability();
        assertEquals(2, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports.containsKey("pa/20/pa-765432100-20-results.h5"));
        assertEquals(Long.valueOf(101L), successfulImports.get("pa/20/pa-765432100-20-results.h5"));
        assertTrue(successfulImports.containsKey("cal/20/cal-1-1-B-20-results.h5"));
        assertEquals(Long.valueOf(101L), successfulImports.get("cal/20/cal-1-1-B-20-results.h5"));

        // check that the data files made it to their destinations
        assertTrue(datastoreRootPath.resolve(Paths.get("pa", "20", "pa-765432100-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-B-20-results.h5"))
            .toFile()
            .exists());

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertEquals(5, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve("models-sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("pdc-1-1-22-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("pa-001234567-20-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("cal-1-1-A-20-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest.xml").toFile().exists());
        assertEquals(5, modelImporterSubdirPath.toFile().listFiles().length);
        assertTrue(modelImporterSubdirPath.resolve("tess2020321141517-12345_024-geometry.xml")
            .toFile()
            .exists());
        assertTrue(modelImporterSubdirPath.resolve("tess2020321141517-12345_025-geometry.xml")
            .toFile()
            .exists());
        assertTrue(modelImporterSubdirPath.resolve("calibration-4.12.19.h5").toFile().exists());
        assertTrue(modelImporterSubdirPath.resolve("simple-text.h5").toFile().exists());
        assertTrue(modelImporterSubdirPath.resolve("model-importer-subdir-manifest.xml")
            .toFile()
            .exists());

        // Get the manifest out of the database
        Manifest dbManifest = module.getManifest();
        assertNotNull(dbManifest);
        assertEquals(2L, dbManifest.getDatasetId());
        assertTrue(dbManifest.isAcknowledged());
        assertEquals(DataReceiptStatus.VALID, dbManifest.getStatus());
        assertEquals("data-importer-subdir-manifest.xml", dbManifest.getName());
        assertNotNull(dbManifest.getImportTime());

        // The manifest and the acknowledgement should be moved to the manifests hidden
        // directory
        Path manifestDir = DirectoryProperties.manifestsDir();
        assertTrue(Files.exists(manifestDir));
        assertTrue(Files.exists(manifestDir.resolve("data-importer-subdir-manifest.xml")));
        assertTrue(Files.exists(manifestDir.resolve("data-importer-subdir-manifest-ack.xml")));

        // The data directory should be deleted
        assertFalse(Files.exists(dataImporterSubdirPath));

        // The models directory should still be present with its same file content
        assertTrue(Files.exists(modelImporterSubdirPath));
        assertEquals(5, modelImporterSubdirPath.toFile().listFiles().length);

        // The parent directory should still have its same file content,
        // except for the data subdirectory (but with the manifests directory
        // the number of files is 6 again anyway)
        assertEquals(5, dataImporterPath.toFile().listFiles().length);
    }

    @Test
    public void testImportFromModelsSubdir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        setUpModelsForImport(modelImporterSubdirPath);
        constructManifests();

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(modelSubdirUow);
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        Mockito.when(pipelineTask.getId()).thenReturn(101L);

        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.processTask();

        // Obtain the producer-consumer records and check that only the data files are listed
        assertEquals(0, module.getFailedImportsDataAccountability().size());
        Set<DatastoreProducerConsumer> producerConsumerRecords = module
            .getSuccessfulImportsDataAccountability();
        assertEquals(4, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertTrue(
            successfulImports.containsKey("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertTrue(
            successfulImports.containsKey("models/calibration/2020-12-29.calibration-4.12.19.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/calibration/2020-12-29.calibration-4.12.19.h5"));

        // check that the model files made it to their destinations
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_024-geometry.xml"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_025-geometry.xml"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "calibration", "2020-12-29.calibration-4.12.19.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "ravenswood", "2020-12-29.0001-simple-text.h5"))
            .toFile()
            .exists());

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertEquals(5, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve("sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("pdc-1-1-22-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("pa-001234567-20-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("cal-1-1-A-20-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest.xml").toFile().exists());
        assertEquals(3, dataImporterSubdirPath.toFile().listFiles().length);
        assertTrue(dataImporterSubdirPath.resolve("pa-765432100-20-results.h5").toFile().exists());
        assertTrue(dataImporterSubdirPath.resolve("cal-1-1-B-20-results.h5").toFile().exists());
        assertTrue(
            dataImporterSubdirPath.resolve("data-importer-subdir-manifest.xml").toFile().exists());

        // Get the manifest out of the database
        Manifest dbManifest = module.getManifest();
        assertNotNull(dbManifest);
        assertEquals(3L, dbManifest.getDatasetId());
        assertTrue(dbManifest.isAcknowledged());
        assertEquals(DataReceiptStatus.VALID, dbManifest.getStatus());
        assertEquals("model-importer-subdir-manifest.xml", dbManifest.getName());
        assertNotNull(dbManifest.getImportTime());
    }

    @Test
    public void testImportWithErrors() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        setUpModelsForImport(dataImporterPath);
        constructManifests();

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        Mockito.when(pipelineTask.getId()).thenReturn(101L);

        // Generate data and model importers that will throw IOExceptions at opportune moments
        DefaultDataImporter dataImporter = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        dataImporter = Mockito.spy(dataImporter);
        Mockito.doThrow(IOException.class)
            .when(dataImporter)
            .moveOrSymlink(dataImporterPath.resolve(Paths.get("pa-001234567-20-results.h5")),
                datastoreRootPath.resolve(Paths.get("pa", "20", "pa-001234567-20-results.h5")));

        ModelImporter modelImporter = new ModelImporterForTest(dataImporterPath.toString(),
            "unit test");
        modelImporter = Mockito.spy(modelImporter);
        Path destFileToFlunk = Paths.get(datastoreRootPath.toString(), "models", "geometry",
            "tess2020321141517-12345_025-geometry.xml");
        Path srcFileToFlunk = Paths.get(dataImporterPath.toString(),
            "tess2020321141517-12345_025-geometry.xml");
        Mockito.doThrow(IOException.class)
            .when(modelImporter)
            .moveOrSymlink(srcFileToFlunk, destFileToFlunk);

        // Install the data and model importers in the pipeline module
        DataReceiptModuleForTest pipelineModule = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        final DataReceiptModuleForTest module = Mockito.spy(pipelineModule);
        Mockito.doReturn(dataImporter)
            .when(module)
            .dataImporter(ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Path.class));
        Mockito.doReturn(modelImporter)
            .when(module)
            .modelImporter(ArgumentMatchers.any(Path.class), ArgumentMatchers.any(String.class));

        // install a dummy alert service in the module
        Mockito.doReturn(Mockito.mock(AlertService.class)).when(module).alertService();

        // Perform the import
        boolean exceptionThrown = false;
        try {
            module.processTask();
        } catch (PipelineException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // Obtain the producer-consumer records and check that the expected files are listed with
        // the correct producer
        Set<DatastoreProducerConsumer> producerConsumerRecords = module
            .getSuccessfulImportsDataAccountability();
        assertEquals(4, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports.containsKey("cal/20/cal-1-1-A-20-results.h5"));
        assertEquals(Long.valueOf(101L), successfulImports.get("cal/20/cal-1-1-A-20-results.h5"));
        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertTrue(
            successfulImports.containsKey("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/ravenswood/2020-12-29.0001-simple-text.h5"));
        assertTrue(
            successfulImports.containsKey("models/calibration/2020-12-29.calibration-4.12.19.h5"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/calibration/2020-12-29.calibration-4.12.19.h5"));

        // check that the files made it to their destinations
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-A-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_024-geometry.xml"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "calibration", "2020-12-29.calibration-4.12.19.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "ravenswood", "2020-12-29.0001-simple-text.h5"))
            .toFile()
            .exists());

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertEquals(7, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve("sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("models-sub-dir").toFile().exists());
        assertTrue(dataImporterPath.resolve("pdc-1-1-22-results.h5").toFile().exists());
        assertTrue(dataImporterPath.resolve("pa-001234567-20-results.h5").toFile().exists());
        assertTrue(
            dataImporterPath.resolve("tess2020321141517-12345_025-geometry.xml").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest.xml").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest-ack.xml").toFile().exists());
        assertEquals(3, dataImporterSubdirPath.toFile().listFiles().length);
        assertTrue(dataImporterSubdirPath.resolve("pa-765432100-20-results.h5").toFile().exists());
        assertTrue(dataImporterSubdirPath.resolve("cal-1-1-B-20-results.h5").toFile().exists());
        assertTrue(
            dataImporterSubdirPath.resolve("data-importer-subdir-manifest.xml").toFile().exists());
        assertEquals(0, modelImporterSubdirPath.toFile().listFiles().length);

        // Get the manifest out of the database
        Manifest dbManifest = module.getManifest();
        assertNotNull(dbManifest);
        assertEquals(1L, dbManifest.getDatasetId());
        assertTrue(dbManifest.isAcknowledged());
        assertEquals(DataReceiptStatus.VALID, dbManifest.getStatus());
        assertEquals("data-importer-manifest.xml", dbManifest.getName());
        assertNotNull(dbManifest.getImportTime());

        // Finally, check that the expected files are in the failed imports table.
        Set<FailedImport> failedImports = module.getFailedImportsDataAccountability();
        assertEquals(2, failedImports.size());
        Map<String, Long> failedImportMap = new HashMap<>();
        for (FailedImport failedImport : failedImports) {
            failedImportMap.put(failedImport.getFilename(), failedImport.getDataReceiptTaskId());
        }
        assertTrue(failedImportMap.containsKey("pa/20/pa-001234567-20-results.h5"));
        assertEquals(Long.valueOf(101L), failedImportMap.get("pa/20/pa-001234567-20-results.h5"));
        assertTrue(failedImportMap.containsKey("tess2020321141517-12345_025-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            failedImportMap.get("tess2020321141517-12345_025-geometry.xml"));
    }

    @Test(expected = PipelineException.class)
    public void testCleanupFailOnNonEmptyDir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        setUpModelsForImport(dataImporterPath);
        constructManifests();

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        Mockito.when(pipelineTask.getId()).thenReturn(101L);

        // Perform the import
        DataReceiptPipelineModule module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.processTask();
    }

    @Test
    public void testImportOnEmptyDirectory() throws IOException {

        FileUtils.cleanDirectory(dataImporterPath.toFile());
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.disableDirectoryCleanup();
        assertTrue(module.processTask());
    }

    @Test(expected = PipelineException.class)
    public void testMissingManifest() {
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.disableDirectoryCleanup();
        module.processTask();
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

    private Set<String> constructFilesForImport() throws IOException {

        Set<String> filenames = new HashSet<>();
        // create a couple of files in the DatastoreIdSample1 pattern
        File sample1 = new File(dataImporterPath.toFile(), "pa-001234567-20-results.h5");
        File sample2 = new File(dataImporterSubdirPath.toFile(), "pa-765432100-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a couple of files in the DatastoreIdSample2 pattern
        sample1 = new File(dataImporterPath.toFile(), "cal-1-1-A-20-results.h5");
        sample2 = new File(dataImporterSubdirPath.toFile(), "cal-1-1-B-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a file that matches neither pattern
        sample1 = new File(dataImporterPath.toFile(), "pdc-1-1-22-results.h5");
        sample1.createNewFile();
        filenames.add(sample1.getName());
        return filenames;
    }

    private void setUpModelsForImport(Path modelDirPath) throws IOException {
        String modelImportDir = modelDirPath.toString();
        // create the new files to be imported
        new File(modelImportDir, "tess2020321141517-12345_024-geometry.xml").createNewFile();
        new File(modelImportDir, "tess2020321141517-12345_025-geometry.xml").createNewFile();
        new File(modelImportDir, "calibration-4.12.19.h5").createNewFile();
        new File(modelImportDir, "simple-text.h5").createNewFile();
    }

    private void constructManifests() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {
        constructManifest(dataImporterSubdirPath, "data-importer-subdir-manifest.xml", 2L);
        constructManifest(modelImporterSubdirPath, "model-importer-subdir-manifest.xml", 3L);
        constructManifest(dataImporterPath, "data-importer-manifest.xml", 1L);
    }

    private void constructManifest(Path dir, String name, long datasetId)
        throws IOException, InstantiationException, IllegalAccessException, SAXException,
        JAXBException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException {
        Manifest manifest = Manifest.generateManifest(dir, datasetId);
        manifest.setName(name);
        if (manifest.getFileCount() > 0) {
            manifest.write(dir);
        }
    }

    /**
     * Specialized subclass of {@link ModelImporter} that produces {@link ModelMetadata} instances
     * with a fixed timestamp.
     *
     * @author PT
     */
    private class ModelImporterForTest extends ModelImporter {

        private ModelCrud crud = Mockito.mock(ModelCrud.class);

        public ModelImporterForTest(String directory, String modelDescription) {
            super(directory, modelDescription);
            Mockito.when(crud.retrieveAllModelTypes())
                .thenReturn(ImmutableList.of(modelType1, modelType2, modelType3));
            Mockito.when(crud.retrieveUnlockedRegistry()).thenReturn(new ModelRegistry());
            Mockito.when(crud.retrieveUnlockedRegistryId()).thenReturn(2L);
        }

        @Override
        protected ModelMetadata modelMetadata(ModelType modelType, String modelName,
            String modelDescription, ModelMetadata currentRegistryMetadata) {
            return new ModelMetadataFixedDate(modelType, modelName, modelDescription,
                currentRegistryMetadata).toSuper();
        }

        @Override
        protected ModelCrud modelCrud() {
            return crud;
        }
    }

    /**
     * Specialized subclass of {@link DataReceiptPipelineModule} that produces an instance of
     * {@link ModelImporter} that, in turn, produces instances of {@link ModelMetadata} with fixed
     * timestamps.
     *
     * @author PT
     */
    private class DataReceiptModuleForTest extends DataReceiptPipelineModule implements Runnable {

        private boolean performDirectoryCleanupEnabled = true;
        private ProcessingState processingState = ProcessingState.INITIALIZING;
        private Set<DatastoreProducerConsumer> successfulImportsDataAccountability = new HashSet<>();
        private Set<FailedImport> failedImportsDataAccountability = new HashSet<>();
        private Manifest manifest;

        public DataReceiptModuleForTest(PipelineTask pipelineTask, RunMode runMode) {
            super(pipelineTask, runMode);
        }

        @Override
        public void executingTaskAction() {
            super.executingTaskAction();
        }

        @Override
        public void storingTaskAction() {
            super.storingTaskAction();
        }

        @Override
        public void processingCompleteTaskAction() {
            super.processingCompleteTaskAction();
        }

        @Override
        ModelImporter modelImporter(Path importDirectory, String description) {
            if (modelImporter == null) {
                modelImporter = new ModelImporterForTest(importDirectory.toString(), description);
            }
            return modelImporter;
        }

        @Override
        void updateModelRegistryForPipelineInstance() {
        }

        @Override
        public void performDirectoryCleanup() {
            if (performDirectoryCleanupEnabled) {
                super.performDirectoryCleanup();
            }
        }

        @Override
        protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
            Collection<Path> failedImports, DataReceiptFileType fileType) {
            for (Path file : successfulImports) {
                successfulImportsDataAccountability.add(
                    new DatastoreProducerConsumer(pipelineTask.getId(), file.toString(), fileType));
            }
            for (Path file : failedImports) {
                failedImportsDataAccountability.add(new FailedImport(pipelineTask, file, fileType));
            }
        }

        /**
         * Returns the sequence of {@link ProcessingState} instances that are produced by the
         * production version of {@link #getProcessingState()} during pipeline execution. This
         * allows us to live without a database connection for these tests.
         */
        @Override
        public ProcessingState getProcessingState() {
            return processingState;
        }

        @Override
        public void incrementProcessingState() {
            processingState = nextProcessingState(processingState);
        }

        @Override
        protected void flushDatabase() {
        }

        public void disableDirectoryCleanup() {
            performDirectoryCleanupEnabled = false;
        }

        public Set<DatastoreProducerConsumer> getSuccessfulImportsDataAccountability() {
            return successfulImportsDataAccountability;
        }

        public Set<FailedImport> getFailedImportsDataAccountability() {
            return failedImportsDataAccountability;
        }

        @Override
        ManifestCrud manifestCrud() {
            return new ManifestCrudForTest();
        }

        public Manifest getManifest() {
            return manifest;
        }

        /**
         * Version of {@link ManifestCrud} that's safe to use in testing.
         *
         * @author PT
         */
        private class ManifestCrudForTest extends ManifestCrud {

            @Override
            public void persist(Object o) {
                manifest = (Manifest) o;
            }

            @Override
            public boolean datasetIdExists(long datasetId) {
                return false;
            }
        }

        @Override
        public void run() {
            processTask();
        }
    }
}
