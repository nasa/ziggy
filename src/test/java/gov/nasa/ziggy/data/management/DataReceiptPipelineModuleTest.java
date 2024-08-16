package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.DATA_RECEIPT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.USE_SYMLINKS;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
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
    private Path datastoreRootPath;
    private UnitOfWork singleUow = new UnitOfWork();
    private UnitOfWork dataSubdirUow = new UnitOfWork();
    private PipelineDefinitionNode node = new PipelineDefinitionNode();
    private ModelType modelType1, modelType2, modelType3;
    private DatastoreDirectoryDataReceiptDefinition dataReceiptDefinition;
    private ModelImporter modelImporter, subdirModelImporter;
    private ModelCrud modelCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private DatastoreWalker datastoreWalker;
    private DataReceiptOperations dataReceiptOperations;
    ModelRegistry registry;

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

        // Construct the necessary directories.
        dataImporterPath = Paths.get(dataReceiptDirPropertyRule.getValue()).toAbsolutePath();
        datastoreRootPath = Paths.get(datastoreRootDirPropertyRule.getValue()).toAbsolutePath();
        dataImporterSubdirPath = dataImporterPath.resolve("sub-dir");
        dataSubdirUow.addParameter(new Parameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        singleUow.addParameter(new Parameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        dataSubdirUow
            .addParameter(new Parameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PARAMETER_NAME,
                "sub-dir", ZiggyDataType.ZIGGY_STRING));
        singleUow.addParameter(new Parameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PARAMETER_NAME,
            "", ZiggyDataType.ZIGGY_STRING));

        // construct the model type information
        node.setModelTypes(ImmutableSet.of(modelType1, modelType2, modelType3));

        // Create the "database objects," these are actually an assortment of mocks
        // so we can test this without needing an actual database.
        Mockito.when(pipelineTask.getId()).thenReturn(101L);
        PipelineInstance pipelineInstance = new PipelineInstance();
        pipelineInstance.setId(2L);
        pipelineInstanceCrud = Mockito.mock(PipelineInstanceCrud.class);
        Mockito.when(pipelineInstanceCrud.retrieve(ArgumentMatchers.anyLong()))
            .thenReturn(pipelineInstance);

        dataReceiptOperations = Mockito.spy(DataReceiptOperations.class);
        Mockito.doReturn(pipelineInstanceCrud).when(dataReceiptOperations).pipelineInstanceCrud();

        // Put in a mocked AlertService instance.
        AlertService.setInstance(Mockito.mock(AlertService.class));

        // Set up the model importer and data receipt definition.
        constructDataReceiptDefinition();
        Mockito.doReturn(dataReceiptOperations).when(dataReceiptDefinition).dataReceiptOperations();
        Mockito.doReturn(List.of(modelType1, modelType2, modelType3))
            .when(dataReceiptDefinition)
            .modelTypes();
        modelCrud = Mockito.mock(ModelCrud.class);

        registry = new ModelRegistry();
        modelImporter = new ModelImporter(dataImporterPath, "unit test");
        modelImporter = Mockito.spy(modelImporter);
        Mockito.doReturn(registry).when(modelImporter).unlockedRegistry();
        Mockito.doNothing()
            .when(modelImporter)
            .persistModelMetadata(ArgumentMatchers.any(ModelMetadata.class));
        Mockito.doReturn(1L)
            .when(modelImporter)
            .mergeRegistryAndReturnUnlockedId(ArgumentMatchers.any(ModelRegistry.class));
        Mockito.doReturn(modelImporter).when(dataReceiptDefinition).modelImporter();

        subdirModelImporter = new ModelImporter(dataImporterSubdirPath, "unit test");
        subdirModelImporter = Mockito.spy(subdirModelImporter);
        Mockito.doReturn(registry).when(subdirModelImporter).unlockedRegistry();
        Mockito.doNothing()
            .when(subdirModelImporter)
            .persistModelMetadata(ArgumentMatchers.any(ModelMetadata.class));
        Mockito.doReturn(1L)
            .when(subdirModelImporter)
            .mergeRegistryAndReturnUnlockedId(ArgumentMatchers.any(ModelRegistry.class));
    }

    @After
    public void shutDown() throws InterruptedException, IOException {
        AlertService.setInstance(null);
    }

    @Test
    public void testImportFromDataReceiptDir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the importer files
        constructFilesForImport(dataImporterPath, true);

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
        assertEquals(7, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports
            .containsKey("sector-0002/mda/dr/pixels/target/science/1:1:A/1:1:A.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/dr/pixels/target/science/1:1:A/1:1:A.nc"));

        assertTrue(successfulImports
            .containsKey("sector-0002/mda/cal/pixels/ffi/collateral/1:1:A/1:1:A.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/cal/pixels/ffi/collateral/1:1:A/1:1:A.nc"));

        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        assertEquals(3, registry.getModels().size());
        ModelMetadata metadata = registry.getModels().get(modelType1);
        String datastoreName = Paths.get("models")
            .resolve(modelType1.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType2);
        datastoreName = Paths.get("models")
            .resolve(modelType2.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType3);
        datastoreName = Paths.get("models")
            .resolve(modelType3.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        // check that all the files made it to their destinations
        assertTrue(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "dr", "pixels", "target", "science",
                    "1:1:A", "1:1:A.nc"))
                .toFile()
                .exists());
        assertTrue(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "cal", "pixels", "ffi", "collateral",
                    "1:1:A", "1:1:A.nc"))
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

        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType1.getType())
            .resolve(registry.getModels().get(modelType1).getDatastoreFileName())
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType2.getType())
            .resolve(registry.getModels().get(modelType2).getDatastoreFileName())
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType3.getType())
            .resolve(registry.getModels().get(modelType3).getDatastoreFileName())
            .toFile()
            .exists());
    }

    @Test
    public void testImportFromDataSubdir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the importer files
        constructFilesForImport(dataImporterPath, true);
        constructFilesForImport(dataImporterSubdirPath, false);
        Mockito.doReturn(subdirModelImporter).when(dataReceiptDefinition).modelImporter();

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
        assertEquals(5, producerConsumerRecords.size());
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports
            .containsKey("sector-0002/mda/dr/pixels/target/science/1:1:B/1:1:B.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/dr/pixels/target/science/1:1:B/1:1:B.nc"));

        // check that the data files made it to their destinations
        assertTrue(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "dr", "pixels", "target", "science",
                    "1:1:B", "1:1:B.nc"))
                .toFile()
                .exists());

        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        assertEquals(3, registry.getModels().size());
        ModelMetadata metadata = registry.getModels().get(modelType1);
        String datastoreName = Paths.get("models")
            .resolve(modelType1.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType2);
        datastoreName = Paths.get("models")
            .resolve(modelType2.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType3);
        datastoreName = Paths.get("models")
            .resolve(modelType3.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertEquals(3, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve("models").toFile().exists());
        assertTrue(dataImporterPath.resolve("sector-0002").toFile().exists());
        assertTrue(dataImporterPath.resolve("data-importer-manifest.xml").toFile().exists());

        // The manifest and the acknowledgement should be moved to the manifests
        // directory
        Path manifestDir = DirectoryProperties.manifestsDir();
        assertTrue(Files.exists(manifestDir));
        assertTrue(Files.exists(manifestDir.resolve("data-importer-subdir-manifest.xml")));
        assertTrue(Files.exists(manifestDir.resolve("data-importer-subdir-manifest-ack.xml")));

        // The data directory should be deleted
        assertFalse(Files.exists(dataImporterSubdirPath));
    }

    @Test
    public void testImportWithErrors() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Populate the models
        constructFilesForImport(dataImporterPath, true);

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);

        // Generate data and model importers that will throw IOExceptions at opportune moments
        Path dataReceiptExceptionPath = dataImporterPath.resolve(Paths.get("sector-0002", "mda",
            "cal", "pixels", "ffi", "collateral", "1:1:A", "1:1:A.nc"));
        Path datastoreExceptionPath = datastoreRootPath.resolve(Paths.get("sector-0002", "mda",
            "cal", "pixels", "ffi", "collateral", "1:1:A", "1:1:A.nc"));
        Mockito.doThrow(IOException.class)
            .when(dataReceiptDefinition)
            .move(dataReceiptExceptionPath, datastoreExceptionPath);

        Path destFileToFlunk = Paths.get(datastoreRootPath.toString(), "models", "geometry",
            "tess2020321141517-12345_025-geometry.xml");
        Path srcFileToFlunk = Paths.get(dataImporterPath.toString(), "models",
            "tess2020321141517-12345_025-geometry.xml");
        Mockito.doThrow(IOException.class)
            .when(modelImporter)
            .move(srcFileToFlunk, destFileToFlunk);

        // Install the data and model importers in the pipeline module
        DataReceiptModuleForTest pipelineModule = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        final DataReceiptModuleForTest module = Mockito.spy(pipelineModule);

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
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }
        assertTrue(successfulImports
            .containsKey("sector-0002/mda/dr/pixels/target/science/1:1:A/1:1:A.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/dr/pixels/target/science/1:1:A/1:1:A.nc"));

        assertTrue(successfulImports
            .containsKey("sector-0002/mda/cal/pixels/ffi/collateral/1:1:B/1:1:B.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/cal/pixels/ffi/collateral/1:1:B/1:1:B.nc"));

        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_024-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_024-geometry.xml"));

        assertEquals(3, registry.getModels().size());
        ModelMetadata metadata = registry.getModels().get(modelType1);
        String datastoreName = Paths.get("models")
            .resolve(modelType1.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType2);
        datastoreName = Paths.get("models")
            .resolve(modelType2.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        metadata = registry.getModels().get(modelType3);
        datastoreName = Paths.get("models")
            .resolve(modelType3.getType())
            .resolve(metadata.getDatastoreFileName())
            .toString();
        assertTrue(successfulImports.containsKey(datastoreName));
        assertEquals(Long.valueOf(101L), successfulImports.get(datastoreName));

        assertEquals(5, successfulImports.size());

        // check that the files made it to their destinations
        assertTrue(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "dr", "pixels", "target", "science",
                    "1:1:A", "1:1:A.nc"))
                .toFile()
                .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_024-geometry.xml"))
            .toFile()
            .exists());

        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType1.getType())
            .resolve(registry.getModels().get(modelType1).getDatastoreFileName())
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType2.getType())
            .resolve(registry.getModels().get(modelType2).getDatastoreFileName())
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve("models")
            .resolve(modelType3.getType())
            .resolve(registry.getModels().get(modelType3).getDatastoreFileName())
            .toFile()
            .exists());

        assertFalse(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_025-geometry.xml"))
            .toFile()
            .exists());
        assertFalse(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "cal", "pixels", "ffi", "collateral",
                    "1:1:A", "1:1:A.nc"))
                .toFile()
                .exists());

        // Check that the files were removed from the import directories, or not, as
        // appropriate
        assertTrue(dataImporterPath.resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A")
            .resolve("1:1:A.nc")
            .toFile()
            .exists());
        assertTrue(dataImporterPath.resolve("models")
            .resolve("tess2020321141517-12345_025-geometry.xml")
            .toFile()
            .exists());

        // Finally, check that the expected files are in the failed imports table.
        Set<FailedImport> failedImports = module.getFailedImportsDataAccountability();
        assertEquals(2, failedImports.size());
        Map<String, Long> failedImportMap = new HashMap<>();
        for (FailedImport failedImport : failedImports) {
            failedImportMap.put(failedImport.getFilename(), failedImport.getDataReceiptTaskId());
        }
        assertTrue(failedImportMap.containsKey(Paths.get("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A")
            .resolve("1:1:A.nc")
            .toString()));
        assertEquals(Long.valueOf(101L),
            failedImportMap.get(Paths.get("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("collateral")
                .resolve("1:1:A")
                .resolve("1:1:A.nc")
                .toString()));

        assertTrue(failedImportMap.containsKey("models/tess2020321141517-12345_025-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            failedImportMap.get("models/tess2020321141517-12345_025-geometry.xml"));
    }

    @Test
    public void testReEntrantImportAfterError() throws InstantiationException,
        IllegalAccessException, IllegalArgumentException, InvocationTargetException,
        NoSuchMethodException, SecurityException, IOException, SAXException, JAXBException {
        testImportWithErrors();

        // Reconstruct the data receipt definition and model importer so that the extant versions
        // don't throw IOExceptions.
        constructDataReceiptDefinition();
        Mockito.doReturn(List.of(modelType1, modelType2, modelType3))
            .when(dataReceiptDefinition)
            .modelTypes();
        modelImporter = new ModelImporter(dataImporterPath, "unit test");
        modelImporter = Mockito.spy(modelImporter);
        Mockito.doReturn(modelImporter).when(dataReceiptDefinition).modelImporter();
        Mockito.doReturn(registry).when(modelImporter).unlockedRegistry();
        Mockito.doNothing()
            .when(modelImporter)
            .persistModelMetadata(ArgumentMatchers.any(ModelMetadata.class));
        Mockito.doReturn(1L)
            .when(modelImporter)
            .mergeRegistryAndReturnUnlockedId(ArgumentMatchers.any(ModelRegistry.class));

        assertFalse(Files.exists(dataImporterPath.resolve("data-importer-manifest.xml")));

        DataReceiptModuleForTest pipelineModule = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        pipelineModule.storingTaskAction();

        // Obtain the producer-consumer records and check that the expected files are listed with
        // the correct producer
        Set<DatastoreProducerConsumer> producerConsumerRecords = pipelineModule
            .getSuccessfulImportsDataAccountability();
        Map<String, Long> successfulImports = new HashMap<>();
        for (DatastoreProducerConsumer producerConsumer : producerConsumerRecords) {
            successfulImports.put(producerConsumer.getFilename(), producerConsumer.getProducer());
        }

        assertTrue(successfulImports
            .containsKey("sector-0002/mda/cal/pixels/ffi/collateral/1:1:A/1:1:A.nc"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("sector-0002/mda/cal/pixels/ffi/collateral/1:1:A/1:1:A.nc"));

        assertTrue(successfulImports
            .containsKey("models/geometry/tess2020321141517-12345_025-geometry.xml"));
        assertEquals(Long.valueOf(101L),
            successfulImports.get("models/geometry/tess2020321141517-12345_025-geometry.xml"));

        assertEquals(0, pipelineModule.getFailedImportsDataAccountability().size());

        // check that the files made it to their destinations
        assertTrue(
            datastoreRootPath
                .resolve(Paths.get("sector-0002", "mda", "cal", "pixels", "ffi", "collateral",
                    "1:1:A", "1:1:A.nc"))
                .toFile()
                .exists());
        assertTrue(datastoreRootPath
            .resolve(Paths.get("models", "geometry", "tess2020321141517-12345_025-geometry.xml"))
            .toFile()
            .exists());

        // Check that the data import directory is empty because cleanup ran successfully.
        File[] files = dataImporterPath.toFile().listFiles();
        assertEquals(0, files.length);
    }

    @Test(expected = PipelineException.class)
    public void testCleanupFailOnNonEmptyDir() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        constructFilesForImport(dataImporterPath, true);

        // Set up the pipeline module to return the single unit of work task and the appropriate
        // families of model and data types
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        Mockito.when(pipelineTask.getId()).thenReturn(101L);

        // Perform the import
        DataReceiptPipelineModule module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.performDirectoryCleanup();
    }

    @Test
    public void testImportOnEmptyDirectory() throws IOException {

        if (Files.exists(dataImporterPath)) {
            FileUtils.cleanDirectory(dataImporterPath.toFile());
            Files.delete(dataImporterPath);
        }
        Files.createDirectories(dataImporterPath);
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        constructDataReceiptDefinition();
        // Perform the import
        DataReceiptModuleForTest module = new DataReceiptModuleForTest(pipelineTask,
            RunMode.STANDARD);
        module.disableDirectoryCleanup();
        assertTrue(module.processTask());
    }

    @Test(expected = PipelineException.class)
    public void testMissingManifest() throws InstantiationException, IllegalAccessException,
        IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException, IOException, SAXException, JAXBException {

        constructFilesForImport(dataImporterPath, true);
        Files.delete(dataImporterPath.resolve("data-importer-manifest.xml"));

        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(singleUow);
        constructDataReceiptDefinition();
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

    /**
     * Constructs the data receipt directory. Specifically, files are placed in the main DR
     * directory and in each of two subdirectories, and each of the 3 directories then gets a
     * manifest generated.
     */
    public static void constructFilesForImport(Path importerPath, boolean useMainImportDirectory)
        throws IOException, InstantiationException, IllegalAccessException,
        IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException, SAXException, JAXBException {

        // Start with the dataImporterPath files.
        if (useMainImportDirectory) {
            Path sample1 = importerPath.resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve("1:1:A.nc");
            Files.createDirectories(sample1.getParent());
            Files.createFile(sample1);

            sample1 = importerPath.resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("collateral")
                .resolve("1:1:A")
                .resolve("1:1:A.nc");
            Files.createDirectories(sample1.getParent());
            Files.createFile(sample1);

            sample1 = importerPath.resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("collateral")
                .resolve("1:1:B")
                .resolve("1:1:B.nc");
            Files.createDirectories(sample1.getParent());
            Files.createFile(sample1);

            setUpModelsForImport(importerPath);
            constructManifest(importerPath, "data-importer-manifest.xml", -1L);
            return;
        }

        // Now do the dataImporterSubdirPath.
        Path sample2 = importerPath.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")
            .resolve("1:1:B.nc");
        Files.createDirectories(sample2.getParent());
        Files.createFile(sample2);

        if (importerPath.equals(importerPath)) {
            setUpModelsForImport(importerPath);
            constructManifest(importerPath, "data-importer-subdir-manifest.xml", -2L);
        }
    }

    private static void setUpModelsForImport(Path dataImportDir) throws IOException {
        Path modelImportDir = dataImportDir.resolve("models");
        // create the new files to be imported
        Files.createDirectories(modelImportDir);
        // create the new files to be imported
        Path modelFile1 = modelImportDir.resolve("tess2020321141517-12345_024-geometry.xml");
        Files.createFile(modelFile1);
        Path modelFile2 = modelImportDir.resolve("tess2020321141517-12345_025-geometry.xml");
        Files.createFile(modelFile2);
        Path modelFile3 = modelImportDir.resolve("calibration-4.12.19.h5");
        Files.createFile(modelFile3);
        Path modelFile4 = modelImportDir.resolve("simple-text.h5");
        Files.createFile(modelFile4);
    }

    private static void constructManifest(Path dir, String name, long datasetId)
        throws IOException, InstantiationException, IllegalAccessException, SAXException,
        JAXBException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException {
        Manifest manifest = Manifest.generateManifest(dir, datasetId);
        manifest.setName(name);
        if (manifest.getFileCount() > 0) {
            manifest.write(dir);
        }
    }

    private void constructDataReceiptDefinition() {
        dataReceiptDefinition = new DatastoreDirectoryDataReceiptDefinition();
        dataReceiptDefinition.setDataImportDirectory(dataImporterPath);
        dataReceiptDefinition.setPipelineTask(pipelineTask);
        dataReceiptDefinition = Mockito.spy(dataReceiptDefinition);
        Mockito.doReturn(List.of(modelType1, modelType2, modelType3))
            .when(dataReceiptDefinition)
            .modelTypes();
        datastoreWalker = new DatastoreWalker(DatastoreTestUtils.regexpsByName(),
            DatastoreTestUtils.datastoreNodesByFullPath());
        Mockito.doReturn(datastoreWalker).when(dataReceiptDefinition).datastoreWalker();
        Mockito.doReturn(modelImporter).when(dataReceiptDefinition).modelImporter();
        Mockito.doReturn(modelCrud).when(dataReceiptOperations).modelCrud();
        Mockito.doNothing().when(dataReceiptDefinition).updateModelRegistryForPipelineInstance();
    }

    /**
     * Specialized subclass of {@link DataReceiptPipelineModule} that holds onto some of the data
     * accountability results for later inspection.
     *
     * @author PT
     */
    private class DataReceiptModuleForTest extends DataReceiptPipelineModule implements Runnable {

        private boolean performDirectoryCleanupEnabled = true;
        private ProcessingStep processingStep = processingSteps().get(0);
        private Set<DatastoreProducerConsumer> successfulImportsDataAccountability = new HashSet<>();
        private Set<FailedImport> failedImportsDataAccountability = new HashSet<>();

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
        public void performDirectoryCleanup() {
            if (performDirectoryCleanupEnabled) {
                super.performDirectoryCleanup();
            }
        }

        @Override
        DataReceiptDefinition dataReceiptDefinition() {
            return dataReceiptDefinition;
        }

        @Override
        DataReceiptOperations dataReceiptOperations() {
            return dataReceiptOperations;
        }

        @Override
        protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
            Collection<Path> failedImports) {
            for (Path file : successfulImports) {
                successfulImportsDataAccountability
                    .add(new DatastoreProducerConsumer(pipelineTask.getId(), file.toString()));
            }
            for (Path file : failedImports) {
                failedImportsDataAccountability.add(new FailedImport(pipelineTask, file));
            }
        }

        /**
         * Returns the sequence of {@link ProcessingStep} instances that are produced by the
         * production version of {@link #currentProcessingStep()} during pipeline execution. This
         * allows us to live without a database connection for these tests.
         */
        @Override
        public ProcessingStep currentProcessingStep() {
            return processingStep;
        }

        @Override
        public void incrementProcessingStep() {
            processingStep = nextProcessingStep(processingStep);
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
        public void run() {
            processTask();
        }
    }
}
