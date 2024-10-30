package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Unit tests for {@link DataReceiptOperations}.
 *
 * @author PT
 */
public class DataReceiptOperationsTest {

    private DataReceiptOperations dataReceiptOperations;
    private ModelOperations modelOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;
    private PipelineInstanceCrud pipelineInstanceCrud = Mockito.spy(PipelineInstanceCrud.class);
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud = Mockito
        .spy(DatastoreProducerConsumerCrud.class);
    private FailedImportCrud failedImportCrud = Mockito.spy(FailedImportCrud.class);
    private ModelCrud modelCrud = Mockito.spy(ModelCrud.class);
    private PipelineModuleDefinition dataReceiptModule;
    private PipelineDefinitionNode dataReceiptNode;
    private ModelRegistry modelRegistry;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        dataReceiptOperations = Mockito.spy(DataReceiptOperations.class);
        modelOperations = new ModelOperations();
        pipelineInstanceOperations = new PipelineInstanceOperations();
        Mockito.when(dataReceiptOperations.datastoreProducerConsumerCrud())
            .thenReturn(datastoreProducerConsumerCrud);
        Mockito.when(dataReceiptOperations.failedImportCrud()).thenReturn(failedImportCrud);
        Mockito.when(dataReceiptOperations.modelCrud()).thenReturn(modelCrud);
        Mockito.when(dataReceiptOperations.pipelineInstanceCrud()).thenReturn(pipelineInstanceCrud);
        dataReceiptModule = new PipelineModuleDefinitionOperations()
            .createDataReceiptPipelineModule();
        dataReceiptNode = testOperations
            .merge(new PipelineDefinitionNode(dataReceiptModule.getName(), "dummy"));

        // The model registry should be locked because we're simulating a pipeline instance
        // that runs data receipt, hence one that has a locked model registry.
        modelRegistry = modelOperations.unlockedRegistry();
        modelOperations.lockCurrentRegistry();
    }

    @Test
    public void testDataReceiptInstances() {

        Map<Long, ImportFiles> importFilesByInstanceId = new HashMap<>();
        ImportFiles importFiles = new TestOperations().setUpDataReceiptInstance(2, 2);
        importFilesByInstanceId.put(importFiles.getInstanceId(), importFiles);
        importFiles = new TestOperations().setUpDataReceiptInstance(5, 1);
        List<DataReceiptInstance> dataReceiptInstances = dataReceiptOperations
            .dataReceiptInstances();
        importFilesByInstanceId.put(importFiles.getInstanceId(), importFiles);

        for (DataReceiptInstance dataReceiptInstance : dataReceiptInstances) {
            importFiles = importFilesByInstanceId.get(dataReceiptInstance.getInstanceId());
            assertEquals(importFiles.getSuccessfulImportFiles().size(),
                dataReceiptInstance.getSuccessfulImportCount());
            assertEquals(importFiles.getFailedImportFiles().size(),
                dataReceiptInstance.getFailedImportCount());
        }
        assertEquals(2, dataReceiptInstances.size());
    }

    @Test
    public void testDataReceiptFilesForInstance() {
        ImportFiles importFiles = new TestOperations().setUpDataReceiptInstance(2, 2);
        List<DataReceiptFile> dataReceiptFiles = dataReceiptOperations
            .dataReceiptFilesForInstance(1L);
        Map<String, DataReceiptFile> dataReceiptFileByName = new HashMap<>();
        for (DataReceiptFile dataReceiptFile : dataReceiptFiles) {
            dataReceiptFileByName.put(dataReceiptFile.getName(), dataReceiptFile);
        }
        for (Path successfulImportFile : importFiles.getSuccessfulImportFiles()) {
            assertNotNull(dataReceiptFileByName.get(successfulImportFile.toString()));
            assertEquals("Imported",
                dataReceiptFileByName.get(successfulImportFile.toString()).getStatus());
            assertEquals(1L,
                dataReceiptFileByName.get(successfulImportFile.toString()).getTaskId());
        }
        for (Path failedImportFile : importFiles.getFailedImportFiles()) {
            assertNotNull(dataReceiptFileByName.get(failedImportFile.toString()));
            assertEquals("Failed",
                dataReceiptFileByName.get(failedImportFile.toString()).getStatus());
            assertEquals(1L, dataReceiptFileByName.get(failedImportFile.toString()).getTaskId());
        }
        assertEquals(4, dataReceiptFiles.size());
    }

    @Test
    public void testUpdateModelRegistry() {
        new TestOperations().setUpDataReceiptInstance(5, 0);
        PipelineInstance instance = pipelineInstanceOperations.pipelineInstance(1L);
        assertNotNull(instance.getModelRegistry());
        assertEquals(1L, instance.getModelRegistry().getId().longValue());
        assertTrue(instance.getModelRegistry().isLocked());
        dataReceiptOperations.updateModelRegistryForPipelineInstance(1L);
        assertEquals(2L,
            pipelineInstanceOperations.pipelineInstance(1L).getModelRegistry().getId().longValue());
    }

    /** Container class for mock files created for mock import instances. */
    private static class ImportFiles {

        private final List<Path> successfulImportFiles = new ArrayList<>();
        private final List<Path> failedImportFiles = new ArrayList<>();
        private final long instanceId;

        public ImportFiles(long instanceId) {
            this.instanceId = instanceId;
        }

        public void addSuccessfulFile(Path file) {
            successfulImportFiles.add(file);
        }

        public void addFailedFile(Path file) {
            failedImportFiles.add(file);
        }

        public long getInstanceId() {
            return instanceId;
        }

        public List<Path> getSuccessfulImportFiles() {
            return successfulImportFiles;
        }

        public List<Path> getFailedImportFiles() {
            return failedImportFiles;
        }
    }

    private class TestOperations extends DatabaseOperations {

        private ImportFiles setUpDataReceiptInstance(int successfulImports, int failedImports) {

            return performTransaction(() -> {
                PipelineInstance instance1 = new PipelineInstance();
                instance1 = new PipelineInstanceCrud().merge(instance1);
                instance1.setModelRegistry(modelRegistry);
                ImportFiles importFiles = new ImportFiles(instance1.getId());
                PipelineInstanceNode instanceNode1 = new PipelineInstanceNodeCrud()
                    .merge(new PipelineInstanceNode(dataReceiptNode, dataReceiptModule));
                instance1.addPipelineInstanceNode(instanceNode1);
                instance1 = new PipelineInstanceCrud().merge(instance1);
                PipelineTask task1 = new PipelineTask(instance1, instanceNode1, null);
                task1 = new PipelineTaskCrud().merge(task1);
                instanceNode1.addPipelineTask(task1);
                instanceNode1 = new PipelineInstanceNodeCrud().merge(instanceNode1);
                for (int fileIndex = 0; fileIndex < successfulImports; fileIndex++) {
                    importFiles
                        .addSuccessfulFile(Paths.get(RandomStringUtils.random(10, true, true)));
                }
                datastoreProducerConsumerCrud.createOrUpdateProducer(task1,
                    importFiles.getSuccessfulImportFiles());
                for (int fileIndex = 0; fileIndex < failedImports; fileIndex++) {
                    importFiles.addFailedFile(Paths.get(RandomStringUtils.random(10, true, true)));
                }
                failedImportCrud.create(task1, importFiles.getFailedImportFiles());
                return importFiles;
            });
        }

        public PipelineDefinitionNode merge(PipelineDefinitionNode pipelineDefinitionNode) {
            return performTransaction(
                () -> new PipelineDefinitionNodeCrud().merge(pipelineDefinitionNode));
        }
    }
}
