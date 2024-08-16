package gov.nasa.ziggy.models;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreConfigurationImporter;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Unit tests for {@link ModelOperations}.
 *
 * @author PT
 */
public class ModelOperationsTest {

    // The easiest way to get the model definitions into the database is to import them from a file.
    private static final Path DATASTORE = TEST_DATA.resolve("datastore");
    private static final String FILE_1 = DATASTORE.resolve("pd-test-1.xml").toString();

    private ModelOperations modelOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Before
    public void setUp() {
        modelOperations = new ModelOperations();
        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1), false);
        dataFileImporter.importConfiguration();
    }

    @Test
    public void testAllModelTypes() {
        List<ModelType> modelTypes = modelOperations.allModelTypes();
        List<String> typeStrings = modelTypes.stream()
            .map(ModelType::getType)
            .collect(Collectors.toList());
        assertTrue(typeStrings.contains("geometry"));
        assertTrue(typeStrings.contains("read-noise"));
        assertEquals(2, typeStrings.size());
    }

    @Test
    public void testRetrieveUnlockedRegistry() {
        ModelRegistry modelRegistry = modelOperations.unlockedRegistry();
        assertEquals(1L, modelRegistry.getId().longValue());
        assertFalse(modelRegistry.isLocked());
        modelOperations.lockCurrentRegistry();
        ModelRegistry newRegistry = modelOperations.unlockedRegistry();
        assertEquals(2L, newRegistry.getId().longValue());
        assertFalse(newRegistry.isLocked());
    }

    @Test
    public void lockCurrentRegistry() {
        modelOperations.lockCurrentRegistry();
        ModelRegistry lockedRegistry = new TestOperations().currentRegistry();
        assertEquals(1L, lockedRegistry.getId().longValue());
        assertTrue(lockedRegistry.isLocked());
    }

    @Test
    public void testModelTypeMap() {
        Map<String, ModelType> modelTypeMap = modelOperations.modelTypeMap();
        assertNotNull(modelTypeMap.get("geometry"));
        assertNotNull(modelTypeMap.get("read-noise"));
        assertEquals(2, modelTypeMap.size());
    }

    @Test
    public void testPersistModelMetadata() {
        Map<String, ModelType> modelTypeMap = modelOperations.modelTypeMap();
        ModelType modelType = modelTypeMap.get("geometry");
        ModelMetadata geometryMetadata = new ModelMetadata(modelType,
            "tess1234567890123-12345_001-geometry.xml", "the geometry model", null);
        modelOperations.persistModelMetadata(geometryMetadata);
        ModelRegistry registry = modelOperations.unlockedRegistry();
        registry.updateModelMetadata(geometryMetadata);
        modelOperations.mergeRegistryAndReturnUnlockedId(registry);
        ModelRegistry updatedRegistry = modelOperations.unlockedRegistry();
        Map<ModelType, ModelMetadata> metadataByModel = updatedRegistry.getModels();
        ModelMetadata databaseMetadata = metadataByModel.get(modelType);
        assertNotNull(databaseMetadata);
        assertEquals("001", databaseMetadata.getModelRevision());
        assertEquals("the geometry model", databaseMetadata.getModelDescription());
    }

    @Test
    public void testMergeRegistryAndReturnUnlockedId() {
        Map<String, ModelType> modelTypeMap = modelOperations.modelTypeMap();
        ModelType modelType = modelTypeMap.get("geometry");
        ModelMetadata geometryMetadata = new ModelMetadata(modelType,
            "tess1234567890123-12345_001-geometry.xml", "the geometry model", null);
        modelOperations.persistModelMetadata(geometryMetadata);
        ModelRegistry registry = modelOperations.unlockedRegistry();
        assertEquals(1L, registry.getId().longValue());
        registry.updateModelMetadata(geometryMetadata);
        assertEquals(1L, modelOperations.mergeRegistryAndReturnUnlockedId(registry));
        modelOperations.lockCurrentRegistry();
        ModelMetadata updatedMetadata = new ModelMetadata(modelType,
            "tess1234567890123-12345_002-geometry.xml", "the geometry model", geometryMetadata);
        modelOperations.persistModelMetadata(updatedMetadata);
        registry.updateModelMetadata(updatedMetadata);
        assertEquals(2L, modelOperations.mergeRegistryAndReturnUnlockedId(registry));
        ModelRegistry updatedRegistry = modelOperations.unlockedRegistry();
        assertEquals(2L, updatedRegistry.getId().longValue());
        Map<ModelType, ModelMetadata> metadataByModel = updatedRegistry.getModels();
        ModelMetadata databaseMetadata = metadataByModel.get(modelType);
        assertNotNull(databaseMetadata);
        assertEquals("002", databaseMetadata.getModelRevision());
        assertEquals("the geometry model", databaseMetadata.getModelDescription());
    }

    private static class TestOperations extends DatabaseOperations {

        public ModelRegistry currentRegistry() {
            return performTransaction(() -> new ModelCrud().retrieveCurrentRegistry());
        }
    }
}
