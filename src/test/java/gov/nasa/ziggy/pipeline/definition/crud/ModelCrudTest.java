package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Unit tests for the ModelCrud class.
 *
 * @author PT
 */
public class ModelCrudTest {

    private ModelType modelType1, modelType2;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setup() {

        // Set up a couple of model types and persist them to the database
        setUpModelTypes();
        DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            modelCrud.create(modelType1);
            modelCrud.create(modelType2);
            return null;
        });
    }

    @Test
    public void retrieveUnlockedRegistryWhenEmpty() {

        ModelRegistry registry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                return new ModelCrud().retrieveUnlockedRegistry();
            });
        assertEquals(1L, registry.getId());
        assertFalse(registry.isLocked());

    }

    @Test
    public void retrieveCurrentRegistryWhenEmpty() {

        ModelRegistry registry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                return new ModelCrud().retrieveCurrentRegistry();
            });
        assertEquals(1L, registry.getId());
        assertFalse(registry.isLocked());

    }

    @Test
    public void retrieveCurrentAndUnlockedRegistries() {

        ModelRegistry registry = (ModelRegistry) DatabaseTransactionFactory
            .performTransaction(() -> {
                ModelCrud modelCrud = new ModelCrud();
                return modelCrud.retrieveCurrentRegistry();
            });
        // Retrieve the current registry and see that it's registry 1,
        // and not locked
        registry = (ModelRegistry) DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            return modelCrud.retrieveCurrentRegistry();
        });
        assertEquals(1L, registry.getId());
        assertFalse(registry.isLocked());

        // Lock the current registry and then retrieve it; it should
        // still be registry 1, but now locked
        registry = (ModelRegistry) DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            modelCrud.lockCurrentRegistry();
            return modelCrud.retrieveCurrentRegistry();
        });
        assertEquals(1L, registry.getId());
        assertTrue(registry.isLocked());

        // Retrieve the unlocked registry and see that it's really
        // unlocked, and that it's registry 2
        registry = (ModelRegistry) DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            return modelCrud.retrieveUnlockedRegistry();
        });
        assertEquals(2L, registry.getId());
        assertFalse(registry.isLocked());

        // Just retrieve the ID of the unlocked registry
        long currentRegistryId = (long) DatabaseTransactionFactory.performTransaction(() -> {
            ModelCrud modelCrud = new ModelCrud();
            return modelCrud.retrieveUnlockedRegistryId();
        });
        assertEquals(2L, currentRegistryId);

    }

    @Test
    public void testRetrieveModelTypes() {

        @SuppressWarnings("unchecked")
        List<ModelType> modelTypes = (List<ModelType>) DatabaseTransactionFactory
            .performTransaction(() -> {
                return new ModelCrud().retrieveAllModelTypes();
            });
        assertEquals(2, modelTypes.size());
        assertTrue(modelTypes.contains(modelType1));
        assertTrue(modelTypes.contains(modelType2));

    }

    @Test
    public void testRetrieveModelTypeMap() {

        @SuppressWarnings("unchecked")
        Map<String, ModelType> modelTypeMap = (Map<String, ModelType>) DatabaseTransactionFactory
            .performTransaction(() -> {
                return new ModelCrud().retrieveModelTypeMap();
            });
        assertEquals(2, modelTypeMap.size());
        assertEquals(modelType1, modelTypeMap.get("geometry"));
        assertEquals(modelType2, modelTypeMap.get("read-noise"));

    }

    private void setUpModelTypes() {
        modelType1 = new ModelType();
        modelType1.setType("geometry");
        modelType1.setFileNameRegex("geometry.xml");
        modelType1.setTimestampGroup(-1);
        modelType1.setVersionNumberGroup(-1);

        modelType2 = new ModelType();
        modelType2.setType("read-noise");
        modelType2.setFileNameRegex("read-noise.xml");
        modelType2.setTimestampGroup(-1);
        modelType2.setVersionNumberGroup(-1);
    }

}
