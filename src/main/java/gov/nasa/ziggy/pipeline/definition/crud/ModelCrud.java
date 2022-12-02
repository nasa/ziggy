package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Query;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;

/**
 * CRUD class for model registries and model metadata instances. Each model metadata instance
 * represents one version of one model; a model registry is the set of the most recent models. Model
 * registries and their metadata objects are locked the first time they are used in the pipeline in
 * order to preserve a permanent record of the models used for each pipeline instance. When
 * modifications have to be made, a new, unlocked instance is created if all existing instances are
 * locked.
 *
 * @author PT
 */
public class ModelCrud extends AbstractCrud {

    /**
     * Retrieves the current registry (the registry with the highest ID number). If no registry
     * exists in the database, a new, empty registry is created and returned.
     */
    public ModelRegistry retrieveCurrentRegistry() {
        Query query = createQuery(
            "from ModelRegistry where id in (select max(id) from ModelRegistry)");
        ModelRegistry modelRegistry = uniqueResult(query);
        if (modelRegistry == null) {
            modelRegistry = new ModelRegistry();
            create(modelRegistry);
        }
        return modelRegistry;
    }

    /**
     * Retrieves the current unlocked registry. If no such registry exists, one is created which
     * duplicates the existing registry in other respects, and returned.
     */
    public ModelRegistry retrieveUnlockedRegistry() {

        ModelRegistry modelRegistry = retrieveCurrentRegistry();
        if (modelRegistry.isLocked()) {
            modelRegistry = new ModelRegistry(modelRegistry);
            create(modelRegistry);
        }
        return modelRegistry;
    }

    /**
     * Lock the current model registry and all the model metadata it contains. This action is
     * performed when the registry is attached to a pipeline instance.
     */
    public ModelRegistry lockCurrentRegistry() {

        ModelRegistry modelRegistry = retrieveCurrentRegistry();
        Map<ModelType, ModelMetadata> models = modelRegistry.getModels();
        for (ModelMetadata metadata : models.values()) {
            metadata.lock();
            update(metadata);
        }
        modelRegistry.lock();
        update(modelRegistry);
        flush();
        return modelRegistry;
    }

    /**
     * Retrieves all model types in the database.
     */
    public List<ModelType> retrieveAllModelTypes() {
        Criteria criteria = createCriteria(ModelType.class);
        return list(criteria);
    }

    /**
     * Retrieves a Map from model type string to ModelType instance, for all ModelType instances in
     * the database.
     */
    public Map<String, ModelType> retrieveModelTypeMap() {
        List<ModelType> modelTypes = retrieveAllModelTypes();
        Map<String, ModelType> modelTypeMap = new HashMap<>();
        for (ModelType modelType : modelTypes) {
            modelTypeMap.put(modelType.getType(), modelType);
        }
        return modelTypeMap;
    }

    /**
     * Retrieves the ID of the current unlocked model registry.
     */
    public long retrieveUnlockedRegistryId() {
        return retrieveUnlockedRegistry().getId();
    }
}
