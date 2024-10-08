package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry_;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ModelType_;

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
public class ModelCrud extends AbstractCrud<ModelRegistry> {

    /**
     * Retrieves the current registry (the registry with the highest ID number). If no registry
     * exists in the database, a new, empty registry is created and returned.
     */
    public ModelRegistry retrieveCurrentRegistry() {

        ZiggyQuery<ModelRegistry, ModelRegistry> query = createZiggyQuery(ModelRegistry.class);
        ZiggyQuery<ModelRegistry, Long> idSubquery = query.ziggySubquery(ModelRegistry.class,
            Long.class);
        idSubquery.column(ModelRegistry_.id).max();
        query.column(ModelRegistry_.id).in(idSubquery);
        ModelRegistry currentRegistry = uniqueResult(query);
        if (currentRegistry == null) {
            currentRegistry = new ModelRegistry();
            persist(currentRegistry);
        }
        return currentRegistry;
    }

    public ModelRegistry merge(ModelRegistry registry) {
        ModelRegistry databaseRegistry = retrieve(registry.getId()).isLocked()
            ? new ModelRegistry(registry)
            : registry;
        return super.merge(databaseRegistry);
    }

    private ModelRegistry retrieve(long id) {
        ZiggyQuery<ModelRegistry, ModelRegistry> query = createZiggyQuery(ModelRegistry.class);
        query.column(ModelRegistry_.id).in(id);
        return uniqueResult(query);
    }

    /**
     * Retrieves the current unlocked registry. If no such registry exists, one is created which
     * duplicates the existing registry in other respects, and returned.
     */
    public ModelRegistry retrieveUnlockedRegistry() {

        ModelRegistry modelRegistry = retrieveCurrentRegistry();
        if (modelRegistry.isLocked()) {
            modelRegistry = new ModelRegistry(modelRegistry);
            persist(modelRegistry);
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
            merge(metadata);
        }
        modelRegistry.lock();
        return super.merge(modelRegistry);
    }

    /**
     * Retrieves all model types in the database.
     */
    public List<ModelType> retrieveAllModelTypes() {
        return list(createZiggyQuery(ModelType.class));
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
     * Retrieves a {@link ModelType} based on the type string of the instance.
     *
     * @param type
     * @return
     */
    public ModelType retrieveModelType(String type) {
        return uniqueResult(createZiggyQuery(ModelType.class).column(ModelType_.type).in(type));
    }

    /**
     * Retrieves the ID of the current unlocked model registry.
     */
    public long retrieveUnlockedRegistryId() {
        return retrieveUnlockedRegistry().getId();
    }

    public List<ModelRegistry> retrieveAllModelRegistries() {
        return list(createZiggyQuery(ModelRegistry.class));
    }

    @Override
    public Class<ModelRegistry> componentClass() {
        return ModelRegistry.class;
    }
}
