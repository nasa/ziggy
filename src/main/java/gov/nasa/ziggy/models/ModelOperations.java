package gov.nasa.ziggy.models;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for models, model metadata, and model registries.
 *
 * @author Todd Klaus
 * @author PT
 */
public class ModelOperations extends DatabaseOperations {
    private static final Logger log = LoggerFactory.getLogger(ModelOperations.class);

    private ValidatingXmlManager<ModelRegistry> xmlManager;
    private ModelCrud modelCrud = new ModelCrud();

    /**
     * Returns a String containing a textual report of the latest version of the metadata for all
     * models in the registry.
     */
    public String report() {
        return performTransaction(() -> {
            ModelRegistry registry = modelCrud().retrieveCurrentRegistry();

            if (registry != null) {
                return report(registry);
            }
            return "No Model Registry found";
        });
    }

    /**
     * Returns a String containing a textual report of the model registry associated with the
     * specified pipeline instance.
     */
    public String report(PipelineInstance pipelineInstance) {
        ModelRegistry registry = pipelineInstance.getModelRegistry();

        if (registry != null) {
            return report(registry);
        }
        return "No Model Registry found for this pipeline instance.";
    }

    /**
     * Produce a report for the specified ModelRegistry
     */
    private String report(ModelRegistry registry) {
        Map<ModelType, ModelMetadata> models = registry.getModels();
        List<ModelType> latestModelTypes = new LinkedList<>(models.keySet());
        Collections.sort(latestModelTypes);

        StringBuilder s = new StringBuilder();
        s.append("version=")
            .append(registry.getId())
            .append(", locked=")
            .append(registry.isLocked())
            .append(", lockTimestamp=")
            .append(registry.getLockTime())
            .append("\n");

        if (latestModelTypes.isEmpty()) {
            s.append("  <No models in registry>\n");
        } else {
            for (ModelType type : latestModelTypes) {
                ModelMetadata model = models.get(type);

                s.append("  type=")
                    .append(type.getType())
                    .append("\n    importTime=")
                    .append(model.getImportTime())
                    .append("\n    revision=")
                    .append(model.getModelRevision())
                    .append("\n    description=")
                    .append(model.getModelDescription())
                    .append("\n    locked=")
                    .append(model.isLocked())
                    .append("\n    lockTime=")
                    .append(model.getLockTime())
                    .append("\n\n");
            }
        }
        return s.toString();
    }

    /**
     * Export the current contents of the Data Model Registry to an XML file.
     */
    public void exportModelRegistry(String destinationPath) {
        File destinationFile = new File(destinationPath);
        if (destinationFile.exists() && destinationFile.isDirectory()) {
            throw new IllegalArgumentException(
                "destinationPath exists and is a directory: " + destinationFile);
        }

        ModelRegistry registry = modelCrud().retrieveCurrentRegistry();

        if (registry != null) {
            xmlManager().marshal(registry, destinationFile);
        } else {
            log.warn("Unable to export model registry as no registries present in database");
        }
    }

    public List<ModelType> allModelTypes() {
        return performTransaction(() -> modelCrud().retrieveAllModelTypes());
    }

    public ModelRegistry unlockedRegistry() {
        return performTransaction(() -> modelCrud().retrieveUnlockedRegistry());
    }

    public ModelRegistry lockCurrentRegistry() {
        return performTransaction(() -> modelCrud().lockCurrentRegistry());
    }

    public void persistModelMetadata(ModelMetadata modelMetadata) {
        performTransaction(() -> modelCrud().persist(modelMetadata));
    }

    public long mergeRegistryAndReturnUnlockedId(ModelRegistry modelRegistry) {
        return performTransaction(() -> modelCrud().merge(modelRegistry).getId());
    }

    /**
     * Constructs the {@link ValidatingXmlManager} when it is first used. This is necessary so that
     * the {@link ModelOperations} class can instantiate in a test environment, which may not have
     * access to the schema directory.
     */
    private ValidatingXmlManager<ModelRegistry> xmlManager() {
        if (xmlManager == null) {
            xmlManager = new ValidatingXmlManager<>(ModelRegistry.class);
        }
        return xmlManager;
    }

    public Map<String, ModelType> modelTypeMap() {
        return performTransaction(() -> modelCrud().retrieveModelTypeMap());
    }

    public ModelCrud modelCrud() {
        return modelCrud;
    }
}
