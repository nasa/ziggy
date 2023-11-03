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
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;

/**
 * Operations class for the model metadata registry. This registry maintains metadata about models
 * in the system for data accountability purposes.
 *
 * @author Todd Klaus
 */
public class ModelRegistryOperations {
    private static final Logger log = LoggerFactory.getLogger(ModelRegistryOperations.class);

    private ValidatingXmlManager<ModelRegistry> xmlManager;

    public ModelRegistryOperations() {
        xmlManager = new ValidatingXmlManager<>(ModelRegistry.class);
    }

    /**
     * Returns a String containing a textual report of the latest version of the metadata for all
     * models in the registry.
     */
    public String report() {
        ModelCrud modelCrud = new ModelCrud();
        ModelRegistry registry = modelCrud.retrieveCurrentRegistry();

        if (registry != null) {
            return report(registry);
        }
        return "No Model Registry found";
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
        StringBuilder sb = new StringBuilder();

        Map<ModelType, ModelMetadata> models = registry.getModels();
        List<ModelType> latestModelTypes = new LinkedList<>(models.keySet());
        Collections.sort(latestModelTypes);

        sb.append("version=" + registry.getId() + ", locked=" + registry.isLocked()
            + ", lockTimestamp=" + registry.getLockTime() + "\n");

        if (latestModelTypes.isEmpty()) {
            sb.append("  <No models in registry>\n");
        } else {
            for (ModelType type : latestModelTypes) {
                ModelMetadata model = models.get(type);

                sb.append("  type=" + type.getType() + "\n");
                sb.append("    importTime=" + model.getImportTime() + "\n");
                sb.append("    revision=" + model.getModelRevision() + "\n");
                sb.append("    description=" + model.getModelDescription() + "\n");
                sb.append("    locked=" + model.isLocked() + "\n");
                sb.append("    lockTime=" + model.getLockTime() + "\n");
                sb.append("\n");
            }
        }
        return sb.toString();
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

        ModelCrud modelCrud = new ModelCrud();
        ModelRegistry registry = modelCrud.retrieveCurrentRegistry();

        if (registry != null) {
            xmlManager.marshal(registry, destinationFile);
        } else {
            log.warn("Unable to export model registry as no registries present in database");
        }
    }
}
