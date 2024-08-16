package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for {@link PipelineDefinition}s.
 *
 * @author PT
 */
public class PipelineDefinitionOperations extends DatabaseOperations {

    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();

    public PipelineDefinition pipelineDefinition(String pipelineName) {
        return performTransaction(
            () -> pipelineDefinitionCrud().retrieveLatestVersionForName(pipelineName));
    }

    /**
     * Retrieves the latest version of all pipeline definitions.
     */
    public List<PipelineDefinition> allPipelineDefinitions() {
        return performTransaction(() -> pipelineDefinitionCrud().retrieveLatestVersions());
    }

    public List<PipelineDefinition> allPipelineDefinitionsForName(String pipelineName) {
        return performTransaction(
            () -> pipelineDefinitionCrud().retrieveAllVersionsForName(pipelineName));
    }

    /** Returns the latest pipeline definition after first locking it. */
    public PipelineDefinition lockAndReturnLatestPipelineDefinition(String pipelineName) {
        return performTransaction(() -> {
            PipelineDefinition pipelineDefinition = pipelineDefinitionCrud()
                .retrieveLatestVersionForName(pipelineName);
            pipelineDefinition.lock();
            return pipelineDefinition;
        });
    }

    public PipelineDefinitionNode pipelineDefinitionNodeByName(
        PipelineDefinition pipelineDefinition, String name) {
        return performTransaction(() -> {
            PipelineDefinition retrievedDefinition = pipelineDefinitionCrud()
                .retrieve(pipelineDefinition.getName(), pipelineDefinition.getVersion());
            return findPipelineDefinitionNode(retrievedDefinition.getRootNodes(), name);
        });
    }

    private PipelineDefinitionNode findPipelineDefinitionNode(
        Collection<PipelineDefinitionNode> nodes, String name) {
        for (PipelineDefinitionNode node : nodes) {
            if (node.getModuleName().equals(name)) {
                return node;
            }
        }

        // If we got this far, then we didn't find it so we need to dig down into the
        // node tree.
        for (PipelineDefinitionNode node : nodes) {
            PipelineDefinitionNode foundNode = findPipelineDefinitionNode(node.getNextNodes(),
                name);
            if (foundNode != null) {
                return foundNode;
            }
        }

        // If we got this far, the node doesn't exist anywhere in the tree.
        return null;
    }

    public List<PipelineDefinitionNode> rootNodes(PipelineDefinition pipelineDefinition) {
        return performTransaction(
            () -> pipelineDefinitionCrud().retrieveRootNodes(pipelineDefinition));
    }

    public List<PipelineDefinition> pipelineDefinitions() {
        return performTransaction(() -> pipelineDefinitionCrud().retrieveAll());
    }

    public List<String> pipelineDefinitionNames() {
        return performTransaction(() -> pipelineDefinitionCrud().retrieveNames());
    }

    public List<PipelineDefinition> pipelineDefinitions(boolean pipelineNameProvided,
        String pipelineName) {
        List<PipelineDefinition> pipelineDefinitions = null;
        if (pipelineNameProvided) {
            pipelineDefinitions = performTransaction(
                () -> pipelineDefinitionCrud().retrieveAllVersionsForName(pipelineName));
        } else {
            pipelineDefinitions = performTransaction(() -> pipelineDefinitionCrud().retrieveAll());
        }
        return pipelineDefinitions;
    }

    public ProcessingMode processingMode(String pipelineName) {
        return performTransaction(
            () -> pipelineDefinitionCrud().retrieveProcessingMode(pipelineName));
    }

    public void updateProcessingMode(String pipelineName, ProcessingMode processingMode) {
        performTransaction(
            () -> pipelineDefinitionCrud().updateProcessingMode(pipelineName, processingMode));
    }

    public PipelineDefinition merge(PipelineDefinition pipeline) {
        return performTransaction(() -> pipelineDefinitionCrud().merge(pipeline));
    }

    public PipelineDefinition rename(PipelineDefinition pipeline, String newName) {
        return performTransaction(() -> pipelineDefinitionCrud().rename(pipeline, newName));
    }

    public void delete(PipelineDefinition pipeline) {
        performTransaction(() -> pipelineDefinitionCrud().deletePipeline(pipeline));
    }

    public void lock(PipelineDefinition pipelineDefinition) {
        performTransaction(() -> {
            PipelineDefinition databaseDefinition = pipelineDefinitionCrud()
                .retrieve(pipelineDefinition.getName(), pipelineDefinition.getVersion());
            databaseDefinition.lock();
            pipelineDefinitionCrud().merge(databaseDefinition);
        });
    }

    public Set<String> parameterSetNames(PipelineDefinition pipelineDefinition) {
        return performTransaction(
            () -> pipelineDefinitionCrud().retrieveParameterSetNames(pipelineDefinition));
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }

    PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }
}
