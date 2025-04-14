package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for {@link Pipeline}s.
 *
 * @author PT
 */
public class PipelineOperations extends DatabaseOperations {

    private PipelineCrud pipelineCrud = new PipelineCrud();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();

    public Pipeline pipeline(String pipelineName) {
        return performTransaction(() -> pipelineCrud().retrieveLatestVersionForName(pipelineName));
    }

    /**
     * Retrieves the latest version of all pipelines.
     */
    public List<Pipeline> allPipelines() {
        return performTransaction(() -> pipelineCrud().retrieveLatestVersions());
    }

    public List<Pipeline> allPipelinesForName(String pipelineName) {
        return performTransaction(() -> pipelineCrud().retrieveAllVersionsForName(pipelineName));
    }

    /** Returns the latest pipeline after first locking it. */
    public Pipeline lockAndReturnLatestPipeline(String pipelineName) {
        return performTransaction(() -> {
            Pipeline pipeline = pipelineCrud().retrieveLatestVersionForName(pipelineName);
            pipeline.lock();
            return pipeline;
        });
    }

    public PipelineNode pipelineNodeByName(Pipeline pipeline, String name) {
        return performTransaction(() -> {
            Pipeline retrievedPipeline = pipelineCrud().retrieve(pipeline.getName(),
                pipeline.getVersion());
            return findPipelineNode(retrievedPipeline.getRootNodes(), name);
        });
    }

    private PipelineNode findPipelineNode(Collection<PipelineNode> nodes, String name) {
        for (PipelineNode node : nodes) {
            if (node.getPipelineStepName().equals(name)) {
                return node;
            }
        }

        // If we got this far, then we didn't find it so we need to dig down into the
        // node tree.
        for (PipelineNode node : nodes) {
            PipelineNode foundNode = findPipelineNode(node.getNextNodes(), name);
            if (foundNode != null) {
                return foundNode;
            }
        }

        // If we got this far, the node doesn't exist anywhere in the tree.
        return null;
    }

    public List<PipelineNode> rootNodes(Pipeline pipeline) {
        return performTransaction(() -> pipelineCrud().retrieveRootNodes(pipeline));
    }

    public List<Pipeline> pipelines() {
        return pipelines(false);
    }

    /**
     * @param initializeNodes if true, the list of {@link PipelineNode}s is initialized from the
     * database.
     */
    public List<Pipeline> pipelines(boolean initializeNodes) {
        return performTransaction(() -> {
            List<Pipeline> pipelines = pipelineCrud().retrieveAll();
            if (!initializeNodes) {
                return pipelines;
            }
            for (Pipeline pipeline : pipelines) {
                for (PipelineNode pipelineNode : pipeline.getRootNodes()) {
                    initializePipelineNode(pipelineNode);
                }
            }
            return pipelines;
        });
    }

    private void initializePipelineNode(PipelineNode pipelineNode) {
        Hibernate.initialize(pipelineNode.getInputDataFileTypes());
        Hibernate.initialize(pipelineNode.getOutputDataFileTypes());
        Hibernate.initialize(pipelineNode.getModelTypes());
        Hibernate.initialize(pipelineNode.getParameterSetNames());
        for (PipelineNode childNode : pipelineNode.getNextNodes()) {
            initializePipelineNode(childNode);
        }
    }

    public List<String> pipelineNames() {
        return performTransaction(() -> pipelineCrud().retrieveNames());
    }

    public List<Pipeline> pipelines(boolean pipelineNameProvided, String pipelineName) {
        List<Pipeline> pipelines = null;
        if (pipelineNameProvided) {
            pipelines = performTransaction(
                () -> pipelineCrud().retrieveAllVersionsForName(pipelineName));
        } else {
            pipelines = performTransaction(() -> pipelineCrud().retrieveAll());
        }
        return pipelines;
    }

    public ProcessingMode processingMode(String pipelineName) {
        return performTransaction(() -> pipelineCrud().retrieveProcessingMode(pipelineName));
    }

    public void updateProcessingMode(String pipelineName, ProcessingMode processingMode) {
        performTransaction(() -> pipelineCrud().updateProcessingMode(pipelineName, processingMode));
    }

    public Pipeline merge(Pipeline pipeline) {
        return performTransaction(() -> pipelineCrud().merge(pipeline));
    }

    public void lock(Pipeline pipeline) {
        performTransaction(() -> {
            Pipeline localPipeline = pipelineCrud().retrieve(pipeline.getName(),
                pipeline.getVersion());
            localPipeline.lock();
            pipelineCrud().merge(localPipeline);
        });
    }

    public Set<String> parameterSetNames(Pipeline pipeline) {
        return performTransaction(() -> pipelineCrud().retrieveParameterSetNames(pipeline));
    }

    PipelineCrud pipelineCrud() {
        return pipelineCrud;
    }

    PipelineNodeOperations pipelineNodeOperations() {
        return pipelineNodeOperations;
    }
}
