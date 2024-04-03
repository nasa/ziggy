package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;

public class PipelineDefinitionCrudProxy
    extends RetrieveLatestVersionsCrudProxy<PipelineDefinition> {

    public PipelineDefinitionCrudProxy() {
    }

    public PipelineDefinition rename(final PipelineDefinition pipeline, final String newName) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            return crud.rename(pipeline, newName);
        });
    }

    public void deletePipeline(final PipelineDefinition pipeline) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.deletePipeline(pipeline);
            return null;
        });
    }

    public void deletePipelineNode(final PipelineDefinitionNode pipelineNode) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.remove(pipelineNode);
            return null;
        });
    }

    public PipelineDefinition retrieveLatestVersionForName(final String name) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            PipelineDefinition result1 = crud.retrieveLatestVersionForName(name);
            result1.buildPaths();
            initializePipelineDefinitionNodes(result1);
            return result1;
        });
    }

    @Override
    public List<PipelineDefinition> retrieveLatestVersions() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();

            List<PipelineDefinition> result1 = crud.retrieveLatestVersions();

            for (PipelineDefinition pipelineDefinition : result1) {
                pipelineDefinition.buildPaths();
                initializePipelineDefinitionNodes(pipelineDefinition);
            }

            return result1;
        });
    }

    public List<PipelineDefinition> retrieveAll() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();

            List<PipelineDefinition> result1 = crud.retrieveAll();

            for (PipelineDefinition pipelineDefinition : result1) {
                pipelineDefinition.buildPaths();
                initializePipelineDefinitionNodes(pipelineDefinition);
            }

            return result1;
        });
    }

    @Override
    public PipelineDefinition update(PipelineDefinition entity) {
        return createOrUpdate(entity);
    }

    public PipelineDefinition createOrUpdate(PipelineDefinition entity) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            return crud.merge(entity);
        });
    }

    private void initializePipelineDefinitionNodes(PipelineDefinition pipelineDefinition) {
        List<PipelineDefinitionNode> nodes = pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            Hibernate.initialize(node.getInputDataFileTypes());
            Hibernate.initialize(node.getOutputDataFileTypes());
            Hibernate.initialize(node.getModelTypes());
        }
    }

    public ProcessingMode retrieveProcessingMode(String pipelineName) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            return crud.retrieveProcessingMode(pipelineName);
        });
    }

    public void updateProcessingMode(String pipelineName, ProcessingMode processingMode) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.updateProcessingMode(pipelineName, processingMode);
            return null;
        });
    }
}
