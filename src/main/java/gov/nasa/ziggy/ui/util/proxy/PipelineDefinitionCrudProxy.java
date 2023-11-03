package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.services.security.Privilege;

public class PipelineDefinitionCrudProxy
    extends RetrieveLatestVersionsCrudProxy<PipelineDefinition> {

    public PipelineDefinitionCrudProxy() {
    }

    public PipelineDefinition rename(final PipelineDefinition pipeline, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            updateAuditInfo(pipeline.getAuditInfo());
            return crud.rename(pipeline, newName);
        });
    }

    public void deletePipeline(final PipelineDefinition pipeline) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.deletePipeline(pipeline);
            return null;
        });
    }

    public void deletePipelineNode(final PipelineDefinitionNode pipelineNode) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.remove(pipelineNode);
            return null;
        });
    }

    public PipelineDefinition retrieveLatestVersionForName(final String name) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
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
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
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
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
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
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
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
}
