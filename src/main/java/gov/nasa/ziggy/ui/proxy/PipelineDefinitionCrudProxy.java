package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

public class PipelineDefinitionCrudProxy extends CrudProxy {
    public PipelineDefinitionCrudProxy() {
    }

    public void save(final PipelineDefinition pipeline) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            updateAuditInfo(pipeline.getAuditInfo());
            crud.create(pipeline);
            return null;
        });
    }

    public void rename(final PipelineDefinition pipeline, final String newName) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            updateAuditInfo(pipeline.getAuditInfo());
            crud.rename(pipeline, newName);
            return null;
        });
    }

    public void deletePipeline(final PipelineDefinition pipeline) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.deletePipeline(pipeline);
            return null;
        });
    }

    public void deletePipelineNode(final PipelineDefinitionNode pipelineNode) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.delete(pipelineNode);
            return null;
        });
    }

    public PipelineDefinition retrieveLatestVersionForName(final String name) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            PipelineDefinition result1 = crud.retrieveLatestVersionForName(name);
            result1.buildPaths();
            initializePipelineDefinitionNodes(result1);
            return result1;
        });
    }

    public PipelineDefinition retrieveLatestVersionForName(final PipelineDefinitionName name) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            PipelineDefinition result1 = crud.retrieveLatestVersionForName(name);
            result1.buildPaths();
            initializePipelineDefinitionNodes(result1);
            return result1;
        });
    }

    public List<PipelineDefinition> retrieveLatestVersions() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
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
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();

            List<PipelineDefinition> result1 = crud.retrieveAll();

            for (PipelineDefinition pipelineDefinition : result1) {
                pipelineDefinition.buildPaths();
                initializePipelineDefinitionNodes(pipelineDefinition);
            }

            return result1;
        });
    }

    public void saveChanges(PipelineDefinition pipelineDefinition) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        updateAuditInfo(pipelineDefinition.getAuditInfo());
        super.saveChanges();
    }

    @Override
    public void saveChanges() {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        super.saveChanges();
    }

    public void delete(final PipelineDefinition pipeline) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.delete(pipeline);
            return null;
        });
    }

    public void update(PipelineDefinition pipelineDefinition) {
        // TODO Auto-generated method stub
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineDefinitionCrud crud = new PipelineDefinitionCrud();
            crud.update(pipelineDefinition);
            return null;
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
