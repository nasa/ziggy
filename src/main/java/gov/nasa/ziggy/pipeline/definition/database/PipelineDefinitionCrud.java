package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.HibernateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions_;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * Provides CRUD methods for {@link PipelineDefinition}
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineDefinitionCrud
    extends UniqueNameVersionPipelineComponentCrud<PipelineDefinition> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionCrud.class);

    public PipelineDefinitionCrud() {
    }

    public List<PipelineDefinition> retrieveAll() {
        return list(createZiggyQuery(PipelineDefinition.class));
    }

    @Override
    public PipelineDefinition retrieve(String name, int version) {
        return super.retrieve(name, version);
    }

    /**
     * Retrieves the names of all {@link PipelineDefinition}s that are associated with
     * {@link PipelineInstance}s.
     *
     * @return a non-{@code null} list of {@link PipelineDefinition} names.
     * @throws HibernateException if there were problems accessing the database.
     */
    public List<String> retrievePipelineDefinitionNamesInUse() {
        ZiggyQuery<PipelineDefinition, String> query = createZiggyQuery(PipelineDefinition.class,
            String.class);
        query.column(PipelineDefinition_.NAME).select();
        query.column(PipelineDefinition_.LOCKED).in(true);
        query.distinct(true);
        return list(query);
    }

    public void deletePipeline(PipelineDefinition pipelineDefinition) {
        if (pipelineDefinition.getVersion() > 0 || pipelineDefinition.isLocked()) {
            throw new PipelineException("Unable to remove " + componentNameForExceptionMessages()
                + " as " + pipelineDefinition.getName()
                + " is locked or its version is greater than zero");
        }

        // Must delete the nodes before deleting the pipeline because the cascade rules do not
        // include delete (having Cascade.ALL would cause errors in the console when manually
        // deleting individual nodes).
        deleteNodes(pipelineDefinition.getRootNodes());
        remove(pipelineDefinition);
    }

    /**
     * Delete all of the nodes in a pipeline and clear the rootNodes List.
     */
    public void deleteAllPipelineNodes(PipelineDefinition pipelineDefinition) {
        List<PipelineDefinitionNode> rootNodes = pipelineDefinition.getRootNodes();
        deleteNodes(rootNodes);
        pipelineDefinition.setRootNodes(Collections.emptyList());
    }

    /**
     * Recursively delete all of the nodes in a pipeline.
     *
     * @param rootNodes
     */
    private void deleteNodes(List<PipelineDefinitionNode> nodes) {
        for (PipelineDefinitionNode node : nodes) {
            deleteNodes(node.getNextNodes());
            remove(node);
        }
    }

    public boolean processingModeExistsInDatabase(String pipelineName) {
        return uniqueResult(createZiggyQuery(PipelineDefinitionProcessingOptions.class)
            .column(PipelineDefinitionProcessingOptions_.pipelineName)
            .in(pipelineName)) != null;
    }

    public ProcessingMode retrieveProcessingMode(String pipelineName) {
        return uniqueResult(
            createZiggyQuery(PipelineDefinitionProcessingOptions.class, ProcessingMode.class)
                .column(PipelineDefinitionProcessingOptions_.pipelineName)
                .in(pipelineName)
                .column(PipelineDefinitionProcessingOptions_.processingMode)
                .select());
    }

    public PipelineDefinitionProcessingOptions updateProcessingMode(String pipelineName,
        ProcessingMode processingMode) {
        PipelineDefinitionProcessingOptions pipelineDefinitionProcessingOptions = uniqueResult(
            createZiggyQuery(PipelineDefinitionProcessingOptions.class)
                .column(PipelineDefinitionProcessingOptions_.pipelineName)
                .in(pipelineName));
        pipelineDefinitionProcessingOptions.setProcessingMode(processingMode);
        return super.merge(pipelineDefinitionProcessingOptions);
    }

    public PipelineDefinition merge(PipelineDefinition pipelineDefinition) {
        if (!processingModeExistsInDatabase(pipelineDefinition.getName())) {
            persist(new PipelineDefinitionProcessingOptions(pipelineDefinition.getName()));
        }
        return super.merge(pipelineDefinition);
    }

    public List<PipelineDefinitionNode> retrieveRootNodes(PipelineDefinition pipelineDefinition) {
        ZiggyQuery<PipelineDefinition, PipelineDefinitionNode> query = createZiggyQuery(
            PipelineDefinition.class, PipelineDefinitionNode.class);
        query.column(PipelineDefinition_.id).in(pipelineDefinition.getId());
        query.column(PipelineDefinition_.rootNodes).select();
        return list(query);
    }

    public Set<String> retrieveParameterSetNames(PipelineDefinition pipelineDefinition) {
        ZiggyQuery<PipelineDefinition, String> query = createZiggyQuery(PipelineDefinition.class,
            String.class);
        query.column(PipelineDefinition_.id).in(pipelineDefinition.getId());
        query.column(PipelineDefinition_.parameterSetNames).select();
        return new HashSet<>(list(query));
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "pipeline definition";
    }

    @Override
    public Class<PipelineDefinition> componentClass() {
        return PipelineDefinition.class;
    }
}
