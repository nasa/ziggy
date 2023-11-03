package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.HibernateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
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
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionCrud.class);

    public PipelineDefinitionCrud() {
    }

    public List<PipelineDefinition> retrieveAll() {
        return list(createZiggyQuery(PipelineDefinition.class));
    }

    @Override
    protected void populateXmlFields(Collection<PipelineDefinition> results) {
        for (PipelineDefinition result : results) {
            result.populateXmlFields();
        }
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

    public void deleteAllVersionsForName(String name) {
        List<PipelineDefinition> allVersions = retrieveAllVersionsForName(name);

        for (PipelineDefinition pipelineDefinition : allVersions) {
            log.info("deleting existing pipeline def: " + pipelineDefinition);
            deletePipeline(pipelineDefinition);
        }
    }

    public void deletePipeline(PipelineDefinition pipelineDefinition) {
        /*
         * Must delete the nodes before deleting the pipeline because the cascade rules do not
         * include delete (having Cascade.ALL would cause errors in the console when manually
         * deleting individual nodes)
         */
        if (pipelineDefinition.isLocked()) {
            throw new PipelineException("Cannot delete locked version "
                + pipelineDefinition.getVersion() + " of pipeline " + pipelineDefinition.getName());
        }
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

    @Override
    public String componentNameForExceptionMessages() {
        return "pipeline definition";
    }

    @Override
    public Class<PipelineDefinition> componentClass() {
        return PipelineDefinition.class;
    }
}
