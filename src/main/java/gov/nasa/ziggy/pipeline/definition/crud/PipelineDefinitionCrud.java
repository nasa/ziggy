package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link PipelineDefinition}
 *
 * @author Todd Klaus
 */
public class PipelineDefinitionCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionCrud.class);

    public PipelineDefinitionCrud() {
    }

    public PipelineDefinitionCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<PipelineDefinition> retrieveAll() {
        Query query = createQuery("from PipelineDefinition");

        List<PipelineDefinition> results = list(query);

        return results;
    }

    private static void populateXmlFields(Collection<PipelineDefinition> results) {
        for (PipelineDefinition result : results) {
            result.populateXmlFields();
        }
    }

    public List<PipelineDefinition> retrieveAllVersionsForName(String name) {
        Query q = createQuery("from PipelineDefinitionName m where m.name = :name");
        q.setString("name", name);
        q.setMaxResults(1);

        PipelineDefinitionName pipelineDefName = uniqueResult(q);

        if (pipelineDefName == null) {
            return new ArrayList<>();
        }

        q = createQuery("from PipelineDefinition pd where pd.name = :name order by version asc");
        q.setEntity("name", pipelineDefName);

        List<PipelineDefinition> results = list(q);
        populateXmlFields(results);
        return results;
    }

    public PipelineDefinition retrieveLatestVersionForName(String name) {
        Query q = createQuery("from PipelineDefinitionName where name = :name");
        q.setString("name", name);
        q.setMaxResults(1);
        PipelineDefinitionName pipelineDefName = uniqueResult(q);

        if (pipelineDefName == null) {
            log.warn("No PipelineDefinitionName found for name = " + name);
            return null;
        }

        PipelineDefinition result = retrieveLatestVersionForName(pipelineDefName);
        return result;
    }

    public PipelineDefinition retrieveLatestVersionForName(PipelineDefinitionName name) {
        Query q = createQuery("from PipelineDefinition where name = :name order by version desc");
        q.setEntity("name", name);
        q.setMaxResults(1);

        PipelineDefinition result = uniqueResult(q);
        result.populateXmlFields();
        return result;
    }

    public List<PipelineDefinition> retrieveLatestVersions() {
        Query q = createQuery("from PipelineDefinitionName order by name");

        List<PipelineDefinitionName> names = list(q);

        List<PipelineDefinition> results = new ArrayList<>();

        for (PipelineDefinitionName name : names) {
            results.add(retrieveLatestVersionForName(name));
        }

        return results;
    }

    /**
     * Retrieves the unique list of names of all {@link PipelineDefinition}s.
     *
     * @return a non-{@code null} list of {@link PipelineDefinition} names.
     * @throws HibernateException if there were problems accessing the database.
     */
    public List<String> retrievePipelineDefinitionNames() {
        Query query = createQuery("select name from PipelineDefinitionName " + "order by name asc");

        List<String> results = list(query);

        return results;
    }

    /**
     * Retrieves the names of all {@link PipelineDefinition}s that are associated with
     * {@link PipelineInstance}s.
     *
     * @return a non-{@code null} list of {@link PipelineDefinition} names.
     * @throws HibernateException if there were problems accessing the database.
     */
    public List<String> retrievePipelineDefinitionNamesInUse() {
        Query query = createQuery(
            "select distinct pdn.name from PipelineDefinitionName pdn, PipelineInstance pinst "
                + "where pdn.name = pinst.pipelineDefinition.name order by pdn.name asc");

        List<String> names = list(query);

        return names;
    }

    public void deleteAllVersionsForName(String name) {
        List<PipelineDefinition> allVersions = retrieveAllVersionsForName(name);

        for (PipelineDefinition pipelineDefinition : allVersions) {
            log.info("deleting existing pipeline def: " + pipelineDefinition);
            deletePipeline(pipelineDefinition);
        }
    }

    public void deletePipeline(PipelineDefinition pipeline) {
        /*
         * Must delete the nodes before deleting the pipeline because the cascade rules do not
         * include delete (having Cascade.ALL would cause errors in the console when manually
         * deleting individual nodes)
         */
        deleteNodes(pipeline.getRootNodes());

        delete(pipeline);
    }

    /**
     * Delete all of the nodes in a pipeline and clear the rootNodes List.
     *
     * @param pipeline
     */
    public void deleteAllPipelineNodes(PipelineDefinition pipeline) {
        List<PipelineDefinitionNode> rootNodes = pipeline.getRootNodes();
        deleteNodes(rootNodes);
        pipeline.setRootNodes(Collections.emptyList());
    }

    /**
     * Recursively delete all of the nodes in a pipeline.
     *
     * @param rootNodes
     */
    private void deleteNodes(List<PipelineDefinitionNode> nodes) {
        for (PipelineDefinitionNode node : nodes) {
            deleteNodes(node.getNextNodes());
            delete(node);
        }
    }

    public void rename(PipelineDefinition pipelineDef, String newName) {
        String oldName = pipelineDef.getName().getName();

        // first, create the new name in PI_PD_NAME
        PipelineDefinitionName newNameEntity = new PipelineDefinitionName(newName);
        create(newNameEntity);

        // flush these changes so the updates below will see them
        flush();

        /*
         * @formatter:off
         *
         * second, update all references to the old name
         * This includes:
         *   PipelineDefinition
         *
         * @formatter:on
         */
        Query updateQuery1 = createSQLQuery(
            "update PI_PIPELINE_DEF set PI_PD_NAME_NAME = :newName where "
                + "PI_PD_NAME_NAME = :oldName");
        updateQuery1.setParameter("newName", newName);
        updateQuery1.setParameter("oldName", oldName);

        int updatedRows = updateQuery1.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_PIPELINE_DEF");

        // flush these changes so the delete below will not fail due to a foreign key
        // constraint violation
        flush();

        Query deleteQuery = createSQLQuery("delete from PI_PD_NAME where NAME = :oldName");
        deleteQuery.setParameter("oldName", oldName);
        int deletedRows = deleteQuery.executeUpdate();
        log.debug("Deleted " + deletedRows + " rows in PI_PD_NAME");
    }

}
