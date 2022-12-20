package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.ModuleName;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link PipelineModuleDefinition}
 *
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(PipelineModuleDefinitionCrud.class);

    public PipelineModuleDefinitionCrud() {
    }

    public PipelineModuleDefinitionCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<PipelineModuleDefinition> retrieveAll() {
        return list(createQuery("from PipelineModuleDefinition pmd order by pmd.name"));
    }

    public List<PipelineModuleDefinition> retrieveAllVersionsForName(String name) {
        Query q = createQuery("from ModuleName m where m.name = :name");
        q.setString("name", name);
        q.setMaxResults(1);

        ModuleName moduleName = uniqueResult(q);

        if (moduleName == null) {
            return new ArrayList<>();
        }

        q = createQuery(
            "from PipelineModuleDefinition p where p.name = :name order by version asc");
        q.setEntity("name", moduleName);

        return list(q);
    }

    public PipelineModuleDefinition retrieveLatestVersionForName(String name) {
        Query q = createQuery("from ModuleName m where m.name = :name");
        q.setString("name", name);
        q.setMaxResults(1);
        ModuleName moduleName = uniqueResult(q);

        if (moduleName == null) {
            log.info("No ModuleName found for name = " + name);
            return null;
        }

        return retrieveLatestVersionForName(moduleName);
    }

    public PipelineModuleDefinition retrieveLatestVersionForName(ModuleName name) {
        Query q = createQuery(
            "from PipelineModuleDefinition pmd where pmd.name = :name order by version desc");
        q.setEntity("name", name);
        q.setMaxResults(1);

        return uniqueResult(q);
    }

    public List<PipelineModuleDefinition> retrieveLatestVersions() {
        Query q = createQuery("from ModuleName order by name");

        List<ModuleName> names = list(q);

        List<PipelineModuleDefinition> results = new ArrayList<>();

        for (ModuleName name : names) {
            results.add(retrieveLatestVersionForName(name));
        }

        return results;
    }

    public void rename(PipelineModuleDefinition moduleDef, String newName) {
        String oldName = moduleDef.getName().getName();

        // first, create the new name in PI_MOD_NAME
        ModuleName newNameEntity = new ModuleName(newName);
        create(newNameEntity);

        // flush these changes so the updates below will see them
        flush();

        /*
         * @formatter:off
         *
         * second, update all references to the old name
         * This includes:
         *   PipelineModuleDefinition
         *   PipelineDefinitionNode
         *   PipelineInstanceNode
         *   TriggerDefinitionNode
         *   MrReport
         *
         * @formatter:on
         */
        Query updateQuery1 = createSQLQuery(
            "update PI_MOD_DEF set PI_MOD_NAME_NAME = :newName where "
                + "PI_MOD_NAME_NAME = :oldName");
        updateQuery1.setParameter("newName", newName);
        updateQuery1.setParameter("oldName", oldName);

        int updatedRows = updateQuery1.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_MOD_DEF");

        Query updateQuery2 = createSQLQuery(
            "update PI_PIPELINE_DEF_NODE set PI_MOD_NAME_NAME = :newName where "
                + "PI_MOD_NAME_NAME = :oldName");
        updateQuery2.setParameter("newName", newName);
        updateQuery2.setParameter("oldName", oldName);

        updatedRows = updateQuery2.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_PIPELINE_DEF_NODE");

        Query updateQuery3 = createSQLQuery(
            "update PI_TRIGGER_DEF_NODE set PI_MOD_NAME_NAME = :newName where "
                + "PI_MOD_NAME_NAME = :oldName");
        updateQuery3.setParameter("newName", newName);
        updateQuery3.setParameter("oldName", oldName);

        updatedRows = updateQuery3.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_TRIGGER_DEF_NODE");

        Query updateQuery4 = createSQLQuery(
            "update MR_REPORT set PI_MOD_NAME_NAME = :newName where "
                + "PI_MOD_NAME_NAME = :oldName");
        updateQuery4.setParameter("newName", newName);
        updateQuery4.setParameter("oldName", oldName);

        updatedRows = updateQuery4.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in MR_REPORT");

        // flush these changes so the delete below will not fail due to a foreign key
        // constraint violation
        flush();

        Query deleteQuery = createSQLQuery("delete from PI_MOD_NAME where NAME = :oldName");
        deleteQuery.setParameter("oldName", oldName);
        int deletedRows = deleteQuery.executeUpdate();
        log.debug("Deleted " + deletedRows + " rows in PI_MOD_NAME");
    }
}
