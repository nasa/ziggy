package gov.nasa.ziggy.pipeline.definition.crud;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.parameters.InternalParameters;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link ParameterSet}. Note: methods that return all parameter sets will
 * not return those that contain instances of {@link InternalParameters}, since these are supposed
 * to be "invisible" to users under normal circumstances (and in all events they aren't supposed to
 * be edited by users).
 *
 * @author Todd Klaus
 * @author PT
 */
public class ParameterSetCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetCrud.class);

    public ParameterSetCrud() {
    }

    public ParameterSetCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<ParameterSet> retrieveAllVersionsForName(String name) {
        Query q = createQuery("from ParameterSetName p where p.name = :name");
        q.setString("name", name);
        q.setMaxResults(1);

        ParameterSetName parameterSetName = uniqueResult(q);

        if (parameterSetName == null) {
            return new ArrayList<>();
        }

        q = createQuery("from ParameterSet p where p.name = :name order by version asc");
        q.setEntity("name", parameterSetName);

        List<ParameterSet> result = list(q);
        populateXmlFields(result);

        return result;
    }

    public List<ParameterSet> retrieveAll() {
        Query q = createQuery("from ParameterSet p order by name asc");

        List<ParameterSet> result = list(q);
        populateXmlFields(result);

        return visibleParameterSets(result);
    }

    public List<ParameterSet> visibleParameterSets(List<ParameterSet> allParameterSets) {
        return allParameterSets.stream()
            .filter(ParameterSet::visibleParameterSet)
            .collect(Collectors.toList());
    }

    public ParameterSet retrieveLatestVersionForName(String name) {
        Query q = createQuery("from ParameterSetName p where p.name = :name");
        q.setString("name", name);
        q.setMaxResults(1);
        ParameterSetName parameterSetName = uniqueResult(q);

        if (parameterSetName == null) {
            log.debug("No ParameterSetName found for name = " + name);
            return null;
        }

        ParameterSet result = retrieveLatestVersionForName(parameterSetName);
        populateXmlFields(result);
        return result;
    }

    public ParameterSet retrieve(long id) {
        Query q = createQuery("from ParameterSet p where p.id = :id");
        q.setLong("id", id);
        q.setMaxResults(1);

        ParameterSet result = uniqueResult(q);
        populateXmlFields(result);
        return result;
    }

    public ParameterSet retrieveLatestVersionForName(ParameterSetName name) {
        Query q = createQuery("from ParameterSet pmps where pmps.name = :name order by id desc");
        q.setEntity("name", name);
        q.setMaxResults(1);

        ParameterSet result = uniqueResult(q);
        populateXmlFields(result);
        return result;
    }

    public List<ParameterSet> retrieveLatestVersions() {
        Query q = createQuery("from ParameterSetName order by name");

        List<ParameterSetName> names = list(q);

        List<ParameterSet> results = new ArrayList<>();

        for (ParameterSetName name : names) {
            ParameterSet latestVersion = retrieveLatestVersionForName(name);

            if (latestVersion != null) {
                results.add(latestVersion);
            } else {
                log.debug("no versions found for name = " + name);
            }
        }
        populateXmlFields(results);
        return visibleParameterSets(results);
    }

    public void rename(ParameterSet parameterSet, String newName) {
        String oldName = parameterSet.getName().getName();

        // first, create the new name in PI_PS_NAME
        ParameterSetName newNameEntity = new ParameterSetName(newName);
        create(newNameEntity);

        // flush these changes so the updates below will see them
        flush();

        /*
         * @formatter:off
         *
         * second, update all references to the old name
         * This includes:
         *  TriggerDefinition (via PI_TD_PSN)
         *  TriggerDefinitionNode (via PI_TDN_MPS)
         *  PipelineInstance (via PI_INSTANCE_PS)
         *  PipelineInstanceNode (via PI_PIN_PI_MPS)
         *
         * @formatter:on
         */
        Query updateQuery1 = createSQLQuery(
            "update PI_PS set PI_PS_NAME_NAME = :newName where " + "PI_PS_NAME_NAME = :oldName");
        updateQuery1.setParameter("newName", newName);
        updateQuery1.setParameter("oldName", oldName);

        int updatedRows = updateQuery1.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_PS");

        Query updateQuery2 = createSQLQuery("update PI_TD_PSN set PI_PS_NAME_NAME = :newName where "
            + "PI_PS_NAME_NAME = :oldName");
        updateQuery2.setParameter("newName", newName);
        updateQuery2.setParameter("oldName", oldName);

        updatedRows = updateQuery2.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_TD_PSN");

        Query updateQuery3 = createSQLQuery(
            "update PI_TDN_MPS set PI_PS_NAME_NAME = :newName where "
                + "PI_PS_NAME_NAME = :oldName");
        updateQuery3.setParameter("newName", newName);
        updateQuery3.setParameter("oldName", oldName);

        updatedRows = updateQuery3.executeUpdate();

        log.debug("Updated " + updatedRows + " rows in PI_TDN_MPS");

        // flush these changes so the delete below will not fail due to a foreign key
        // constraint violation
        flush();

        Query deleteQuery = createSQLQuery("delete from PI_PS_NAME where NAME = :oldName");
        deleteQuery.setParameter("oldName", oldName);
        int deletedRows = deleteQuery.executeUpdate();
        log.debug("Deleted " + deletedRows + " rows in PI_PS_NAME");
    }

    /**
     * Retrieves the latest {@link RemoteParameters} from the database. These may be later than
     * those associated with the task to allow restarting the task with modified parameters.
     *
     * @param pipelineTask the non-{@code null} pipeline task to base the retrieval upon
     * @return the RemoteParameters to use
     */
    public RemoteParameters retrieveRemoteParameters(PipelineTask pipelineTask) {
        checkNotNull(pipelineTask, "pipelineTask");

        ParameterSet parameterSet = pipelineTask.getParameterSet(RemoteParameters.class, false);
        if (parameterSet == null) {
            return null;
        }
        ParameterSetName name = parameterSet.getName();
        ParameterSet latestParameterSet = retrieveLatestVersionForName(name);

        return latestParameterSet.parametersInstance();
    }

    /**
     * Retrieves the version number of the latest {@link RemoteParameters} from the database. This
     * may be later than the value associated with the task to allow restarting the task with
     * modified parameters.
     *
     * @param pipelineTask the non-{@code null} pipeline task to base the retrieval upon
     * @return the version number of the current {@link RemoteParameters} instance for the selected
     * task.
     */
    public int retrieveRemoteParameterVersionNumber(PipelineTask pipelineTask) {
        checkNotNull(pipelineTask, "pipelineTask");

        ParameterSet parameterSet = pipelineTask.getParameterSet(RemoteParameters.class);
        if (parameterSet == null) {
            return -1;
        }
        ParameterSetName name = parameterSet.getName();
        ParameterSet latestParameterSet = retrieveLatestVersionForName(name);

        return latestParameterSet.getVersion();
    }

    /**
     * Populates the XML fields for a {@link ParameterSet} instance. This ensures that the instance
     * has its XML and database fields consistent with each other upon retrieval from the database.
     */
    private void populateXmlFields(ParameterSet result) {
        if (result != null) {
            result.populateXmlFields();
        }
    }

    /**
     * Populates the XML fields for a {@link Collection} of {@link ParameterSet} instances. This
     * ensures that the instances have their XML and database fields consistent with each other upon
     * retrieval from the database.
     */
    private void populateXmlFields(Collection<ParameterSet> results) {
        for (ParameterSet result : results) {
            populateXmlFields(result);
        }
    }
}
