package gov.nasa.ziggy.pipeline.definition.crud;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.parameters.InternalParameters;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSet_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Provides CRUD methods for {@link ParameterSet}. Note: methods that return all parameter sets will
 * not return those that contain instances of {@link InternalParameters}, since these are supposed
 * to be "invisible" to users under normal circumstances (and in all events they aren't supposed to
 * be edited by users).
 *
 * @author Todd Klaus
 * @author PT
 */
public class ParameterSetCrud extends UniqueNameVersionPipelineComponentCrud<ParameterSet> {

    public ParameterSetCrud() {
    }

    public List<ParameterSet> retrieveAll() {
        List<ParameterSet> result = list(createZiggyQuery(ParameterSet.class));
        populateXmlFields(result);
        return visibleParameterSets(result);
    }

    public List<ParameterSet> visibleParameterSets(List<ParameterSet> allParameterSets) {
        return allParameterSets.stream()
            .filter(ParameterSet::visibleParameterSet)
            .collect(Collectors.toList());
    }

    public ParameterSet retrieve(long id) {
        ZiggyQuery<ParameterSet, ParameterSet> query = createZiggyQuery(ParameterSet.class);
        query.column(ParameterSet_.id).in(id);
        ParameterSet result = uniqueResult(query);
        populateXmlFields(result);
        return result;
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
        String name = parameterSet.getName();
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
        String name = parameterSet.getName();
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
    @Override
    protected void populateXmlFields(Collection<ParameterSet> results) {
        for (ParameterSet result : results) {
            populateXmlFields(result);
        }
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "parameter set";
    }

    @Override
    public Class<ParameterSet> componentClass() {
        return ParameterSet.class;
    }
}
