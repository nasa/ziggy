package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Utility functions for managing the parameter library.
 *
 * @author Forrest Girouard
 * @author Todd Klaus
 * @author PT
 */
public class ParametersOperations extends DatabaseOperations {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParametersOperations.class);

    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();

    public void save(ParameterSet parameterSet) {
        performTransaction(() -> parameterSetCrud.persist(parameterSet));
    }

    public ParameterSet rename(ParameterSet parameterSet, String newName) {
        return performTransaction(() -> parameterSetCrud.rename(parameterSet, newName));
    }

    public void delete(ParameterSet parameterSet) {
        performTransaction(() -> parameterSetCrud.remove(parameterSet));
    }

    /**
     * Populates the {@link Set} of {@link ParameterSet} instances for a pipeline instance or a
     * pipeline instance node, using the {@link Set} of {@link String} instances that represent the
     * parameter set names from the pipeline definition or pipeline definition node. The
     * parameterSets argument should be empty at call time.
     */
    public void bindParameterSets(Set<String> parameterSetNames, Set<ParameterSet> parameterSets) {

        // Add the current parameter sets to the instance and lock same.
        // NB: these CRUD calls do not need to be in a transaction because both of the callers
        // call the method from within a transaction.
        for (String parameterSetName : parameterSetNames) {
            ParameterSet paramSet = parameterSetCrud()
                .retrieveLatestVersionForName(parameterSetName);
            parameterSets.add(paramSet);
            paramSet.lock();
            parameterSetCrud().merge(paramSet);
        }
    }

    public ParameterSet parameterSet(String parameterSetName) {
        return performTransaction(
            () -> parameterSetCrud().retrieveLatestVersionForName(parameterSetName));
    }

    public List<ParameterSet> parameterSets() {
        return performTransaction(() -> parameterSetCrud().retrieveLatestVersions());
    }

    public List<ParameterSet> parameterSets(Collection<String> names) {
        return performTransaction(() -> parameterSetCrud().retrieveLatestVersions(names));
    }

    public List<String> parameterSetNames() {
        return performTransaction(() -> parameterSetCrud().retrieveNames());
    }

    public void lock(String parameterSetName) {
        performTransaction(() -> {
            ParameterSet databaseParameterSet = parameterSetCrud()
                .retrieveLatestVersionForName(parameterSetName);
            databaseParameterSet.lock();
            parameterSetCrud().merge(databaseParameterSet);
        });
    }

    public Set<ParameterSet> parameterSets(PipelineTask pipelineTask) {
        return performTransaction(() -> parameterSetCrud().retrieveParameterSets(pipelineTask));
    }

    public ParameterSet updateParameterSet(String parameterSetName, ParameterSet newParameters) {

        // N.B. Two database transactions are required here. This is because we retrieve the latest
        // version of the parameter set here, modify it so that it's got the updated parameters, and
        // then in UniqueNameVersionPipelineComponentCrud we compare the resulting parameter set to
        // the database version. If the two retrievals are in the same transaction, the second
        // retrieval gets the one from the Hibernate cache ... which is the first one ... which is
        // modified ... so when the comparison occurs, the two parameter sets are identical, which
        // is -- not what we wanted to know (we want to compare to the version actually in the
        // database). Hence, retrieve the parameter set in one transaction and merge in another.

        ParameterSet databaseParameterSet = parameterSet(parameterSetName);
        return updateParameterSet(databaseParameterSet, newParameters, false);
    }

    /**
     * Update the specified {@link ParameterSet},
     * <p>
     * If if the new parameter set is different than the one in the database, then apply the
     * changes. If locked, first create a new version.
     * <p>
     * The new ParameterSet version is returned if one was created, otherwise the old one is
     * returned.
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet, ParameterSet newParameters,
        boolean forceSave) {
        return updateParameterSet(parameterSet, newParameters.getParameters(),
            parameterSet.getDescription(), forceSave);
    }

    /**
     * Update the specified {@link ParameterSet},
     * <p>
     * If if the new parameter set is different than the one in the database, then apply the
     * changes. If locked, first create a new version.
     * <p>
     * The new ParameterSet version is returned if one was created, otherwise the old one is
     * returned.
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet, Set<Parameter> newParameters,
        String newDescription, boolean forceSave) {

        String currentDescription = parameterSet.getDescription();

        boolean descriptionChanged = false;
        if (currentDescription == null) {
            if (newDescription != null) {
                descriptionChanged = true;
            }
        } else if (!currentDescription.equals(newDescription)) {
            descriptionChanged = true;
        }

        if (!Parameter.identicalParameters(parameterSet.getParameters(), newParameters)
            || descriptionChanged || forceSave) {
            parameterSet.setParameters(newParameters);
            parameterSet.setDescription(newDescription);
            return performTransaction(() -> parameterSetCrud().merge(parameterSet));
        }
        return parameterSet;
    }

    public Set<ParameterSet> parameterSets(PipelineDefinition pipelineDefinition) {
        return performTransaction(() -> new HashSet<>(parameterSetCrud().retrieveLatestVersions(
            pipelineDefinitionCrud().retrieveParameterSetNames(pipelineDefinition))));
    }

    public Set<ParameterSet> parameterSets(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(() -> new HashSet<>(parameterSetCrud().retrieveLatestVersions(
            pipelineDefinitionNodeCrud().retrieveParameterSetNames(pipelineDefinitionNode))));
    }

    public Map<String, Parameter> nameToTypedPropertyMap(Set<Parameter> typedProperties) {
        Map<String, Parameter> nameToTypedProperty = new HashMap<>();
        for (Parameter property : typedProperties) {
            nameToTypedProperty.put(property.getName(), property);
        }
        return nameToTypedProperty;
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    ParameterSetCrud parameterSetCrud() {
        return parameterSetCrud;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        return pipelineDefinitionNodeCrud;
    }
}
