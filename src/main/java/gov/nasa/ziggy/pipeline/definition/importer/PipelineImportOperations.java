package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.database.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepCrud;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.PipelineStepExecutionResources;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironmentOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventOperations;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Provides operations methods for the import of pipeline definitions.
 *
 * @author PT
 */
public class PipelineImportOperations extends DatabaseOperations {

    private static final Logger log = LoggerFactory.getLogger(PipelineImportOperations.class);

    private PipelineStepCrud pipelineStepCrud = new PipelineStepCrud();
    private PipelineCrud pipelineCrud = new PipelineCrud();
    private PipelineNodeCrud pipelineNodeCrud = new PipelineNodeCrud();
    private ParametersOperations parametersOperations = new ParametersOperations();
    private DatastoreOperations datastoreOperations = new DatastoreOperations();
    private RemoteEnvironmentOperations remoteEnvironmentOperations = new RemoteEnvironmentOperations();
    private ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
    private ZiggyEventOperations ziggyEventOperations = new ZiggyEventOperations();

    /** Persist all the data objects that define a cluster. */
    public void persistClusterDefinition(List<ParameterSetDescriptor> parameterSetDescriptors,
        DatastoreImportConditioner datastoreImportConditioner,
        List<ZiggyEventHandler> eventHandlers,
        Map<PipelineStep, PipelineStepExecutionResources> resourcesByPipelineStep,
        Map<Pipeline, Set<PipelineNodeExecutionResources>> resourcesByNode,
        List<RemoteEnvironment> remoteEnvironments) {
        performTransaction(() -> {

            // Parameters.
            if (!CollectionUtils.isEmpty(parameterSetDescriptors)) {
                persistParameterSets(parameterSetDescriptors);
            }

            // Datastore configuration (data file types, model types, datastore nodes,
            // datastore regexps).
            if (datastoreImportConditioner != null) {
                Set<DatastoreNode> datastoreNodesToRemove = new HashSet<>();
                for (String fullPathForNodeToRemove : datastoreImportConditioner
                    .getFullPathsForNodesToRemove()) {
                    datastoreNodesToRemove
                        .add(datastoreImportConditioner.getDatabaseDatastoreNodesByFullPath()
                            .get(fullPathForNodeToRemove));
                }
                datastoreOperations().persistDatastoreConfiguration(
                    datastoreImportConditioner.getDataFileTypes(),
                    datastoreImportConditioner.getModelTypes(),
                    datastoreImportConditioner.getRegexps(), datastoreNodesToRemove,
                    datastoreImportConditioner.nodesForDatabase(), log);
            }
            // Pipeline steps.
            if (resourcesByPipelineStep != null && !resourcesByPipelineStep.isEmpty()) {
                for (Map.Entry<PipelineStep, PipelineStepExecutionResources> entry : resourcesByPipelineStep
                    .entrySet()) {
                    pipelineStepCrud().merge(entry.getValue());
                    pipelineStepCrud().merge(entry.getKey());
                }
            }

            // Pipelines.
            if (resourcesByNode != null && !resourcesByNode.isEmpty()) {
                for (Map.Entry<Pipeline, Set<PipelineNodeExecutionResources>> entry : resourcesByNode
                    .entrySet()) {
                    pipelineCrud().merge(entry.getKey());
                    for (PipelineNodeExecutionResources resources : entry.getValue()) {
                        pipelineNodeCrud().merge(resources);
                    }
                }
            }

            // Event handlers.
            if (!CollectionUtils.isEmpty(eventHandlers)) {
                for (ZiggyEventHandler eventHandler : eventHandlers) {
                    ziggyEventOperations().mergeEventHandler(eventHandler);
                }
            }

            // Remote environments.
            if (!CollectionUtils.isEmpty(remoteEnvironments)) {
                for (RemoteEnvironment remoteEnvironment : remoteEnvironments) {
                    remoteEnvironmentOperations().merge(remoteEnvironment);
                }
            }
        });
    }

    public void persistParameterSets(List<ParameterSetDescriptor> parameterSetDescriptors) {
        for (ParameterSetDescriptor descriptor : parameterSetDescriptors) {
            if (!descriptor.getState().needsPersisting()) {
                continue;
            }
            if (descriptor.getState().equals(ParameterSetDescriptor.State.CREATE)) {
                if (descriptor.getParameterSet().isPartial()) {
                    throw new PipelineException(
                        "Cannot override non-existent parameter set " + descriptor.getName());
                }
                createNewParameterSet(descriptor);
            }
            if (descriptor.getState().equals(ParameterSetDescriptor.State.UPDATE)) {
                ParameterSet currentParamSet = performTransaction(
                    () -> parameterSetCrud().retrieveLatestVersionForName(descriptor.getName()));
                if (descriptor.getParameterSet().isPartial()) {
                    overrideExistingParameters(currentParamSet, descriptor);
                } else {
                    replaceExistingParameters(currentParamSet, descriptor);
                }
            }
        }
    }

    /**
     * Creates a new named parameter set in the database.
     */
    private void createNewParameterSet(ParameterSetDescriptor desc) {

        // Make certain that the ParameterSetDescriptor is valid.
        desc.validate();

        String name = desc.getName();
        desc.setState(ParameterSetDescriptor.State.CREATE);

        // Construct an instance of the Parameters class that has all the fields.
        log.debug("Adding {} as it is not yet in parameter library", name);
        ParameterSet newParamSet = new ParameterSet(name);
        newParamSet.setDescription("Created by importParameterLibrary @ " + new Date());
        newParamSet.setAlgorithmInterfaceName(desc.getAlgorithmInterfaceName());
        newParamSet.setParameters(desc.getImportedProperties());
        performTransaction(() -> parameterSetCrud().merge(newParamSet));
    }

    /**
     * Applies overrides to an existing parameter set. In this case, the parameter values in the
     * {@link ParameterSetDescriptor} overwrite parameter values in the database, but any parameters
     * that are not specified in the {@link ParameterSetDescriptor} keep their database values.
     */
    private void overrideExistingParameters(ParameterSet databaseParameterSet,
        ParameterSetDescriptor desc) {

        ParameterSet mergedParameterSet = mergeParameterSets(databaseParameterSet, desc);

        // Make maps from the property names to the two sets of properties (imported and merged).
        Map<String, Parameter> importedProperties = parameterOperations()
            .nameToTypedPropertyMap(desc.getImportedProperties());
        Map<String, Parameter> mergedProperties = parameterOperations()
            .nameToTypedPropertyMap(mergedParameterSet.getParameters());
        Set<Parameter> libraryProperties = new HashSet<>();

        // Find the merged properties that have no corresponding imported ones and capture them
        // as library properties
        for (String name : mergedProperties.keySet()) {
            if (!importedProperties.containsKey(name)) {
                libraryProperties.add(mergedProperties.get(name));
            }
        }
        desc.setLibraryProps(ParameterSetImportConditioner.formatProps(libraryProperties));

        // If the parameter values have changed, commit the new parameters to the database
        parameterOperations().updateParameterSet(databaseParameterSet, mergedParameterSet, false);
    }

    private ParameterSet mergeParameterSets(ParameterSet databaseParameterSet,
        ParameterSetDescriptor desc) {

        // Here we need to validate the descriptor against the fields of the database parameter
        // set, and we can't yet validate the primitive types of the typed properties:
        desc.validateAgainstParameterSet(databaseParameterSet);

        // Next we need to create a new ParameterSet instance that's a copy of the
        // database instance and copy new values into it.
        ParameterSet mergedParameterSet = databaseParameterSet.newInstance();
        mergedParameterSet.setParameters(databaseParameterSet.copyOfParameters());

        // Make maps of the parameters by name from both imported and merged parameter sets.
        Map<String, Parameter> importedParameterByName = parameterOperations()
            .nameToTypedPropertyMap(desc.getImportedProperties());
        Map<String, Parameter> mergedParameterByName = parameterOperations()
            .nameToTypedPropertyMap(mergedParameterSet.getParameters());

        // Find the merged properties that correspond to the imported ones and copy over the
        // imported value.
        for (String name : importedParameterByName.keySet()) {
            mergedParameterByName.get(name)
                .setString(importedParameterByName.get(name).getString());
        }
        return mergedParameterSet;
    }

    /**
     * Performs complete replacement of an existing parameter set with a new one.
     * <p>
     * The replacement allows not only the values but also the parameters themselves. The original
     * parameters (names, types, and values) are replaced. The current library parameters are not
     * copied to the new parameter set. For partial parameter set replacement (i.e., keep some
     * values and replace others), use parameter overrides.
     */
    private void replaceExistingParameters(ParameterSet databaseParameterSet,
        ParameterSetDescriptor desc) {

        // Make certain that the ParameterSetDescriptor is valid.
        desc.validate();

        // perform updates as needed.
        parameterOperations().updateParameterSet(databaseParameterSet, desc.getParameterSet(),
            false);
    }

    PipelineStepCrud pipelineStepCrud() {
        return pipelineStepCrud;
    }

    PipelineCrud pipelineCrud() {
        return pipelineCrud;
    }

    PipelineNodeCrud pipelineNodeCrud() {
        return pipelineNodeCrud;
    }

    ParametersOperations parameterOperations() {
        return parametersOperations;
    }

    DatastoreOperations datastoreOperations() {
        return datastoreOperations;
    }

    ParameterSetCrud parameterSetCrud() {
        return parameterSetCrud;
    }

    ZiggyEventOperations ziggyEventOperations() {
        return ziggyEventOperations;
    }

    RemoteEnvironmentOperations remoteEnvironmentOperations() {
        return remoteEnvironmentOperations;
    }
}
