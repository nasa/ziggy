package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceFinishedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;

/**
 * Operations class for methods mainly concerned with {@link PipelineInstance}s.
 *
 * @author PT
 */
public class PipelineInstanceOperations extends DatabaseOperations {

    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
    private ParametersOperations parametersOperations = new ParametersOperations();

    public List<PipelineInstance> pipelineInstances() {
        return performTransaction(() -> pipelineInstanceCrud().retrieveAll());
    }

    /**
     * Forces a {@link PipelineInstance} into the ERRORS_STALLED state. This should only be used in
     * the peculiar circumstance of an instance that has failed without any {@link PipelineTask}s
     * associated with the instance failing. This is the case when UOW generation for a given
     * {@link PipelineInstanceNode} has failed in some way.
     */
    public void setInstanceToErrorsStalledState(PipelineInstance pipelineInstance) {
        try {
            performTransaction(() -> {
                PipelineInstance.State.ERRORS_STALLED.setExecutionClockState(pipelineInstance);
                pipelineInstance.setState(PipelineInstance.State.ERRORS_STALLED);
                pipelineInstanceCrud().merge(pipelineInstance);
            });
        } finally {
            ZiggyMessenger.publish(new PipelineInstanceFinishedMessage());
        }
    }

    /** Merges a pipeline instance and returns the merged instance. */
    public PipelineInstance merge(PipelineInstance instance) {
        return performTransaction(() -> pipelineInstanceCrud().merge(instance));
    }

    /** Retrieves a specified pipeline instance. */
    public PipelineInstance pipelineInstance(long instanceId) {
        return performTransaction(() -> pipelineInstanceCrud().retrieve(instanceId));
    }

    /** Return all instances that match the specified filter. */
    public List<PipelineInstance> pipelineInstance(PipelineInstanceFilter filter) {
        return performTransaction(() -> pipelineInstanceCrud().retrieve(filter));
    }

    public List<PipelineInstanceNode> instanceNodes(PipelineInstance instance) {
        return performTransaction(() -> pipelineInstanceNodeCrud()
            .retrieveAll(pipelineInstanceCrud().retrieve(instance.getId())));
    }

    public Set<ParameterSet> parameterSets(PipelineInstance pipelineInstance) {
        return performTransaction(
            () -> pipelineInstanceCrud().retrieveParameterSets(pipelineInstance));
    }

    public List<PipelineInstanceNode> rootNodes(PipelineInstance pipelineInstance) {
        return performTransaction(() -> pipelineInstanceCrud().retrieveRootNodes(pipelineInstance));
    }

    /**
     * Returns a {@link TaskCounts} instance for a given {@link PipelineInstance}.
     */
    public TaskCounts taskCounts(PipelineInstance pipelineInstance) {
        return performTransaction(() -> new TaskCounts(
            pipelineTaskDisplayDataOperations().pipelineTaskDisplayData(pipelineInstance)));
    }

    public void addRootNode(PipelineInstance instance, PipelineInstanceNode pipelineInstanceNode) {
        performTransaction(() -> {
            PipelineInstance databaseInstance = pipelineInstance(instance.getId());
            databaseInstance.getRootNodes().add(pipelineInstanceNode);
            databaseInstance.getPipelineInstanceNodes().add(pipelineInstanceNode);
            pipelineInstanceCrud().merge(databaseInstance);
        });
    }

    public void addPipelineInstanceNode(PipelineInstance instance,
        PipelineInstanceNode pipelineInstanceNode) {
        performTransaction(() -> {
            PipelineInstance databaseInstance = pipelineInstance(instance.getId());
            databaseInstance.getPipelineInstanceNodes().add(pipelineInstanceNode);
            pipelineInstanceCrud().merge(databaseInstance);
        });
    }

    public PipelineInstance bindParameterSets(PipelineDefinition pipelineDefinition,
        PipelineInstance pipelineInstance) {
        return performTransaction(() -> {
            PipelineInstance databaseInstance = pipelineInstance.getId() == null
                ? merge(pipelineInstance)
                : pipelineInstance(pipelineInstance.getId());
            PipelineDefinition databaseDefinition = pipelineDefinitionCrud()
                .retrieve(pipelineDefinition.getName(), pipelineDefinition.getVersion());
            parametersOperations().bindParameterSets(databaseDefinition.getParameterSetNames(),
                databaseInstance.getParameterSets());
            return merge(databaseInstance);
        });
    }

    public boolean validInstanceId(long pipelineInstanceId) {
        return performTransaction(() -> pipelineInstanceId > 0
            && pipelineInstanceId <= pipelineInstanceCrud().retrieveMaxInstanceId());
    }

    public long instanceId(long pipelineInstanceId, String moduleName) {
        return validInstanceId(pipelineInstanceId) ? pipelineInstanceId
            : performTransaction(
                () -> pipelineInstanceCrud().retrieveInstanceIdOfLatestForModule(moduleName));
    }

    public boolean tasksInInstance(long pipelineInstanceId, String moduleName) {
        if (validInstanceId(pipelineInstanceId)) {
            return performTransaction(() -> !CollectionUtils.isEmpty(pipelineTaskCrud()
                .retrieveTasksForModuleAndInstance(moduleName, pipelineInstanceId)));
        }
        return performTransaction(
            () -> pipelineTaskCrud().retrieveLatestForModule(moduleName) != null);
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }
}
