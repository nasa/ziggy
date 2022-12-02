package gov.nasa.ziggy.ui.proxy;

import java.io.File;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.TriggerValidationResults;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.models.DatabaseModelRegistry;

/**
 * @author Todd Klaus
 */
public class PipelineOperationsProxy extends CrudProxy {
    private static final Logger log = LoggerFactory.getLogger(PipelineOperationsProxy.class);

    public PipelineOperationsProxy() {
    }

    public ParameterSet retrieveLatestParameterSet(final ParameterSetName parameterSetName) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        ParameterSet result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                ParameterSet paramSet = pipelineOps.retrieveLatestParameterSet(parameterSetName);

                return paramSet;
            });
        return result;
    }

    /**
     * Returns a {@link Set} containing all {@link Parameters} classes required by the specified
     * node. This is a union of the Parameters classes required by the PipelineModule itself and the
     * Parameters classes required by the UnitOfWorkTaskGenerator associated with the node.
     *
     * @param pipelineNode
     * @return
     */
    public Set<ClassWrapper<Parameters>> retrieveRequiredParameterClassesForNode(
        final PipelineDefinitionNode pipelineNode) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        Set<ClassWrapper<Parameters>> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                Set<ClassWrapper<Parameters>> requiredParams = pipelineOps
                    .retrieveRequiredParameterClassesForNode(pipelineNode);

                return requiredParams;
            });
        return result;
    }

    /**
     * @param instance
     * @return
     */
    public String generatePedigreeReport(final PipelineInstance instance) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        String result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                String report = pipelineOps.generatePedigreeReport(instance);

                return report;
            });
        return result;
    }

    /**
     * @param pipelineDefinition
     * @param destinationDirectory
     */
    public void exportPipelineParams(final PipelineDefinition pipelineDefinition,
        final File destinationDirectory) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            pipelineOps.exportPipelineParams(pipelineDefinition, destinationDirectory);

            return null;
        });
    }

    /**
     * @param pipelineDefinition
     * @return
     */
    public String generateTriggerReport(final PipelineDefinition pipelineDefinition) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        String result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                String report = pipelineOps.generateTriggerReport(pipelineDefinition);

                return report;
            });
        return result;
    }

    /**
     * Creates a textual report of all ParameterSets in the Parameter Library, including name, type,
     * keys and values.
     *
     * @param csvMode
     * @return
     */
    public String generateParameterLibraryReport(final boolean csvMode) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        String result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                String report = pipelineOps.generateParameterLibraryReport(csvMode);

                return report;
            });
        return result;
    }

    public ParameterSet updateParameterSet(final ParameterSet parameterSet,
        final Parameters newParameters, final String newDescription, final boolean forceSave) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ParameterSet result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                ParameterSet updatedParamSet = pipelineOps.updateParameterSet(parameterSet,
                    newParameters, newDescription, forceSave);
                return updatedParamSet;
            });
        return result;
    }

    /**
     * Sends a fire-trigger request message to the worker.
     */
    public void sendTriggerMessage(WorkerFireTriggerRequest triggerRequest) {
        verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        PipelineOperations pipelineOps = new PipelineOperations();
        pipelineOps.sendTriggerMessage(triggerRequest);
        // invalidate the models since firing a trigger can change the locked state of versioned
        // database objects
        DatabaseModelRegistry.invalidateModels();
    }

    /**
     * Sends a message to the worker requesting information on whether any pipelines are running or
     * queued.
     */
    public void sendRunningPipelinesCheckRequestMessage() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        new PipelineOperations().sendRunningPipelinesCheckRequestMessage();
    }

    /**
     * Validates that this {@link PipelineDefinition} is valid for firing. Checks that the
     * associated pipeline definition objects have not changed in an incompatible way and that all
     * {@link ParameterSetName}s are set.
     */
    public TriggerValidationResults validateTrigger(final PipelineDefinition pipelineDefinition) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        TriggerValidationResults result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronous(() -> {
                PipelineOperations pipelineOps = new PipelineOperations();
                return pipelineOps.validateTrigger(pipelineDefinition);
            });
        return result;
    }
}
