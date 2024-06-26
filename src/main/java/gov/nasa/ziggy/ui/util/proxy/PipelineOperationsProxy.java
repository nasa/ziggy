package gov.nasa.ziggy.ui.util.proxy;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.TriggerValidationResults;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.messages.FireTriggerRequest;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;

/**
 * @author Todd Klaus
 */
public class PipelineOperationsProxy {
    private static final Logger log = LoggerFactory.getLogger(PipelineOperationsProxy.class);

    public PipelineOperationsProxy() {
    }

    public ParameterSet retrieveLatestParameterSet(final String parameterSetName) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.retrieveLatestParameterSet(parameterSetName);
        });
    }

    /**
     * @param instance
     * @return
     */
    public String generatePedigreeReport(final PipelineInstance instance) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.generatePedigreeReport(instance);
        });
    }

    /**
     * @param pipelineDefinition
     * @param destinationDirectory
     */
    public void exportPipelineParams(final PipelineDefinition pipelineDefinition,
        final File destinationDirectory) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            pipelineOps.exportPipelineParams(pipelineDefinition, destinationDirectory);

            return null;
        });
    }

    /**
     * @param pipelineDefinition
     * @return
     */
    public String generatePipelineReport(final PipelineDefinition pipelineDefinition) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.generatePipelineReport(pipelineDefinition);
        });
    }

    /**
     * Creates a textual report of all ParameterSets in the Parameter Library, including name, type,
     * keys and values.
     *
     * @param csvMode
     * @return
     */
    public String generateParameterLibraryReport(final boolean csvMode) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.generateParameterLibraryReport(csvMode);
        });
    }

    public ParameterSet updateParameterSet(final ParameterSet parameterSet,
        final Parameters newParameters, final String newDescription, final boolean forceSave) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.updateParameterSet(parameterSet, newParameters, newDescription,
                forceSave);
        });
    }

    // N.B.: there are two database transactions in the following method. This is because we
    // retrieve the latest version of the parameter set here, modify it so that it's got the updated
    // parameters, and then in UniqueNameVersionPipelineComponentCrud we compare the resulting
    // parameter set to the database version. If the two retrievals are in the same transaction, the
    // second retrieval gets the one from the Hibernate cache ... which is the first one ... which
    // is modified ... so when the comparison occurs, the two parameter sets are identical, which is
    // -- not what we wanted to know (we want to compare to the version actually in the database).
    // Hence, retrieve the parameter set in one transaction and merge in another.
    public ParameterSet updateParameterSet(String parameterSetName,
        ParametersInterface newParameters) {
        ParameterSet databaseParameterSet = CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new PipelineOperations().retrieveLatestParameterSet(parameterSetName));
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineOperations pipelineOps = new PipelineOperations();
            return pipelineOps.updateParameterSet(databaseParameterSet, newParameters, false);
        });
    }

    /**
     * Sends a start pipeline request message to the supervisor.
     */
    public void sendPipelineMessage(FireTriggerRequest pipelineRequest) {
        ZiggyMessenger.publish(pipelineRequest);
        // invalidate the models since starting a pipeline can change the locked state of versioned
        // database objects
        ZiggyMessenger.publish(new InvalidateConsoleModelsMessage());
    }

    /**
     * Sends a message to the supervisor requesting information on whether any pipelines are running
     * or queued.
     */
    public void sendRunningPipelinesCheckRequestMessage() {
        log.info("Sending message to request status of running instances");
        ZiggyMessenger.publish(new RunningPipelinesCheckRequest());
    }

    /**
     * Validates that this {@link PipelineDefinition} is valid for starting. Checks that the
     * associated pipeline definition objects have not changed in an incompatible way and that all
     * {@link ParameterSet}s are set.
     */
    public TriggerValidationResults validatePipeline(final PipelineDefinition pipelineDefinition) {
        return CrudProxyExecutor
            .executeSynchronous(() -> new PipelineOperations().validateTrigger(pipelineDefinition));
    }

    public TaskCounts taskCounts(PipelineInstanceNode node) {
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new PipelineOperations().taskCounts(node));
    }
}
