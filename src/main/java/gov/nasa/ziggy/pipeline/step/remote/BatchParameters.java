package gov.nasa.ziggy.pipeline.step.remote;

import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.step.remote.batch.SupportedBatchSystem;
import gov.nasa.ziggy.ui.pipeline.RemoteExecutionDialog;

/**
 * Calculates and stores parameters needed by a batch processing system. Implementations must supply
 * a no-arg constructor (see {@link SupportedBatchSystem}).
 *
 * @author PT
 */
public interface BatchParameters {

    /** Perform the conversion from execution resources to parameter values. */
    void computeParameterValues(PipelineNodeExecutionResources executionResources,
        int totalSubtasks);

    /** The name to be displayed in the {@link RemoteExecutionDialog}. Cannot be null. */
    String batchParameterSetName();

    /**
     * Parameter values and names for display on the {@link RemoteExecutionDialog}. The keys are in
     * a specified order which the caller should respect. Cannot be empty or null, but the values
     * can be empty or null.
     */
    Map<String, String> batchParametersByName(String costUnit);

    /**
     * The {@link PipelineNodeExecutionResources} used by the {@link BatchParameters} instance.
     * Never null.
     */
    PipelineNodeExecutionResources executionResources();

    /** Obtains the wall time request, in hours. */
    double requestedWallTimeHours();

    int activeCores();

    int nodeCount();

    /** Obtains the cost estimate for the job. */
    double estimatedCost();

    /**
     * Message for display on the {@link RemoteExecutionDialog}. Can be null if the
     * {@link BatchParameters} instance has values that are not calculated.
     */
    String displayMessage();
}
