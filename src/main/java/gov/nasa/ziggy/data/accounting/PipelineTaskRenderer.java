package gov.nasa.ziggy.data.accounting;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Used to print out a PipelineTask for the DataAccountabilityReport.
 *
 * @author Sean McCauliff
 */
public interface PipelineTaskRenderer {
    String renderTask(PipelineTask task);

    /**
     * Renderer for task zero and things that are not tasks.
     *
     * @return
     */
    String renderDefaultTask();
}
