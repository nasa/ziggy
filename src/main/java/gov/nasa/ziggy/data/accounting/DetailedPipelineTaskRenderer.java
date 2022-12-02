package gov.nasa.ziggy.data.accounting;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Writes out a task using PiplineTask.prettyPrint()
 *
 * @author Sean McCauliff
 */
public class DetailedPipelineTaskRenderer implements PipelineTaskRenderer {
    @Override
    public String renderTask(PipelineTask task) {
        try {
            return task.prettyPrint();
        } catch (PipelineException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String renderDefaultTask() {
        return "Data Receipt";
    }
}
