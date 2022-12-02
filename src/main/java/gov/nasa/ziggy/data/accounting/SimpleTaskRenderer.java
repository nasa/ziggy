package gov.nasa.ziggy.data.accounting;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Writes out a pipeline task using uow.briefState();
 *
 * @author Sean McCauliff
 */
public class SimpleTaskRenderer implements PipelineTaskRenderer {
    @Override
    public String renderTask(PipelineTask task) {
        try {
            return task.uowTaskInstance().briefState();
        } catch (PipelineException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String renderDefaultTask() {
        return "Data Receipt";
    }
}
