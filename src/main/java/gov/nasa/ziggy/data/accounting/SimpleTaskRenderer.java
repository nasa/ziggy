package gov.nasa.ziggy.data.accounting;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Writes out a pipeline task using uow.briefState();
 *
 * @author Sean McCauliff
 */
public class SimpleTaskRenderer implements PipelineTaskRenderer {
    @Override
    public String renderTask(PipelineTask task) {
        return task.getUnitOfWork().briefState();
    }

    @Override
    public String renderDefaultTask() {
        return "Data Receipt";
    }
}
