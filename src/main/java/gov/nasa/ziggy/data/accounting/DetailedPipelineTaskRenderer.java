package gov.nasa.ziggy.data.accounting;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Writes out a task using PiplineTask.prettyPrint()
 *
 * @author Sean McCauliff
 */
public class DetailedPipelineTaskRenderer implements PipelineTaskRenderer {
    @Override
    public String renderTask(PipelineTask task) {
        return task.prettyPrint();
    }

    @Override
    public String renderDefaultTask() {
        return "Data Receipt";
    }
}
