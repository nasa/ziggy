package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;

/**
 * @author Todd Klaus
 */
public class PipelineInstanceNodeCrudProxy {

    public PipelineInstanceNodeCrudProxy() {
    }

    public PipelineInstanceNode retrieve(final long id) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceNodeCrud crud = new PipelineInstanceNodeCrud();
            return crud.retrieve(id);
        });
    }

    public List<PipelineInstanceNode> retrieveAll(final PipelineInstance pipelineInstance) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            PipelineInstanceNodeCrud crud = new PipelineInstanceNodeCrud();
            return crud.retrieveAll(pipelineInstance);
        });
    }
}
