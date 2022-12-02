package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class PipelineInstanceNodeCrudProxy extends CrudProxy {

    public PipelineInstanceNodeCrudProxy() {
    }

    public PipelineInstanceNode retrieve(final long id) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        PipelineInstanceNode result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineInstanceNodeCrud crud = new PipelineInstanceNodeCrud();
                PipelineInstanceNode r = crud.retrieve(id);
                return r;
            });
        return result;
    }

    public List<PipelineInstanceNode> retrieveAll(final PipelineInstance pipelineInstance) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<PipelineInstanceNode> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                PipelineInstanceNodeCrud crud = new PipelineInstanceNodeCrud();
                List<PipelineInstanceNode> r = crud.retrieveAll(pipelineInstance);
                return r;
            });
        return result;
    }
}
