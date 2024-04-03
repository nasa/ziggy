package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogCrud;

/**
 * @author Todd Klaus
 */
public class AlertLogCrudProxy {

    public AlertLogCrudProxy() {
    }

    public List<AlertLog> retrieveForPipelineInstance(final long pipelineInstanceId) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            AlertLogCrud crud = new AlertLogCrud();
            return crud.retrieveForPipelineInstance(pipelineInstanceId);
        });
    }
}
