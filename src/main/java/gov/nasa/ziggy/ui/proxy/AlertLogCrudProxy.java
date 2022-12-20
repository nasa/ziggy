package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * @author Todd Klaus
 */
public class AlertLogCrudProxy extends CrudProxy {

    public AlertLogCrudProxy() {
    }

    public List<AlertLog> retrieveForPipelineInstance(final long pipelineInstanceId) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            AlertLogCrud crud = new AlertLogCrud();
            List<AlertLog> result1 = crud.retrieveForPipelineInstance(pipelineInstanceId);
            return result1;
        });
    }
}
