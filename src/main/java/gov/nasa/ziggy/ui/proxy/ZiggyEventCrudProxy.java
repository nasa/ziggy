package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.services.events.ZiggyEvent;
import gov.nasa.ziggy.services.events.ZiggyEventCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

public class ZiggyEventCrudProxy extends CrudProxy {

    public List<ZiggyEvent> retrieveAllEvents() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new ZiggyEventCrud().retrieveAllEvents());
    }

}
