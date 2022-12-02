package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.crud.GroupCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

public class GroupCrudProxy extends CrudProxy {
    public GroupCrudProxy() {
    }

    public void save(final Group group) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.create(group);
            return null;
        });
    }

    public void delete(final Group group) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.delete(group);
            return null;
        });
    }

    public List<Group> retrieveAll() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<Group> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                GroupCrud crud = new GroupCrud();

                List<Group> result1 = crud.retrieveAll();

                return result1;
            });
        return result;
    }
}
