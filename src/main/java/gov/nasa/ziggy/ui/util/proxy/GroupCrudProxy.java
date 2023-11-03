package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.crud.GroupCrud;
import gov.nasa.ziggy.services.security.Privilege;

public class GroupCrudProxy {
    public GroupCrudProxy() {
    }

    public void save(final Group group) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.persist(group);
            return null;
        });
    }

    public void delete(final Group group) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.remove(group);
            return null;
        });
    }

    public List<Group> retrieveAll() {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();

            return crud.retrieveAll();
        });
    }
}
