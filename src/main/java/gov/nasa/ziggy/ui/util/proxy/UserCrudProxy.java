package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;

/**
 * This proxy class provides wrappers for the CRUD methods in {@link UserCrud} to support 'off-line'
 * conversations (modifications to persisted objects without immediate db updates) The pattern is
 * similar for all CRUD operations:
 *
 * <pre>
 *
 * 1- start a transaction
 * 2- invoke real CRUD method
 * 3- call Session.flush()
 * 4- commit the transaction
 * </pre>
 *
 * This class assumes that auto-flushing has been turned off for the current session by the
 * application before calling this class.
 *
 * @author Todd Klaus
 */
public class UserCrudProxy {
    public UserCrudProxy() {
    }

    public void saveRole(final Role role) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.createRole(role);
            return null;
        });
    }

    public Role retrieveRole(final String roleName) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            return crud.retrieveRole(roleName);
        });
    }

    public List<Role> retrieveAllRoles() {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            return crud.retrieveAllRoles();
        });
    }

    public void deleteRole(final Role role) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.deleteRole(role);
            return null;
        });
    }

    public void saveUser(final User user) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.createUser(user);
            return null;
        });
    }

    public User retrieveUser(final String loginName) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            return crud.retrieveUser(loginName);
        });
    }

    public List<User> retrieveAllUsers() {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            return crud.retrieveAllUsers();
        });
    }

    public void deleteUser(final User user) {
        CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.deleteUser(user);
            return null;
        });
    }
}
