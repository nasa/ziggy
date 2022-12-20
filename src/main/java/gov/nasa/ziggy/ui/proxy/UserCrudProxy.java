package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

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
public class UserCrudProxy extends CrudProxy {
    public UserCrudProxy() {
    }

    public void saveRole(final Role role) {
        verifyPrivileges(Privilege.USER_ADMIN);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.createRole(role);
            return null;
        });
    }

    public Role retrieveRole(final String roleName) {
        verifyPrivileges(Privilege.USER_ADMIN);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            Role r = crud.retrieveRole(roleName);
            return r;
        });
    }

    public List<Role> retrieveAllRoles() {
        verifyPrivileges(Privilege.USER_ADMIN);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            List<Role> r = crud.retrieveAllRoles();
            return r;
        });
    }

    public void deleteRole(final Role role) {
        verifyPrivileges(Privilege.USER_ADMIN);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.deleteRole(role);
            return null;
        });
    }

    public void saveUser(final User user) {
        verifyPrivileges(Privilege.USER_ADMIN);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.createUser(user);
            return null;
        });
    }

    public User retrieveUser(final String loginName) {
        verifyPrivileges(Privilege.USER_ADMIN);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            User r = crud.retrieveUser(loginName);
            return r;
        });
    }

    public List<User> retrieveAllUsers() {
        verifyPrivileges(Privilege.USER_ADMIN);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            List<User> r = crud.retrieveAllUsers();
            return r;
        });
    }

    public void deleteUser(final User user) {
        verifyPrivileges(Privilege.USER_ADMIN);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            UserCrud crud = new UserCrud();
            crud.deleteUser(user);
            return null;
        });
    }
}
