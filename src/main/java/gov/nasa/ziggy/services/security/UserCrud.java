package gov.nasa.ziggy.services.security;

import java.util.List;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * This class provides CRUD methods for the security entities ({@link User} and {@link Role}).
 *
 * @author Todd Klaus
 */
public class UserCrud extends AbstractCrud<User> {
    public UserCrud() {
    }

    public UserCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public void createUser(User user) {
        persist(user);
    }

    public List<User> retrieveAllUsers() {

        List<User> results = list(createZiggyQuery(User.class));
        for (User user : results) {
            Hibernate.initialize(user.getPrivileges());
            Hibernate.initialize(user.getRoles());
        }
        return results;
    }

    public User retrieveUser(String loginName) {
        ZiggyQuery<User, User> query = createZiggyQuery(User.class);
        query.column(User_.loginName).in(loginName);
        User user = uniqueResult(query);
        if (user != null) {
            Hibernate.initialize(user.getPrivileges());
            Hibernate.initialize(user.getRoles());
        }
        return user;
    }

    public void deleteUser(User user) {
        remove(user);
    }

    public void createRole(Role role) {
        persist(role);
    }

    public List<Role> retrieveAllRoles() {
        return list(createZiggyQuery(Role.class));
    }

    public Role retrieveRole(String roleName) {
        ZiggyQuery<Role, Role> query = createZiggyQuery(Role.class);
        query.column(Role_.name).in(roleName);
        return uniqueResult(query);
    }

    public void deleteRole(Role role) {
        remove(role);
    }

    @Override
    public Class<User> componentClass() {
        return User.class;
    }
}
