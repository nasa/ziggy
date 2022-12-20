package gov.nasa.ziggy.services.security;

import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Query;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * This class provides CRUD methods for the security entities ({@link User} and {@link Role}).
 *
 * @author Todd Klaus
 */
public class UserCrud extends AbstractCrud {
    public UserCrud() {
    }

    public UserCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public void createUser(User user) {
        create(user);
    }

    public List<User> retrieveAllUsers() {
        List<User> results = list(createQuery("from User"));
        for (User user : results) {
            Hibernate.initialize(user.getPrivileges());
            Hibernate.initialize(user.getRoles());
        }
        return results;
    }

    public User retrieveUser(String loginName) {
        Query query = createQuery("from User where loginName = :loginName");
        query.setString("loginName", loginName);
        User user = uniqueResult(query);
        if (user != null) {
            Hibernate.initialize(user.getPrivileges());
            Hibernate.initialize(user.getRoles());
        }
        return user;
    }

    public void deleteUser(User user) {
        delete(user);
    }

    public void createRole(Role role) {
        create(role);
    }

    public List<Role> retrieveAllRoles() {
        return list(createQuery("from Role"));
    }

    public Role retrieveRole(String roleName) {
        Query query = createQuery("from Role where name = :roleName");
        query.setString("roleName", roleName);
        return uniqueResult(query);
    }

    public void deleteRole(Role role) {
        delete(role);
    }

}
