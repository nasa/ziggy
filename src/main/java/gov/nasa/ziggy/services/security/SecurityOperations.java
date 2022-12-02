package gov.nasa.ziggy.services.security;

import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * @author Todd Klaus
 */
public class SecurityOperations {
    private UserCrud userCrud = null;
    private User user;

    public SecurityOperations() {
        userCrud = new UserCrud();
    }

    public SecurityOperations(DatabaseService databaseService) {
        userCrud = new UserCrud(databaseService);
    }

    public boolean validateLogin(String loginName) {
        user = userCrud.retrieveUser(loginName);
        return user != null;
    }

    public boolean hasPrivilege(User user, String privilege) {
        return user.hasPrivilege(privilege);
    }

    /**
     * Returns the user that was last validated with {@link #validateLogin}.
     *
     * @return the user object, or {@code null} if a user has not yet been validated
     */
    public User getCurrentUser() {
        return user;
    }
}
