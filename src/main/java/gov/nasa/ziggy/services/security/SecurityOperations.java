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
        String name = loginName != null ? loginName : "";
        user = userCrud.retrieveUser(name);
        return user != null;
    }

    public boolean hasPrivilege(User user, String privilege) {
        String privilegeName = privilege != null ? privilege : "";
        return user.hasPrivilege(privilegeName);
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
