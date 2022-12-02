package gov.nasa.ziggy.services.security;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * This class populates seed data for {@link User}s and {@link Role}s.
 */
public class TestSecuritySeedData {
    private final UserCrud userCrud;

    public TestSecuritySeedData() {
        userCrud = new UserCrud();
    }

    /**
     * Loads initial security data into {@link User} and {@link Role} tables. Use
     * {@link #deleteAllUsersAndRoles()} to clear these tables before running this method. The
     * caller is responsible for calling {@link DatabaseService#beginTransaction()} and
     * {@link DatabaseService#commitTransaction()}.
     *
     * @throws PipelineException if there were problems inserting records into the database.
     */
    public void loadSeedData() {
        insertAll();
    }

    private void insertAll() {
        Role superRole = new Role("Super User");
        for (Privilege privilege : Privilege.values()) {
            superRole.addPrivilege(privilege.toString());
        }
        userCrud.createRole(superRole);

        Role opsRole = new Role("Pipeline Operator");
        opsRole.addPrivilege(Privilege.USER_ADMIN.toString());
        opsRole.addPrivilege(Privilege.PIPELINE_CONFIG.toString());
        userCrud.createRole(opsRole);

        Role managerRole = new Role("Pipeline Manager");
        managerRole.addPrivilege(Privilege.PIPELINE_OPERATIONS.toString());
        managerRole.addPrivilege(Privilege.PIPELINE_MONITOR.toString());
        userCrud.createRole(managerRole);

        User admin = new User("admin", "Administrator", "admin@example.com", "x111");
        admin.addRole(superRole);
        userCrud.createUser(admin);

        User joeOps = new User("joe", "Joe Operator", "joe@example.com", "x222");
        joeOps.addRole(opsRole);
        userCrud.createUser(joeOps);

        User tonyOps = new User("tony", "Tony Trainee", "tony@example.com", "x444");
        // Since Tony is only a trainee, we'll just give him monitor privs for
        // now...
        tonyOps.addPrivilege(Privilege.PIPELINE_CONFIG.toString());
        userCrud.createUser(tonyOps);

        User MaryMgr = new User("mary", "Mary Manager", "mary@example.com", "x333");
        MaryMgr.addRole(managerRole);
        userCrud.createUser(MaryMgr);
    }

    public void deleteAllUsersAndRoles() {
        for (User user : userCrud.retrieveAllUsers()) {
            userCrud.deleteUser(user);
        }
        for (Role role : userCrud.retrieveAllRoles()) {
            userCrud.deleteRole(role);
        }
    }

    /**
     * This function runs the tests declared in this class.
     *
     * @param args
     * @throws PipelineException
     */
    public static void main(String[] args) {
        TestSecuritySeedData testSecuritySeedData = new TestSecuritySeedData();

        DatabaseTransactionFactory.performTransaction(() -> {
            testSecuritySeedData.loadSeedData();
            return null;
        });
    }
}
