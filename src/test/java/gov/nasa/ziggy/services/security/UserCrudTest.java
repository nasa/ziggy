package gov.nasa.ziggy.services.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.hamcrest.core.IsInstanceOf;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.database.TestUtils;

public class UserCrudTest {
    private static final Logger log = LoggerFactory.getLogger(UserCrudTest.class);

    private UserCrud userCrud = null;

    private Role superUserRole;
    private Role operatorRole;
    private Role monitorRole;
    private User adminUser;
    private User joeOperator;
    private User maryMonitor;

    @Before
    public void setUp() throws Exception {
        TestUtils.setUpDatabase();
        userCrud = new UserCrud();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.tearDownDatabase();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private void createRoles(User createdBy) throws PipelineException {
        superUserRole = new Role("superuser", createdBy);
        operatorRole = new Role("operator", createdBy);
        monitorRole = new Role("monitor", createdBy);

        userCrud.createRole(superUserRole);
        userCrud.createRole(operatorRole);
        userCrud.createRole(monitorRole);
    }

    private void createAdminUser() throws PipelineException {
        adminUser = new User("admin", "Administrator", "admin@example.com", "x111");

        userCrud.createUser(adminUser);
    }

    private void seed() throws PipelineException {
        createAdminUser();
        createRoles(adminUser);

        adminUser.addRole(superUserRole);

        joeOperator = new User("joe", "Joe Operator", "joe@example.com", "x112");
        joeOperator.addRole(operatorRole);

        maryMonitor = new User("mary", "Mary Monitor", "mary@example.com", "x113");
        maryMonitor.addRole(monitorRole);

        userCrud.createUser(joeOperator);
        userCrud.createUser(maryMonitor);
    }

    @Test
    public void testCreateRetrieve() throws Exception {
        log.info("START TEST: testCreateRetrieve");

        DatabaseTransactionFactory.performTransaction(() -> {
            // store
            seed();

            // retrieve

            List<Role> roles = userCrud.retrieveAllRoles();

            for (Role role : roles) {
                log.info(role.toString());
            }

            assertEquals("BeforeCommit: roles.size()", 3, roles.size());
            assertTrue("BeforeCommit: contains superUserRole", roles.contains(superUserRole));
            assertTrue("BeforeCommit: contains operatorRole", roles.contains(operatorRole));
            assertTrue("BeforeCommit: contains monitorRole", roles.contains(monitorRole));

            List<User> users = userCrud.retrieveAllUsers();

            for (User user : users) {
                log.info(user.toString());
            }

            assertEquals("BeforeCommit: users.size()", 3, users.size());
            assertTrue("BeforeCommit: contains adminUser", users.contains(adminUser));
            assertTrue("BeforeCommit: contains joeOperator", users.contains(joeOperator));
            assertTrue("BeforeCommit: contains maryMonitor", users.contains(maryMonitor));

            assertEquals("AfterCommit: roles.size()", 3, roles.size());
            assertTrue("AfterCommit: contains superUserRole", roles.contains(superUserRole));
            assertTrue("AfterCommit: contains operatorRole", roles.contains(operatorRole));
            assertTrue("AfterCommit: contains monitorRole", roles.contains(monitorRole));

            assertEquals("AfterCommit: users.size()", 3, users.size());
            assertTrue("AfterCommit: contains adminUser", users.contains(adminUser));
            assertTrue("AfterCommit: contains joeOperator", users.contains(joeOperator));
            assertTrue("AfterCommit: contains maryMonitor", users.contains(maryMonitor));
            return null;
        });

    }

    @Test
    public void testDeleteRoleConstraintViolation() throws Throwable {
        expectedException
            .expectCause(IsInstanceOf.<Throwable> instanceOf(ConstraintViolationException.class));

        log.info("START TEST: testDeleteRoleConstraintViolation");

        DatabaseTransactionFactory.performTransaction(() -> {
            // store
            seed();

            // delete
            List<Role> roles = userCrud.retrieveAllRoles();

            /*
             * This should fail because there is a User (maryMonitor) using this Role
             */
            userCrud.deleteRole(roles.get(roles.indexOf(monitorRole)));
            return null;
        });
    }

    /**
     * Verify that we can delete a Role after we have deleted all users that reference that Role
     *
     * @throws Exception
     */
    @Test
    public void testDeleteUserAndRole() throws Exception {
        log.info("START TEST: testDeleteUserAndRole");

        DatabaseTransactionFactory.performTransaction(() -> {
            // store
            seed();

            // delete User

            List<User> users = userCrud.retrieveAllUsers();
            userCrud.deleteUser(users.get(users.indexOf(maryMonitor)));

            // delete Role

            List<Role> roles = userCrud.retrieveAllRoles();
            userCrud.deleteRole(roles.get(roles.indexOf(monitorRole)));

            // retrieve Users

            users = userCrud.retrieveAllUsers();

            for (User user : users) {
                log.info(user.toString());
            }

            assertEquals("users.size()", 2, users.size());
            assertTrue("contains adminUser", users.contains(adminUser));
            assertTrue("contains joeOperator", users.contains(joeOperator));

            // retrieve Roles
            roles = userCrud.retrieveAllRoles();
            assertEquals("AfterCommit: roles.size()", 2, roles.size());
            assertTrue("AfterCommit: contains superUserRole", roles.contains(superUserRole));
            assertTrue("AfterCommit: contains operatorRole", roles.contains(operatorRole));

            return null;
        });
    }
}
