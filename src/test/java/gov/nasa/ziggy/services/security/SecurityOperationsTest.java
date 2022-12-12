package gov.nasa.ziggy.services.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Tests the {@link SecurityOperations} class.
 *
 * @author Bill Wohler
 */
public class SecurityOperationsTest {
    private SecurityOperations securityOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        securityOperations = new SecurityOperations();
    }

    private void populateObjects() {
        DatabaseTransactionFactory.performTransaction(() -> {
            TestSecuritySeedData testSecuritySeedData = new TestSecuritySeedData();
            testSecuritySeedData.loadSeedData();
            return null;
        });
    }

    @Test
    public void testValidateLogin() {
        // Don't need to test validateLogin(User, String) explicitly since that
        // method is tested indirectly by validateLogin(String, String).
        populateObjects();

        assertTrue("valid user/password", securityOperations.validateLogin("admin"));
        assertFalse("invalid user", securityOperations.validateLogin("foo"));
        assertFalse("null user", securityOperations.validateLogin((String) null));
        assertFalse("empty user", securityOperations.validateLogin(""));
    }

    @Test
    public void testHasPrivilege() {
        populateObjects();

        UserCrud userCrud = new UserCrud();
        User user = userCrud.retrieveUser("admin");
        assertTrue("admin has create",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_OPERATIONS.toString()));
        assertTrue("admin has modify",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_MONITOR.toString()));
        assertTrue("admin has monitor",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_CONFIG.toString()));
        assertTrue("admin has operations",
            securityOperations.hasPrivilege(user, Privilege.USER_ADMIN.toString()));

        user = userCrud.retrieveUser("joe");
        assertFalse("joe does not have create",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_OPERATIONS.toString()));
        assertFalse("joe does not have modify",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_MONITOR.toString()));
        assertTrue("joe has monitor",
            securityOperations.hasPrivilege(user, Privilege.PIPELINE_CONFIG.toString()));
        assertTrue("joe has operations",
            securityOperations.hasPrivilege(user, Privilege.USER_ADMIN.toString()));
    }

    @Test
    public void testGetCurrentUser() {
        populateObjects();
        assertNull("user is null", securityOperations.getCurrentUser());
        securityOperations.validateLogin("foo");
        assertNull("user is null", securityOperations.getCurrentUser());
        securityOperations.validateLogin("admin");
        assertEquals("admin", securityOperations.getCurrentUser().getLoginName());
        securityOperations.validateLogin("joe");
        assertEquals("joe", securityOperations.getCurrentUser().getLoginName());
    }
}
