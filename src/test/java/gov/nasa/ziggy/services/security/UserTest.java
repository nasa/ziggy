package gov.nasa.ziggy.services.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link User} class.
 *
 * @author Bill Wohler
 */
public class UserTest {
    private User user;

    @Before
    public void setUp() {
        user = createUser();
    }

    private User createUser() {
        return new User("jamesAdmin", "Administrator", "james@example.com", "x555");
    }

    @Test
    public void testConstructors() {
        User user = new User();
        assertTrue(user.getCreated() != null);

        user = createUser();
        assertEquals("jamesAdmin", user.getLoginName());
        assertEquals("Administrator", user.getDisplayName());
        assertEquals("james@example.com", user.getEmail());
        assertEquals("x555", user.getPhone());
    }

    @Test
    public void testLoginName() {
        assertEquals("jamesAdmin", user.getLoginName());
        String s = "a string";
        user.setLoginName(s);
        assertEquals(s, user.getLoginName());
    }

    @Test
    public void testDisplayName() {
        assertEquals("Administrator", user.getDisplayName());
        String s = "a string";
        user.setDisplayName(s);
        assertEquals(s, user.getDisplayName());
    }

    @Test
    public void testEmail() {
        assertEquals("james@example.com", user.getEmail());
        String s = "a string";
        user.setEmail(s);
        assertEquals(s, user.getEmail());
    }

    @Test
    public void testPhone() {
        assertEquals("x555", user.getPhone());
        String s = "a string";
        user.setPhone(s);
        assertEquals(s, user.getPhone());
    }

    @Test
    public void testRoles() {
        assertEquals(0, user.getRoles().size());

        Role role = new Role("operator");
        List<Role> rList = new LinkedList<>();
        rList.add(role);
        user.setRoles(rList);
        assertEquals(1, user.getRoles().size());
        assertEquals(role, user.getRoles().get(0));

        role = new Role("galley-slave");
        user.addRole(role);
        assertEquals(2, user.getRoles().size());
        assertEquals(role, user.getRoles().get(1));
    }

    @Test
    public void testPrivileges() {
        assertEquals(0, user.getPrivileges().size());

        String privilege = Privilege.PIPELINE_MONITOR.toString();
        List<String> pList = new LinkedList<>();
        pList.add(privilege);
        user.setPrivileges(pList);
        assertEquals(1, user.getPrivileges().size());
        assertEquals(privilege, user.getPrivileges().get(0));

        privilege = Privilege.PIPELINE_OPERATIONS.toString();
        user.addPrivilege(privilege);
        assertEquals(2, user.getPrivileges().size());
        assertEquals(privilege, user.getPrivileges().get(1));

        assertTrue(user.hasPrivilege(Privilege.PIPELINE_OPERATIONS.toString()));
        assertTrue(user.hasPrivilege(Privilege.PIPELINE_MONITOR.toString()));
        assertFalse(user.hasPrivilege(Privilege.PIPELINE_CONFIG.toString()));
        assertFalse(user.hasPrivilege(Privilege.USER_ADMIN.toString()));
    }

    @Test
    public void testCreated() {
        assertTrue(user.getCreated() != null);

        Date date = new Date(System.currentTimeMillis());
        user.setCreated(date);
        assertEquals(date, user.getCreated());
    }

    @Test
    public void testHashCode() {
        int hashCode = user.hashCode();
        hashCode = createUser().hashCode();
        assertEquals(hashCode, createUser().hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(user, createUser());
    }

    @Test
    public void testToString() {
        assertEquals("Administrator", user.toString());
    }
}
