package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Implements unit tests for {@link AuditInfo}.
 */
public class AuditInfoTest {

    private static final long TEST_TIME = 42L;

    @Before
    public void setup() {
        SystemProxy.setUserTime(TEST_TIME);
    }

    /**
     * Tests getting the current user name when there is no
     * exception getting the information from the process
     * handle.
     */
    @Test
    public void testGetUserNameFromProcessHandle() {
        AuditInfo auditInfo = new AuditInfo();
        assertEquals(TEST_TIME, auditInfo.getLastChangedTime().getTime());
        assertEquals(ProcessHandle.current().info().user().get(), auditInfo.getLastChangedUser());
    }

    /**
     * Tests that if getting the user name from the process handle
     * fails, that the user name is set from the system properties.
     */
    @Test
    public void testGetUserNameFromProperties() {
        AuditInfo auditInfo = new AuditInfo() {
            @Override
            public String getUserFromProcessHandle() throws Throwable {
                throw new IOException("read error");
            }
        };
        assertEquals(TEST_TIME, auditInfo.getLastChangedTime().getTime());
        assertEquals(System.getProperty(PropertyName.USER_NAME.property()),
            auditInfo.getLastChangedUser());
    }

    /**
     * Tests that if both getting the user name from the process info and
     * from the system properties fails, that a default name is returned.
     */
    @Test
    public void testGetDefaultUserName() {
        AuditInfo auditInfo = new AuditInfo() {
            @Override
            public String getUserFromProcessHandle() throws Throwable {
                throw new IOException("read error");
            }
            @Override
            public String getUserFromProperties() throws SecurityException {
                throw new SecurityException("cannot access system properties");
            }
        };
        assertEquals(TEST_TIME, auditInfo.getLastChangedTime().getTime());
        assertEquals(AuditInfo.UNKNOWN_USER_NAME, auditInfo.getLastChangedUser());
    }
}
