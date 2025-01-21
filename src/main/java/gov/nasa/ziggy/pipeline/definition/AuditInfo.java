package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.SystemProxy;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;

/**
 * This {@link Embeddable} class is used by {@link Entity} classes that can be modified through the
 * UI. It tracks who made the last modification to the object and and when those changes were made,
 * for audit trail purposes.
 * <p>
 * Instances of {@link AuditInfo} are immutable. When an instance is created, it is created with the
 * current date and current user. When a class that uses {@link AuditInfo} is updated, the existing
 * instance is replaced with a new one that contains the user and date.
 *
 * @author Todd Klaus
 * @author PT
 * @author M Rose
 */
@Embeddable
public class AuditInfo {

    private static final Logger log = LoggerFactory.getLogger(AuditInfo.class);

    /** A value to use when the user name cannot be determined. */
    static final String UNKNOWN_USER_NAME = "unknown";

    private final String lastChangedUser;
    private final Date lastChangedTime;

    /**
     * Creates a new instance with  last changed time equal to the current time.
     */
    public AuditInfo() {
        this.lastChangedTime = new Date(SystemProxy.currentTimeMillis());
        lastChangedUser = getUser();
    }

    /**
     * Gets the current user, or a default value if the user cannot be determined.
     * Logs any errors while determining the user.
     *
     * @return the current user name, as a string, or a default value
     */
    private String getUser() {
        // First try to get the user from the process handle.
        try {
            return getUserFromProcessHandle();
        } catch (Throwable e) {
            log.error("Unable to get current user from process handle", e);
        }

        // Then try the system properties.
        try {
            return getUserFromProperties();
        } catch (SecurityException e) {
            log.error("Unable to read current user name property", e);
        }

        // If all else fails, return a default value.
        return UNKNOWN_USER_NAME;
    }

    /**
     * @return the lastChangedTime
     */
    public Date getLastChangedTime() {
        return lastChangedTime;
    }

    /**
     * @return the lastChangedUser
     */
    public String getLastChangedUser() {
        return lastChangedUser;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastChangedTime, lastChangedUser);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final AuditInfo other = (AuditInfo) obj;
        if (!Objects.equals(lastChangedTime, other.lastChangedTime)
            || !Objects.equals(lastChangedUser, other.lastChangedUser)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /**
     * Gets the current effective user from the system process handle.
     * Note that this throws <code>Throwable</code> rather than a specified
     * exception, because a bug in OS X Java 17 causes a throw from the
     * <code>info()</code> method which is not documented. Default scope
     * for overriding during unit testing.
     *
     * @return the current effective user name
     * @throws Throwable if there is an error accessing the process info
     */
    String getUserFromProcessHandle() throws Throwable {
        return ProcessHandle.current().info().user().get();
    }

    /**
     * Gets the user from the system properties. Default scope for overriding
     * during unit testing.
     *
     * @return the user name
     * @throws SecurityException if the system property cannot be accessed
     */
    String getUserFromProperties() throws SecurityException {
        return System.getProperty(PropertyName.USER_NAME.property());
    }
}
