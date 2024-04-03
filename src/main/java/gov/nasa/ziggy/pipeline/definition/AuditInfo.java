package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

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
 */
@Embeddable
public class AuditInfo {
    private final String lastChangedUser;
    private final Date lastChangedTime;

    public AuditInfo() {
        lastChangedTime = new Date();
        lastChangedUser = ProcessHandle.current().info().user().get();
    }

    public AuditInfo(Date lastChangedTime) {
        lastChangedUser = ProcessHandle.current().info().user().get();
        this.lastChangedTime = lastChangedTime;
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
}
