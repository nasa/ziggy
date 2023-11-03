package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.services.security.User;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

/**
 * This {@link Embeddable} class is used by {@link Entity} classes that can be modified through the
 * UI. It tracks who made the last modification to the object and and when those changes were made,
 * for audit trail purposes.
 *
 * @author Todd Klaus
 */
@Embeddable
public class AuditInfo {
    @ManyToOne
    @JoinColumn(name = "lastChangedUser")
    private User lastChangedUser = null;
    @Column(name = "lastChangedTime")
    private Date lastChangedTime = null;

    public AuditInfo() {
        lastChangedTime = new Date();
    }

    public AuditInfo(User lastChangedUser, Date lastChangedTime) {
        this.lastChangedUser = lastChangedUser;
        this.lastChangedTime = lastChangedTime;
    }

    /**
     * @return the lastChangedTime
     */
    public Date getLastChangedTime() {
        return lastChangedTime;
    }

    /**
     * @param lastChangedTime the lastChangedTime to set
     */
    public void setLastChangedTime(Date lastChangedTime) {
        this.lastChangedTime = lastChangedTime;
    }

    /**
     * @return the lastChangedUser
     */
    public User getLastChangedUser() {
        return lastChangedUser;
    }

    /**
     * @param lastChangedUser the lastChangedUser to set
     */
    public void setLastChangedUser(User lastChangedUser) {
        this.lastChangedUser = lastChangedUser;
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
