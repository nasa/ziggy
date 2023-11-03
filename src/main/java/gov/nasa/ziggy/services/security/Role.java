package gov.nasa.ziggy.services.security;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_Role")
public class Role {
    @Id
    private String name;
    private Date created;

    @ManyToOne
    private User createdBy = null;

    @ElementCollection
    @JoinTable(name = "ziggy_Role_privileges")
    private List<String> privileges = new LinkedList<>();

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private final int dirty = 0;

    /**
     * Used only by the persistence layer
     */
    Role() {
    }

    public Role(String name) {
        this(name, null);
    }

    public Role(String name, User createdBy) {
        this.name = name;
        this.createdBy = createdBy;
        created = new Date(System.currentTimeMillis());
    }

    public List<String> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(List<String> privileges) {
        this.privileges = privileges;
    }

    public void addPrivilege(String privilege) {
        if (!hasPrivilege(privilege)) {
            privileges.add(privilege);
        }
    }

    public void addPrivileges(Role role) {
        List<String> privileges = role.getPrivileges();
        if (privileges != null) {
            for (String privilege : privileges) {
                addPrivilege(privilege);
            }
        }
    }

    public boolean hasPrivilege(String privilege) {
        return privileges.contains(privilege);
    }

    public String getName() {
        return name;
    }

    public void setName(String roleName) {
        name = roleName;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public User getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(User createdBy) {
        this.createdBy = createdBy;
    }

    public int getDirty() {
        return dirty;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Role other = (Role) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }
}
