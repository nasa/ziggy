package gov.nasa.ziggy.services.security;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * A user object.
 *
 * @author Bill Wohler
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_User")
public class User {

    @Id
    private String loginName;
    private String displayName;
    private String email;
    private String phone;
    private Date created;

    @ManyToMany
    @JoinTable(name = "ziggy_User_roles")
    private List<Role> roles = new ArrayList<>();

    @ElementCollection
    @JoinTable(name = "ziggy_User_privileges")
    private List<String> privileges = new ArrayList<>();

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private final int dirty = 0;

    public User() {
        this(null, null, null, null);
    }

    public User(String loginName, String displayName, String email, String phone) {
        this.loginName = loginName;
        this.displayName = displayName;
        this.email = email;
        this.phone = phone;
        created = new Date(System.currentTimeMillis());
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public List<Role> getRoles() {
        return roles;
    }

    public void setRoles(List<Role> roles) {
        this.roles = roles;
    }

    public void addRole(Role role) {
        roles.add(role);
    }

    public List<String> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(List<String> privileges) {
        this.privileges = privileges;
    }

    public void addPrivilege(String privilege) {
        privileges.add(privilege);
    }

    public boolean hasPrivilege(String privilege) {
        // First check for user-level override.
        if (privileges.contains(privilege)) {
            return true;
        }

        // Next check the user's roles.
        for (Role role : roles) {
            if (role.hasPrivilege(privilege)) {
                return true;
            }
        }

        // No matches.
        return false;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public int getDirty() {
        return dirty;
    }

    @Override
    public int hashCode() {
        return loginName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final User other = (User) obj;
        if (!Objects.equals(loginName, other.loginName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
