package gov.nasa.ziggy.pipeline.definition;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

/**
 * Groups are used in the console to organize items into folders since their numbers can grow large
 * over the course of the mission.
 * <p>
 * The names associated with a {@link Group} are represented as element collections with eager
 * fetching because we generally need all of the names whenever we access a group.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@Entity
@Table(name = "ziggy_Group",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "type" }) })

public class Group {
    public static final String DEFAULT_NAME = "<Default Group>";
    public static final Group DEFAULT = new Group(DEFAULT_NAME, null);

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_Group_generator")
    @SequenceGenerator(name = "ziggy_Group_generator", initialValue = 1,
        sequenceName = "ziggy_Group_sequence", allocationSize = 1)
    private Long id;

    private String name;

    private String type;

    @ManyToOne
    private Group parent;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_Group_children")
    private Set<Group> children = new HashSet<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_Group_items")
    private Set<String> items = new HashSet<>();

    // For Hibernate.
    Group() {
    }

    public Group(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        if (name == null) {
            return "";
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Group getParent() {
        return parent;
    }

    public void setParent(Group parentGroup) {
        parent = parentGroup;
    }

    public Set<Group> getChildren() {
        return children;
    }

    public void setChildren(Set<Group> children) {
        this.children = children;
    }

    public Set<String> getItems() {
        return items;
    }

    public void setItems(Set<String> items) {
        this.items = items;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Group other = (Group) obj;
        return Objects.equals(name, other.name) && Objects.equals(type, other.type);
    }

    @Override
    public String toString() {
        return name;
    }
}
