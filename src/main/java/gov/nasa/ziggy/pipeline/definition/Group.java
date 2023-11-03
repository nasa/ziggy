package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * Group identifier for {@link PipelineDefinition}s, {@link PipelineModuleDefinition}s,
 * {@link ParameterSet}s, and {@link PipelineInstance}s.
 * <p>
 * Groups are used in the console to organize these entities into folders since their numbers can
 * grow large over the course of the mission.
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_Group")
public class Group {
    public static final Group DEFAULT = new Group();

    @Id
    private String name;

    @ManyToOne
    private Group parentGroup;

    private String description;

    Group() {
    }

    public Group(String name) {
        this.name = name;
    }

    public Group(Group group) {
        name = group.name;
        description = group.description;
        parentGroup = group.parentGroup;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return name;
    }

    public Group getParentGroup() {
        return parentGroup;
    }

    public void setParentGroup(Group parentGroup) {
        this.parentGroup = parentGroup;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Group other = (Group) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }
}
