package gov.nasa.ziggy.pipeline.definition;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;

/**
 * Group identifier for {@link PipelineDefinition}s, {@link PipelineModuleDefinition}s, and
 * {@link ParameterSet}s.
 * <p>
 * Groups are used in the console to organize these entities into folders since their numbers can
 * grow large over the course of the mission.
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_Group", uniqueConstraints = { @UniqueConstraint(columnNames = { "name" }) })

public class Group {
    public static final Group DEFAULT = new Group();

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_Group_generator")
    @SequenceGenerator(name = "ziggy_Group_generator", initialValue = 1,
        sequenceName = "ziggy_Group_sequence", allocationSize = 1)
    private Long id;

    private String name;

    @ManyToOne
    private Group parentGroup;

    private String description;

    @ElementCollection
    @JoinTable(name = "ziggy_Group_pipelineDefinitionNames")
    private Set<String> pipelineDefinitionNames = new HashSet<>();

    @ElementCollection
    @JoinTable(name = "ziggy_Group_pipelineModuleNames")
    private Set<String> pipelineModuleNames = new HashSet<>();

    @ElementCollection
    @JoinTable(name = "ziggy_Group_parameterSetNames")
    private Set<String> parameterSetNames = new HashSet<>();

    /** Contains whichever of the above is correct for the current use-case. */
    @Transient
    private Set<String> memberNames;

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

    public Set<String> getPipelineDefinitionNames() {
        return pipelineDefinitionNames;
    }

    public void setPipelineDefinitionNames(Set<String> pipelineDefinitionNames) {
        this.pipelineDefinitionNames = pipelineDefinitionNames;
    }

    public Set<String> getPipelineModuleNames() {
        return pipelineModuleNames;
    }

    public void setPipelineModuleNames(Set<String> pipelineModuleNames) {
        this.pipelineModuleNames = pipelineModuleNames;
    }

    public Set<String> getParameterSetNames() {
        return parameterSetNames;
    }

    public void setParameterSetNames(Set<String> parameterSetNames) {
        this.parameterSetNames = parameterSetNames;
    }

    public Set<String> getMemberNames() {
        return memberNames;
    }

    public void setMemberNames(Set<String> memberNames) {
        this.memberNames = memberNames;
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
