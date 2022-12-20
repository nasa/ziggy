package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * {@link PipelineDefinition} uses a {@link PipelineDefinitionName} to refer to the
 * {@link PipelineDefinition} that is launched by that trigger. This is a 'soft reference' because
 * there may be several instances of the {@link PipelineDefinition} with the name in the
 * {@link PipelineDefinitionName} (one for each version). An association to a specific version
 * (usually latest version) is only made when a trigger is launched (the {@link PipelineInstance}
 * contains a hard reference to the {@link PipelineDefinition}).
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "PI_PD_NAME")
public class PipelineDefinitionName {
    @Id
    private String name;

    /**
     * For Hibernate use only
     */
    PipelineDefinitionName() {
    }

    /**
     * Constructors are package-level access because they should only be created by
     * {@link PipelineDefinition}
     *
     * @param name
     */
    public PipelineDefinitionName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
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
        PipelineDefinitionName other = (PipelineDefinitionName) obj;
        return Objects.equals(name, other.name);
    }

    /**
     * {@link XmlAdapter} implementation for {@link PipelineDefinitionName} instances.
     *
     * @author PT
     */
    public static class PipelineNameAdapter extends XmlAdapter<String, PipelineDefinitionName> {

        @Override
        public PipelineDefinitionName unmarshal(String v) throws Exception {
            return new PipelineDefinitionName(v);
        }

        @Override
        public String marshal(PipelineDefinitionName v) throws Exception {
            return v.toString();
        }

    }

}
