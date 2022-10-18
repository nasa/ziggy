package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * {@link PipelineDefinitionNode} uses a {@link ModuleName} to refer to the
 * {@link PipelineModuleDefinition} that corresponds to that node. This is a 'soft reference'
 * because there may be several instances of the {@link PipelineModuleDefinition} with the name in
 * the {@link ModuleName} (one for each version). An association to a specific version (usually
 * latest version) is only made when a pipeline instance is launched (the
 * {@link PipelineInstanceNode} contains a hard reference to the {@link PipelineModuleDefinition}).
 * <p>
 * This class is used instead of just using {@link String} in order to support referential integrity
 * in the database (both the {@link PipelineDefinitionNode} and the {@link PipelineModuleDefinition}
 * refer to the same {@link ModuleName} row)
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "PI_MOD_NAME")
public class ModuleName {
    @Id
    private String name;

    /**
     * For Hibernate use only
     */
    ModuleName() {
    }

    /**
     * Constructors are package-level access because they should only be created by
     * {@link PipelineModuleDefinition}
     *
     * @param name
     */
    public ModuleName(String name) {
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
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ModuleName other = (ModuleName) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

    /**
     * {@link XmlAdapter} implementation for {@link ModuleName} instances.
     *
     * @author PT
     */
    public static class ModuleNameAdapter extends XmlAdapter<String, ModuleName> {

        @Override
        public ModuleName unmarshal(String v) throws Exception {
            return new ModuleName(v);
        }

        @Override
        public String marshal(ModuleName v) throws Exception {
            return v.toString();
        }

    }
}
