package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.adapters.XmlAdapter;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.xml.XmlReference;

/**
 * {@link PipelineDefinition} and {@link PipelineDefinitionNode} use {@link ParameterSetName} to
 * refer to the {@link ParameterSet}s that are used by those definitions ({@link Parameters} or
 * {@link Parameters}).
 * <p>
 * This is a 'soft reference' because there may be several instances of the {@link ParameterSet}
 * with the name in the {@link ParameterSetName} (one for each version). An association to a
 * specific version (usually latest version) is only made when a pipeline instance is launched (the
 * {@link PipelineInstance} and {@link PipelineInstanceNode} contain hard references to the
 * {@link ParameterSet}s).
 * <p>
 * This class is used instead of just using {@link String} in order to support referential integrity
 * in the database (both the {@link PipelineDefinition} and the {@link ParameterSet} refer to the
 * same {@link ParameterSetName} row)
 * <p>
 * Note that in order to support rename capability, foreign key references to this entity need to be
 * updated in ParameterSetCrud.rename().
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "PI_PS_NAME")
public class ParameterSetName extends XmlReference {
    public static String DELIMITER = ":";

    /**
     * For Hibernate use only
     */
    ParameterSetName() {
    }

    /**
     * Constructors are package-level access because they should only be created by
     * {@link ParameterSet}
     *
     * @param name
     */
    public ParameterSetName(String name) {
        super(name);
        if (name.contains(DELIMITER)) {
            throw new IllegalArgumentException("name must not contain '" + DELIMITER + "'");
        }

    }

    // Here we put the Hibernate annotation on the getter rather than on the field. This is
    // necessary because the field is in the superclass, but the superclass is not used to construct
    // a database table.
    @Id
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        if (name.contains(DELIMITER)) {
            throw new IllegalArgumentException("name must not contain '" + DELIMITER + "'");
        }
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
        ParameterSetName other = (ParameterSetName) obj;
        return Objects.equals(name, other.name);
    }

    public static class ParameterSetNameAdapter extends XmlAdapter<String, ParameterSetName> {

        @Override
        public ParameterSetName unmarshal(String v) throws Exception {
            return new ParameterSetName(v);
        }

        @Override
        public String marshal(ParameterSetName v) throws Exception {
            return v.getName();
        }

    }

}
