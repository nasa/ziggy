package gov.nasa.ziggy.parameters;

import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.TypedParameterCollection;

/**
 * Parameters implementation for use-cases in which no specific Java class is required. It is also
 * used a simple marker used by the console to display all possible classes found on the classpath
 * when configuring a new {@link PipelineDefinition}.
 * <p>
 * This class stores parameters using a Set of TypedParameter instances, each of which contains a
 * name, value, and data type. This is a different approach from the old one of supporting every
 * parameter set with a unique Java class that implemented the old Parameters interface and which
 * has parameter names represented as member names.
 * <p>
 * From the user's point of view, a Parameters instance behaves much like the old classes
 * implementing the old Parameters interface, specifically in that the parameters are serialized to
 * HDF5 files in a manner identical to parameter sets backed by a Java class.
 * <p>
 * Subclasses should conform to the JavaBeans specification in order to extract TypedParameters from
 * the class' properties.
 *
 * @author PT
 * @author Bill Wohler
 */

public class Parameters extends TypedParameterCollection implements ParametersInterface {

    public static final String NAME_FIELD = "name";

    // If you change the name of the following field, change the value in the static String, above.
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void validate() {
        // Do nothing, by default.
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public void updateParameter(String name, String value) {
        getParametersByName().get(name).setString(value);
        populate(getParameters());
    }

    // Note: hashCode() and equals() use only the name so that there cannot be any duplicate copies
    // of the parameter set in a Set<Parameters>.
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Parameters other = (Parameters) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }
}
