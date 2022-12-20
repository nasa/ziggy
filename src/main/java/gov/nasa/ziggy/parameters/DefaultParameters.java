package gov.nasa.ziggy.parameters;

import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.TypedParameterCollection;

/**
 * Parameters implementation for use-cases in which no specific Java class is required.
 * <p>
 * The DefaultParameters class stores parameters using a Set of TypedProperty instances, each of
 * which contains a name, value, and data type. This is a different approach from the standard one
 * of supporting every parameter set with a unique Java class that implements Parameters and which
 * has parameter names represented as member names.
 * <p>
 * From the user's point of view, a DefaultParameters instance behaves much like any other
 * Parameters instance, specifically in that the parameters are serialized to HDF5 files in a manner
 * identical to parameter sets backed by a Java class.
 *
 * @author PT
 */
public class DefaultParameters extends TypedParameterCollection implements Parameters {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    // Note: hashCode() and equals() use only the name so that there cannot be any duplicate copies
    // of the parameter set in a Set<DefaultParameters>.
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DefaultParameters other = (DefaultParameters) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

}
