package gov.nasa.ziggy.pipeline.definition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Generic container for a collection of {@link TypedParameter} instances. Intended for use as the
 * superclass for other classes, see for example {@link DefaultParameters} and {@link UnitOfWork}.
 *
 * @author PT
 */
public class TypedParameterCollection {

    private Map<String, TypedParameter> typedProperties = new HashMap<>();

    public Set<TypedParameter> getParameters() {
        return new HashSet<>(typedProperties.values());
    }

    public void setParameters(Set<TypedParameter> parameters) {
        Map<String, TypedParameter> parmap = new HashMap<>();
        for (TypedParameter parameter : parameters) {
            parmap.put(parameter.getName(), parameter);
        }
        typedProperties = parmap;
    }

    public void addParameter(TypedParameter parameter) {
        typedProperties.put(parameter.getName(), parameter);
    }

    public TypedParameter getParameter(String parName) {
        return typedProperties.get(parName);
    }

    /**
     * Generates an array that has copies of all {@link TypedParameter} instances in this object.
     * This is needed in support of the {@link PropertySheetPanel}).
     */
    public TypedParameter[] typedProperties() {
        Set<TypedParameter> propertySet = new HashSet<>();
        for (TypedParameter property : typedProperties.values()) {
            propertySet.add(new TypedParameter(property));
        }
        return propertySet.toArray(new TypedParameter[0]);
    }

}
