package gov.nasa.ziggy.pipeline.definition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Generic container for a collection of {@link TypedParameter} instances. Intended for use as the
 * superclass for other classes, see for example {@link Parameters} and {@link UnitOfWork}.
 *
 * @author PT
 */
public class TypedParameterCollection {

    @ProxyIgnore
    private Map<String, TypedParameter> parametersByName = new HashMap<>();

    public TypedParameterCollection() {
    }

    public TypedParameterCollection(Collection<TypedParameter> parameters) {
        setParameters(parameters);
    }

    /** Returns the parameters in this collection as a sorted set. */
    public Set<TypedParameter> getParameters() {
        return new TreeSet<>(parametersByName.values());
    }

    /** Returns a copy of all of the parameters in this collection as a sorted set. */
    public Set<TypedParameter> getParametersCopy() {
        Set<TypedParameter> parameters = new TreeSet<>();
        for (TypedParameter parameter : parametersByName.values()) {
            parameters.add(new TypedParameter(parameter));
        }
        return parameters;
    }

    public void setParameters(Collection<TypedParameter> parameters) {
        parametersByName = new HashMap<>();
        for (TypedParameter parameter : parameters) {
            parametersByName.put(parameter.getName(), parameter);
        }
    }

    public void addParameter(TypedParameter parameter) {
        parametersByName.put(parameter.getName(), parameter);
    }

    /** Returns the given parameter. */
    public TypedParameter getParameter(String name) {
        return parametersByName.get(name);
    }

    /** Returns the original map. */
    public Map<String, TypedParameter> getParametersByName() {
        return parametersByName;
    }

    /**
     * Performs a deep comparison of two {@link TypedParameterCollection} instances.
     */
    public boolean totalEquals(TypedParameterCollection other) {
        if (other == null || parametersByName.size() != other.parametersByName.size()) {
            return false;
        }

        for (Map.Entry<String, TypedParameter> entry : parametersByName.entrySet()) {
            TypedParameter otherValue = other.parametersByName.get(entry.getKey());
            if (otherValue == null || !otherValue.totalEquals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}
