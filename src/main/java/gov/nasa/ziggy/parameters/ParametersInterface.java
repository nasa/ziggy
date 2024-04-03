package gov.nasa.ziggy.parameters;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ReflectionUtils;

public interface ParametersInterface extends Persistable {

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    default void populate(Collection<TypedParameter> typedParameters) {

        setParameters(typedParameters);

        // Now populate any fields in subclasses.
        for (Field field : ReflectionUtils.getAllFields(this, true)) {
            TypedParameter typedParameter = getParametersByName().get(field.getName());
            if (typedParameter == null) {
                // This happens if the field is something that is not supposed to be mapped
                // between TypedParameter instances and instance fields. Examples include:
                // static fields such as a logger, the Parameters name field, or the
                // TypedParameterCollection Set and Map fields.
                continue;
            }
            try {
                ZiggyDataType.setField(this, field, typedParameter.getString());
            } catch (IllegalArgumentException e) {
                // Can never occur. By construction, the field exists and is writable.
                throw new AssertionError(e);
            }
        }
    }

    void updateParameter(String name, String value);

    void setParameters(Collection<TypedParameter> typedParameters);

    Map<String, TypedParameter> getParametersByName();

    Set<TypedParameter> getParameters();

    void validate();
}
