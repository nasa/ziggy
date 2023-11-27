package gov.nasa.ziggy.parameters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ReflectionUtils;

/**
 * Descriptor for a {@link ParameterSet}, either in the parameter library or in an export directory
 *
 * @author Todd Klaus
 */
public class ParameterSetDescriptor implements Comparable<ParameterSetDescriptor> {
    public enum State {
        /** initial state */
        NONE,
        /**
         * param set exists in the library, and the contents are identical
         */
        SAME,
        /**
         * param set exists in the library, but the contents are different
         */
        UPDATE,
        /** param set does not exist in the library */
        CREATE,
        /**
         * param set exists in the library, but not in the import directory
         */
        LIBRARY_ONLY,
        /** param set will be exported */
        EXPORT,
        /**
         * param set exists in the import directory, but is ignored because it is on the exclude
         * list
         */
        IGNORE,
        /** param class does not exist */
        CLASS_MISSING
    }

    private String name;
    private String className;
    private State state;
    /** textual representation of the props in the library */
    private String libraryProps = null;
    /** textual representation of the props in the export file */
    private String fileProps = null;

    private ParameterSet parameterSet;

    public ParameterSetDescriptor() {
    }

    public ParameterSetDescriptor(String name, String className) {
        this.name = name;
        this.className = className;
    }

    public ParameterSetDescriptor(String name, String className, State state) {
        this.name = name;
        this.className = className;
        this.state = state;
    }

    public Parameters parametersInstance() {
        return parameterSet.parametersInstance();
    }

    public Set<TypedParameter> getImportedProperties() {
        return parameterSet.getTypedParameters();
    }

    /**
     * Checks that the following conditions are met:
     * <ol>
     * <li>The data types of the parameters are correct based on the specified type in the XML for
     * that parameter
     * <li>All the parameter names correct for the class of parameters.
     * </ol>
     */
    public void validate() {
        validateTypedProperties(parameterSet.getTypedParameters());
    }

    /**
     * Validates against an existing {@link ParameterSet} instance, in this case confirming that
     * there are no parameters in the {@link ParameterSetDescriptor} that are absent from the
     * {@link ParameterSet}.
     */
    public void validateAgainstParameterSet(ParameterSet parameterSet) {
        validateTypedProperties(parameterSet.getTypedParameters());
    }

    private void validateTypedProperties(Set<TypedParameter> expectedTypedProperties) {
        Set<TypedParameter> importedTypedProperties = getImportedProperties();
        for (TypedParameter importedProperty : importedTypedProperties) {
            if (!expectedTypedProperties.contains(importedProperty)) {
                throw new IllegalArgumentException("Unexpected parameter name '"
                    + importedProperty.getName() + "' in parameter set '" + getName() + "'");
            }
        }
    }

    /**
     * Sets the data type information of the {@link TypedParameter} instances based on any fields in
     * the {@link Parameters} class.
     * <p>
     * Subclasses of {@link Parameters} have fields with data types and as a result, the data types
     * are omitted from the XML files that define the corresponding parameter sets. As a result, the
     * {@link TypedParameter} instances created at import all use type {@link String} rather than
     * the type required for the field. This method replaces the imported {@link TypedParameter}
     * instances with new versions that have the correct data type and the correct scalar vs array
     * setting.
     *
     * @throws IllegalStateException if the parameter values are not convertible to the field type
     * @return the original {@code parameterSet} with correctly-typed parameters
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    private ParameterSet updateTypedParameterTypes(ParameterSet parameterSet) {

        // Instances of Parameters don't need any modification.
        try {
            if (Class.forName(className).equals(Parameters.class)) {
                return parameterSet;
            }
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Class " + className + " not found", e);
        }

        // Subclasses of Parameters potentially have fields that will need to be populated.
        // We want the TypedParameter instances to have types that match the types of the
        // fields.
        Set<TypedParameter> originalTypedParameters = parameterSet.getTypedParameters();
        Map<String, TypedParameter> originalParametersByName = new HashMap<>();
        for (TypedParameter parameter : originalTypedParameters) {
            originalParametersByName.put(parameter.getName(), parameter);
        }
        Set<TypedParameter> updatedTypedParameters = new HashSet<>();

        List<String> conversionFailureFieldNames = new ArrayList<>();
        ParametersInterface instance = parameterSet.parametersInstance(false);
        for (Field field : ReflectionUtils.getAllFields(parameterSet.clazz(), true)) {
            field.setAccessible(true);

            // The Parameters field name is not to be turned into a TypedParameter.
            if (field.getName().equals(Parameters.NAME_FIELD)) {
                continue;
            }
            ZiggyDataType dataType = null;
            try {
                dataType = ZiggyDataType.getDataType(field);
            } catch (PipelineException e) {

                // Indicates something other than a primitive type or a String, which
                // means that it must be some other field that was never meant to be
                // supported.
                continue;
            }

            // If we've pulled in a Persistable, that also indicates that there's something
            // lurking in this parameters class that shouldn't be converted to a TypedParameter.
            if (dataType.equals(ZiggyDataType.ZIGGY_PERSISTABLE)) {
                continue;
            }
            boolean isScalar = !field.getType().isArray();
            TypedParameter typedParameter = originalParametersByName.get(field.getName());
            if (typedParameter == null) {

                // Check to see if there is a default value in the ParametersInterface instance.
                String defaultValueString = "";
                try {
                    Object defaultValue = field.get(instance);
                    if (defaultValue != null) {
                        defaultValueString = ZiggyDataType.objectToString(defaultValue);
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {

                    // This can never occur. The ParameterSet API ensures that the class returned
                    // by the clazz() method matches the class of the object returned by the
                    // parametersInstance() method, hence
                    throw new AssertionError(e);
                }
                updatedTypedParameters.add(
                    new TypedParameter(field.getName(), defaultValueString, dataType, isScalar));
                continue;
            }

            // As long as we're here, do a type check.
            try {
                ZiggyDataType.stringToObject(typedParameter.getString(), dataType, !isScalar);
            } catch (Exception e) {
                conversionFailureFieldNames.add(field.getName());
                continue;
            }
            updatedTypedParameters.add(new TypedParameter(typedParameter.getName(),
                typedParameter.getString(), dataType, isScalar));
        }
        if (!conversionFailureFieldNames.isEmpty()) {
            throw new IllegalStateException(
                "Unable to convert parameter string values to appropriate type, fields: "
                    + conversionFailureFieldNames.toString());
        }
        parameterSet.setTypedParameters(updatedTypedParameters);
        return parameterSet;
    }

    @Override
    public String toString() {
        return state + " : " + name + " (" + className + ")";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public String shortClassName() {
        if (className != null) {
            return className.substring(className.lastIndexOf(".") + 1);
        }
        return "null";
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getLibraryProps() {
        return libraryProps;
    }

    public void setLibraryProps(String libraryProps) {
        this.libraryProps = libraryProps;
    }

    public String getFileProps() {
        return fileProps;
    }

    public void setFileProps(String fileProps) {
        this.fileProps = fileProps;
    }

    public ParameterSet getParameterSet() {
        return parameterSet;
    }

    public void setParameterSet(ParameterSet parameterSet) {
        parameterSet.setClassname(className);
        this.parameterSet = updateTypedParameterTypes(parameterSet);
    }

    @Override
    public int compareTo(ParameterSetDescriptor o) {
        return name.compareTo(o.getName());
    }
}
