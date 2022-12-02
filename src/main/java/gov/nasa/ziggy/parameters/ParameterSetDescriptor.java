package gov.nasa.ziggy.parameters;

import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * Descriptor for a {@link ParameterSet}, either in the parameter library or in an export directory
 *
 * @author Todd Klaus
 */
public class ParameterSetDescriptor {
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

    private ParameterSet libraryParamSet = null;
    private BeanWrapper<Parameters> importedParamsBean = null;

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
        return importedParamsBean.getInstance();
    }

    public Set<TypedParameter> getImportedProperties() {
        return importedParamsBean.getTypedProperties();
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

        // Data types
        if (!getImportedParamsBean().checkPrimitiveDataTypes()) {
            throw new IllegalStateException("Parameter set descriptor " + getName()
                + " typed property values do not match expected types");
        }

        // Parameter names
        validateTypedProperties(getFullParametersBean().getTypedProperties());
    }

    /**
     * Validates against an existing {@link ParameterSet} instance, in this case confirming that
     * there are no parameters in the {@link ParameterSetDescriptor} that are absent from the
     * {@link ParameterSet}.
     */
    public void validateAgainstParameterSet(ParameterSet parameterSet) {
        validateTypedProperties(parameterSet.getParameters().getTypedProperties());
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

    public BeanWrapper<Parameters> getFullParametersBean() {
        Parameters p = parametersInstance();
        return new BeanWrapper<>(p);
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
        } else {
            return "null";
        }
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

    /**
     * @return the libraryProps
     */
    public String getLibraryProps() {
        return libraryProps;
    }

    /**
     * @param libraryProps the libraryProps to set
     */
    public void setLibraryProps(String libraryProps) {
        this.libraryProps = libraryProps;
    }

    /**
     * @return the fileProps
     */
    public String getFileProps() {
        return fileProps;
    }

    /**
     * @param fileProps the fileProps to set
     */
    public void setFileProps(String fileProps) {
        this.fileProps = fileProps;
    }

    /**
     * @return the libraryParamSet
     */
    public ParameterSet getLibraryParamSet() {
        return libraryParamSet;
    }

    /**
     * @param libraryParamSet the libraryParamSet to set
     */
    public void setLibraryParamSet(ParameterSet libraryParamSet) {
        this.libraryParamSet = libraryParamSet;
    }

    /**
     * @return the importedParamsBean
     */
    public BeanWrapper<Parameters> getImportedParamsBean() {
        return importedParamsBean;
    }

    /**
     * @param importedParamsBean the importedParamsBean to set
     */
    public void setImportedParamsBean(BeanWrapper<Parameters> importedParamsBean) {
        this.importedParamsBean = importedParamsBean;
    }
}
