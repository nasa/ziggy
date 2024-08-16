package gov.nasa.ziggy.pipeline.xml;

import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;

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

    private State state;
    /** textual representation of the props in the library */
    private String libraryProps = null;
    /** textual representation of the props in the export file */
    private String fileProps = null;

    private ParameterSet parameterSet;

    public ParameterSetDescriptor() {
    }

    public ParameterSetDescriptor(ParameterSet parameterSet) {
        this(parameterSet, null);
    }

    public ParameterSetDescriptor(ParameterSet parameterSet, State state) {
        this.parameterSet = parameterSet;
        this.state = state;
    }

    public Set<Parameter> getImportedProperties() {
        return parameterSet.getParameters();
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
        validateTypedProperties(parameterSet.getParameters());
    }

    /**
     * Validates against an existing {@link ParameterSet} instance, in this case confirming that
     * there are no parameters in the {@link ParameterSetDescriptor} that are absent from the
     * {@link ParameterSet}.
     */
    public void validateAgainstParameterSet(ParameterSet parameterSet) {
        validateTypedProperties(parameterSet.getParameters());
    }

    private void validateTypedProperties(Set<Parameter> expectedTypedProperties) {
        Set<Parameter> importedTypedProperties = getImportedProperties();
        for (Parameter importedProperty : importedTypedProperties) {
            if (!expectedTypedProperties.contains(importedProperty)) {
                throw new IllegalArgumentException("Unexpected parameter name '"
                    + importedProperty.getName() + "' in parameter set '" + getName() + "'");
            }
        }
    }

    @Override
    public String toString() {
        return state + " : " + getName();
    }

    public String getName() {
        return parameterSet.getName();
    }

    public String getModuleInterfaceName() {
        return parameterSet.getModuleInterfaceName();
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

    @Override
    public int compareTo(ParameterSetDescriptor o) {
        return getName().compareTo(o.getName());
    }
}
