package gov.nasa.ziggy.parameters;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * Unit test class for {@link ParameterSetDescriptor}
 *
 * @author PT
 */
public class ParameterSetDescriptorTest {

    @Test(expected = IllegalStateException.class)
    public void testValidationFailureDataTypes() {
        DefaultParameters parameters = new DefaultParameters();
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(new TypedParameter("badParameter", "string", ZiggyDataType.ZIGGY_INT));
        parameters.setParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.DefaultParameters");
        desc.setImportedParamsBean(new BeanWrapper<>(parameters));
        desc.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAgainstParameterSetException() {
        DefaultParameters parameters = new DefaultParameters();
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties
            .add(new TypedParameter("badParameter", "string", ZiggyDataType.ZIGGY_STRING));
        parameters.setParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.DefaultParameters");
        desc.setImportedParamsBean(new BeanWrapper<>(parameters));
        DefaultParameters parameters2 = new DefaultParameters();
        Set<TypedParameter> newProperties = new HashSet<>();
        newProperties
            .add(new TypedParameter("mismatchedParameter", "string", ZiggyDataType.ZIGGY_STRING));
        parameters2.setParameters(newProperties);
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(new BeanWrapper<>(parameters2));
        desc.validateAgainstParameterSet(parameterSet);
    }

}
