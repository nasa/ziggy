package gov.nasa.ziggy.pipeline.xml;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;

/**
 * Unit test class for {@link ParameterSetDescriptor}
 *
 * @author PT
 */
public class ParameterSetDescriptorTest {

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        "build");

    @Test(expected = NumberFormatException.class)
    public void testValidationFailureDataTypes() {
        Set<Parameter> typedProperties = new HashSet<>();
        typedProperties.add(new Parameter("badParameter", "string", ZiggyDataType.ZIGGY_INT));
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor(parameterSet);
        desc.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAgainstParameterSetException() {
        Set<Parameter> typedProperties = new HashSet<>();
        typedProperties.add(new Parameter("badParameter", "string", ZiggyDataType.ZIGGY_STRING));
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor(parameterSet);

        Set<Parameter> newProperties = new HashSet<>();
        newProperties
            .add(new Parameter("mismatchedParameter", "string", ZiggyDataType.ZIGGY_STRING));
        ParameterSet parameterSet2 = new ParameterSet();
        parameterSet2.setParameters(newProperties);
        desc.validateAgainstParameterSet(parameterSet2);
    }

    /**
     * Tests that any {@link Parameter} instances that need an update of their type and/or scalar
     * values, get that update.
     */
    @Test
    public void testParameterUpdate() {

        // Start with a Parameters instance.
        Set<Parameter> typedProperties = new HashSet<>();
        typedProperties.add(new Parameter("f1", "100", ZiggyDataType.ZIGGY_INT, true));
        typedProperties.add(new Parameter("f2", "100.5, 100.6", ZiggyDataType.ZIGGY_FLOAT, false));
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor(parameterSet);
        ParametersOperations ops = new ParametersOperations();
        Map<String, Parameter> originalParameters = ops.nameToTypedPropertyMap(typedProperties);
        Map<String, Parameter> descriptorParameters = ops
            .nameToTypedPropertyMap(desc.getParameterSet().getParameters());
        assertEquals(originalParameters.size(), descriptorParameters.size());
        for (Map.Entry<String, Parameter> entry : originalParameters.entrySet()) {
            Parameter updatedParameter = descriptorParameters.get(entry.getKey());
            assertNotNull(updatedParameter);
            Parameter originalParameter = entry.getValue();
            assertEquals(originalParameter.getName(), updatedParameter.getName());
            assertEquals(originalParameter.getString(), updatedParameter.getString());
            assertEquals(originalParameter.getDataType(), updatedParameter.getDataType());
            assertEquals(originalParameter.isScalar(), updatedParameter.isScalar());
        }
    }
}
