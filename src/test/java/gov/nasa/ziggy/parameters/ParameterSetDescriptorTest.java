package gov.nasa.ziggy.parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * Unit test class for {@link ParameterSetDescriptor}
 *
 * @author PT
 */
public class ParameterSetDescriptorTest {

    @Test(expected = NumberFormatException.class)
    public void testValidationFailureDataTypes() {
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(new TypedParameter("badParameter", "string", ZiggyDataType.ZIGGY_INT));
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setTypedParameters(typedProperties);
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.Parameters");
        desc.setParameterSet(parameterSet);
        desc.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAgainstParameterSetException() {
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties
            .add(new TypedParameter("badParameter", "string", ZiggyDataType.ZIGGY_STRING));
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.Parameters");
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setTypedParameters(typedProperties);
        desc.setParameterSet(parameterSet);

        Set<TypedParameter> newProperties = new HashSet<>();
        newProperties
            .add(new TypedParameter("mismatchedParameter", "string", ZiggyDataType.ZIGGY_STRING));
        ParameterSet parameterSet2 = new ParameterSet();
        parameterSet2.setTypedParameters(newProperties);
        desc.validateAgainstParameterSet(parameterSet2);
    }

    /**
     * Tests that any {@link TypedParameter} instances that need an update of their type and/or
     * scalar values, get that update.
     */
    @Test
    public void testTypedParameterUpdate() {

        // Start with a Parameters instance.
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(new TypedParameter("f1", "100", ZiggyDataType.ZIGGY_INT, true));
        typedProperties
            .add(new TypedParameter("f2", "100.5, 100.6", ZiggyDataType.ZIGGY_FLOAT, false));
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.Parameters");
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setTypedParameters(typedProperties);
        desc.setParameterSet(parameterSet);
        ParametersOperations ops = new ParametersOperations();
        Map<String, TypedParameter> originalParameters = ops
            .nameToTypedPropertyMap(typedProperties);
        Map<String, TypedParameter> descriptorParameters = ops
            .nameToTypedPropertyMap(desc.getParameterSet().getTypedParameters());
        assertEquals(originalParameters.size(), descriptorParameters.size());
        for (Map.Entry<String, TypedParameter> entry : originalParameters.entrySet()) {
            TypedParameter updatedParameter = descriptorParameters.get(entry.getKey());
            assertNotNull(updatedParameter);
            TypedParameter originalParameter = entry.getValue();
            assertEquals(originalParameter.getName(), updatedParameter.getName());
            assertEquals(originalParameter.getString(), updatedParameter.getString());
            assertEquals(originalParameter.getDataType(), updatedParameter.getDataType());
            assertEquals(originalParameter.isScalar(), updatedParameter.isScalar());
        }

        // Now for a subclass that has typed fields.
        desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.ParameterSetDescriptorTest$ParWithFields");
        typedProperties.clear();
        typedProperties.add(new TypedParameter("f1", "100"));
        typedProperties.add(new TypedParameter("f2", "100.5, 100.6"));
        parameterSet = new ParameterSet();
        parameterSet.setTypedParameters(typedProperties);
        desc.setParameterSet(parameterSet);
        originalParameters = ops.nameToTypedPropertyMap(typedProperties);
        descriptorParameters = ops
            .nameToTypedPropertyMap(desc.getParameterSet().getTypedParameters());
        assertEquals(originalParameters.size(), descriptorParameters.size());
        assertEquals(2, originalParameters.size());

        TypedParameter par = originalParameters.get("f1");
        assertEquals(ZiggyDataType.ZIGGY_STRING, par.getDataType());
        assertTrue(par.isScalar());
        par = originalParameters.get("f2");
        assertEquals(ZiggyDataType.ZIGGY_STRING, par.getDataType());
        assertTrue(par.isScalar());

        par = descriptorParameters.get("f1");
        assertEquals(ZiggyDataType.ZIGGY_INT, par.getDataType());
        assertTrue(par.isScalar());
        par = descriptorParameters.get("f2");
        assertEquals(ZiggyDataType.ZIGGY_FLOAT, par.getDataType());
        assertFalse(par.isScalar());

        // Now for a collection of TypedParameter instances that doesn't cover all of the
        // class fields.
        desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.ParameterSetDescriptorTest$ParWithFields");
        typedProperties.clear();
        typedProperties.add(new TypedParameter("f1", "100"));
        parameterSet = new ParameterSet();
        parameterSet.setTypedParameters(typedProperties);
        desc.setParameterSet(parameterSet);
        originalParameters = ops.nameToTypedPropertyMap(typedProperties);
        descriptorParameters = ops
            .nameToTypedPropertyMap(desc.getParameterSet().getTypedParameters());
        assertEquals(2, descriptorParameters.size());
        assertEquals(1, originalParameters.size());
        par = originalParameters.get("f1");
        assertEquals(ZiggyDataType.ZIGGY_STRING, par.getDataType());
        assertTrue(par.isScalar());

        par = descriptorParameters.get("f1");
        assertEquals(ZiggyDataType.ZIGGY_INT, par.getDataType());
        assertTrue(par.isScalar());
        par = descriptorParameters.get("f2");
        assertEquals(ZiggyDataType.ZIGGY_FLOAT, par.getDataType());
        assertFalse(par.isScalar());
    }

    /**
     * Tests that the correct exception occurs if the data type of a field and the value that wants
     * to go into that field are mismatched.
     */
    @Test(expected = IllegalStateException.class)
    public void testTypedParameterMismatch() {
        ParameterSetDescriptor desc = new ParameterSetDescriptor("test",
            "gov.nasa.ziggy.parameters.ParameterSetDescriptorTest$ParWithFields");
        ParameterSet parameterSet = new ParameterSet();
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(new TypedParameter("f1", "100"));
        typedProperties.add(new TypedParameter("f2", "Tanar of Pellucidar"));
        parameterSet.setTypedParameters(typedProperties);
        desc.setParameterSet(parameterSet);
    }

    public static class ParWithFields extends Parameters {

        private int f1;
        private float[] f2;

        public int getF1() {
            return f1;
        }

        public float[] getF2() {
            return f2;
        }
    }
}
