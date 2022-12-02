package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.parameters.DefaultParameters;

/**
 * Test of BeanWrapper class when the object being wrapped is a DefaultParameters instance.
 *
 * @author PT
 */
public class BeanWrapperDefaultParametersTest {

    private DefaultParameters parameters = new DefaultParameters();
    private TypedParameter p1, p2, p3;
    private Map<String, TypedParameter> typedPropertyMap = new HashMap<>();

    @Before
    public void setup() {

        p1 = new TypedParameter("tired", "wired", ZiggyDataType.ZIGGY_STRING);
        p2 = new TypedParameter("dummy", "300", ZiggyDataType.ZIGGY_INT);
        p3 = new TypedParameter("euros", "28.56", ZiggyDataType.ZIGGY_FLOAT);
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(p1);
        typedProperties.add(p2);
        typedProperties.add(p3);
        parameters.setName("test");
        parameters.setParameters(typedProperties);
        for (TypedParameter property : typedProperties) {
            typedPropertyMap.put(property.getName(), property);
        }
    }

    @Test
    public void testConstructor() {
        BeanWrapper<DefaultParameters> b1 = new BeanWrapper<>(DefaultParameters.class);
        assertEquals(DefaultParameters.class.getName(), b1.getClassName());
        assertTrue(b1.getTypedProperties().isEmpty());
        assertEquals(DefaultParameters.class, b1.getClazz());

        b1 = new BeanWrapper<>();
        assertNull(b1.getClassName());
        b1.setClazz(DefaultParameters.class);
        assertEquals(DefaultParameters.class.getName(), b1.getClassName());

        b1 = new BeanWrapper<>(parameters);
        assertEquals(DefaultParameters.class.getName(), b1.getClassName());
        assertEquals(DefaultParameters.class, b1.getClazz());
        assertEquals(3, b1.getTypedProperties().size());
        assertTrue(b1.getTypedProperties().contains(p1));
        assertTrue(b1.getTypedProperties().contains(p2));
        assertTrue(b1.getTypedProperties().contains(p3));
        Map<String, String> b1p = BeanWrapper.propertyValueByName(b1.getTypedProperties());
        Set<String> b1pk = b1p.keySet();
        assertTrue(b1pk.contains("tired"));
        assertTrue(b1pk.contains("dummy"));
        assertTrue(b1pk.contains("euros"));
        assertEquals("wired", b1p.get("tired"));
        assertEquals("300", b1p.get("dummy"));
        assertEquals("28.56", b1p.get("euros"));

        BeanWrapper<DefaultParameters> b2 = new BeanWrapper<>(b1);
        assertEquals(DefaultParameters.class.getName(), b2.getClassName());
        assertEquals(DefaultParameters.class, b2.getClazz());
        assertEquals(3, b2.getTypedProperties().size());
        assertTrue(b2.getTypedProperties().contains(p1));
        assertTrue(b2.getTypedProperties().contains(p2));
        assertTrue(b2.getTypedProperties().contains(p3));
        Map<String, String> b2p = BeanWrapper.propertyValueByName(b2.getTypedProperties());
        Set<String> b2pk = b2p.keySet();
        assertTrue(b2pk.contains("tired"));
        assertTrue(b2pk.contains("dummy"));
        assertTrue(b2pk.contains("euros"));
        assertEquals("wired", b2p.get("tired"));
        assertEquals("300", b2p.get("dummy"));
        assertEquals("28.56", b2p.get("euros"));
    }

    @Test
    public void testGetInstance() {
        BeanWrapper<DefaultParameters> b = new BeanWrapper<>(parameters);
        DefaultParameters p = b.getInstance();
        Set<TypedParameter> t = p.getParameters();
        assertEquals(3, t.size());
        assertTrue(t.contains(p1));
        assertTrue(t.contains(p2));
        assertTrue(t.contains(p3));
        assertNull(p.getName());
        Map<String, TypedParameter> instancePropertyMap = new HashMap<>();
        for (TypedParameter typedProperty : t) {
            instancePropertyMap.put(typedProperty.getName(), typedProperty);
        }

        // Test to make sure that the instance has COPIES of the typed properties rather than
        // references to the ones in the BeanWrapper
        Set<String> propertyNames = typedPropertyMap.keySet();
        for (String propertyName : propertyNames) {
            assertFalse(
                instancePropertyMap.get(propertyName) == typedPropertyMap.get(propertyName));
        }
    }

    @Test
    public void testPopulateFromInstance() {
        BeanWrapper<DefaultParameters> b1 = new BeanWrapper<>();
        b1.populateFromInstance(parameters);
        assertEquals(DefaultParameters.class.getName(), b1.getClassName());
        Set<TypedParameter> t = b1.getTypedProperties();
        assertEquals(3, t.size());
        assertTrue(t.contains(p1));
        assertTrue(t.contains(p2));
        assertTrue(t.contains(p3));
        Map<String, TypedParameter> instancePropertyMap = new HashMap<>();
        for (TypedParameter typedProperty : t) {
            instancePropertyMap.put(typedProperty.getName(), typedProperty);
        }

        // Test to make sure that the instance has COPIES of the typed properties rather than
        // references to the ones in the BeanWrapper
        Set<String> propertyNames = typedPropertyMap.keySet();
        for (String propertyName : propertyNames) {
            assertFalse(
                instancePropertyMap.get(propertyName) == typedPropertyMap.get(propertyName));
        }
    }

    @Test
    public void testSetProps() {
        BeanWrapper<DefaultParameters> b1 = new BeanWrapper<>(DefaultParameters.class);
        Map<String, String> m = new HashMap<>();
        m.put("abcd", "efgh");
        m.put("ijkl", "mnop");
        b1.setProperties(m);
        assertEquals(BeanWrapper.propertyValueByName(b1.getTypedProperties()), m);
        Set<TypedParameter> t = b1.getTypedProperties();
        assertEquals(2, t.size());
        List<TypedParameter> tl = new ArrayList<>();
        tl.addAll(t);
        TypedParameter tl0 = tl.get(0);
        if (tl0.getName().equals("abcd")) {
            assertEquals("efgh", tl0.getValue());
            assertEquals("ijkl", tl.get(1).getName());
            assertEquals("mnop", tl.get(1).getValue());
        } else {
            assertEquals("abcd", tl.get(1).getName());
            assertEquals("efgh", tl.get(1).getValue());
            assertEquals("ijkl", tl.get(0).getName());
            assertEquals("mnop", tl.get(0).getValue());
        }
        assertEquals(ZiggyDataType.ZIGGY_STRING, tl.get(0).getDataType());
        assertEquals(ZiggyDataType.ZIGGY_STRING, tl.get(1).getDataType());
    }

    @Test
    public void testSetTypedProperties() {
        BeanWrapper<DefaultParameters> b1 = new BeanWrapper<>(DefaultParameters.class);
        b1.setTypedProperties(parameters.getParameters());
        Set<TypedParameter> t = b1.getTypedProperties();
        assertEquals(3, t.size());
        assertTrue(t.contains(p1));
        assertTrue(t.contains(p2));
        assertTrue(t.contains(p3));
        Map<String, String> m = BeanWrapper.propertyValueByName(b1.getTypedProperties());
        assertEquals(3, m.size());
        Set<String> keys = m.keySet();
        assertTrue(keys.contains("tired"));
        assertTrue(keys.contains("dummy"));
        assertTrue(keys.contains("euros"));
        assertEquals("wired", m.get("tired"));
        assertEquals("300", m.get("dummy"));
        assertEquals("28.56", m.get("euros"));
    }

    @Test
    public void testGetAllPropNames() {
        BeanWrapper<DefaultParameters> b = new BeanWrapper<>(parameters);
        Set<String> s = b.getAllPropertyNames();
        assertEquals(3, s.size());
        assertTrue(s.contains("tired"));
        assertTrue(s.contains("dummy"));
        assertTrue(s.contains("euros"));
    }
}
