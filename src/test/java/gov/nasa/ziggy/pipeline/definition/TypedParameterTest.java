package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import gov.nasa.ziggy.collections.ZiggyDataType;

/**
 * Unit test class for {@link TypedParameter} class.
 *
 * @author PT
 */
public class TypedParameterTest {

    @Test
    public void testConstructor() {

        // test the 3-arg constructor with a funky spelling for the type
        TypedParameter t = new TypedParameter("dummy", "100.", "dOuBlE");
        assertEquals("dummy", t.getName());
        assertEquals("100.0", t.getString());
        assertEquals(ZiggyDataType.ZIGGY_DOUBLE, t.getDataType());
        assertEquals(double.class, t.getType());

        // test the 2-arg constructor, which always produces a string
        t = new TypedParameter("dummy", "100.");
        assertEquals("dummy", t.getName());
        assertEquals("100.", t.getString());
        assertEquals(ZiggyDataType.ZIGGY_STRING, t.getDataType());
        assertEquals(String.class, t.getType());

        // constructor with an array
        t = new TypedParameter("dummy", "100.", "doublearray");
        assertEquals("dummy", t.getName());
        assertEquals("100.0", t.getString());
        assertEquals(ZiggyDataType.ZIGGY_DOUBLE, t.getDataType());
        assertEquals(double[].class, t.getType());
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidConstruction() {
        new TypedParameter("dummy", "text", "dOuBlE");
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalScalarArrayMismatch() {
        new TypedParameter("dummy", "100, 200", "int");
    }

    @Test
    public void testLegalScalarArrayMismatch() {
        TypedParameter t = new TypedParameter("dummy", "100", "intarray");
        assertFalse(t.isScalar());
        Object v = t.getValueAsArray();
        assertTrue(v instanceof int[]);
        int[] i = (int[]) v;
        assertEquals(1, i.length);
        assertEquals(100, i[0]);
    }

    @Test
    public void testGetTypedValue() {

        // Scalar value returns as an array
        TypedParameter t = new TypedParameter("dummy", "100.", "dOuBlE");
        Object v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        double[] vv = (double[]) v;
        assertEquals(1, vv.length);
        assertEquals(100.0, vv[0], 1e-9);

        // commas indicate multiple array values
        t = new TypedParameter("dummy", "100, 200, 300", "doubleArRaY");
        v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        vv = (double[]) v;
        assertEquals(3, vv.length);
        assertEquals(100.0, vv[0], 1e-9);
        assertEquals(200.0, vv[1], 1e-9);
        assertEquals(300.0, vv[2], 1e-9);

        // String array
        t = new TypedParameter("dummy", "100, 200, 300", "stringARRAY");
        v = t.getValueAsArray();
        assertTrue(v instanceof String[]);
        String[] vs = (String[]) v;
        assertEquals(3, vs.length);
        assertEquals("100", vs[0]);
        assertEquals("200", vs[1]);
        assertEquals("300", vs[2]);

        // Handle an empty
        t = new TypedParameter("dummy", "", "double");
        v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        vv = (double[]) v;
        assertEquals(1, vv.length);
        assertEquals(0, vv[0], 1e-9);

        // Handle an array of empties
        t = new TypedParameter("dummy", " , , ", "doublearray");
        v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        vv = (double[]) v;
        assertEquals(3, vv.length);
        assertEquals(0, vv[0], 1e-9);
        assertEquals(0, vv[1], 1e-9);
        assertEquals(0, vv[2], 1e-9);

    }

    @Test
    public void testGetValue() {

        // Scalar value returns as scalar
        TypedParameter t = new TypedParameter("dummy", "100.", "dOuBlE");
        Object v = t.getValue();
        assertTrue(v instanceof Double);
        Double vv = (Double) v;
        assertEquals(100.0, vv, 1e-9);
        assertEquals("100.0", t.getString());

        // commas indicate multiple array values
        t = new TypedParameter("dummy", "100, 200, 300", "doubleArRaY");
        v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        double[] vvv = (double[]) v;
        assertEquals(3, vvv.length);
        assertEquals(100.0, vvv[0], 1e-9);
        assertEquals(200.0, vvv[1], 1e-9);
        assertEquals(300.0, vvv[2], 1e-9);
        assertEquals("100.0,200.0,300.0", t.getString());

        // single value but double array returns as an array
        t = new TypedParameter("dummy", "100", "doubleArRaY");
        v = t.getValueAsArray();
        assertTrue(v instanceof double[]);
        vvv = (double[]) v;
        assertEquals(1, vvv.length);
        assertEquals(100.0, vvv[0], 1e-9);

    }

    @Test
    public void testSetValue() {
        TypedParameter t = new TypedParameter("dummy", "100.", "dOuBlE");
        t.setValue(200);
        Object v = t.getValue();
        assertTrue(v instanceof Double);
        Double vv = (Double) v;
        assertEquals(200.0, vv, 1e-9);
        assertEquals("200", t.getString());

        t = new TypedParameter("dummy", "100.", "doublearray");
        double[] newValue = { 100, 200, 300 };
        t.setValue(newValue);
        v = t.getValue();
        assertTrue(v instanceof double[]);
        double[] vvv = (double[]) v;
        assertEquals(3, vvv.length);
        assertEquals(100, vvv[0], 1e-9);
        assertEquals(200, vvv[1], 1e-9);
        assertEquals(300, vvv[2], 1e-9);
    }
}
