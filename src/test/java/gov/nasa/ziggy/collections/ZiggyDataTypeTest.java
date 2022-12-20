package gov.nasa.ziggy.collections;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BOOLEAN;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_DOUBLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_ENUM;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_FLOAT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_PERSISTABLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_SHORT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static gov.nasa.ziggy.collections.ZiggyDataType.truncateClassName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.hdf5.EnumTest;
import gov.nasa.ziggy.module.hdf5.PersistableSample1;
import gov.nasa.ziggy.module.hdf5.PersistableSample2;

/**
 * Unit tests for the {@link ZiggyDataType} class.
 *
 * @author PT
 */
public class ZiggyDataTypeTest {

    public Object array1;
    public Object array2;
    public Object array3;
    public Object boxedScalar;
    public Object boxedArray1;
    public Object boxedArray2;
    public Object boxedArray3;

    /**
     * Tests that getHdf5DataType returns the correct Hdf5DataType for each type of legal input.
     *
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetHdf5DataTypeFromObject()
        throws IllegalArgumentException, IllegalAccessException {

        ZiggyDataTypeTest testObject = new ZiggyDataTypeTest();
        testObject.array1 = new boolean[5];
        testObject.array2 = new boolean[5][5];
        testObject.array3 = new boolean[5][5][5];
        testObject.boxedScalar = Boolean.valueOf(true);
        testObject.boxedArray1 = new Boolean[5];
        testObject.boxedArray2 = new Boolean[5][5];
        testObject.boxedArray3 = new Boolean[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_BOOLEAN);
        assertEquals(ZIGGY_BOOLEAN, getDataType(true));

        testObject.array1 = new byte[5];
        testObject.array2 = new byte[5][5];
        testObject.array3 = new byte[5][5][5];
        testObject.boxedScalar = Byte.valueOf((byte) 1);
        testObject.boxedArray1 = new Byte[5];
        testObject.boxedArray2 = new Byte[5][5];
        testObject.boxedArray3 = new Byte[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_BYTE);
        assertEquals(ZIGGY_BYTE, getDataType((byte) 2));

        testObject.array1 = new short[5];
        testObject.array2 = new short[5][5];
        testObject.array3 = new short[5][5][5];
        testObject.boxedScalar = Short.valueOf((short) 1);
        testObject.boxedArray1 = new Short[5];
        testObject.boxedArray2 = new Short[5][5];
        testObject.boxedArray3 = new Short[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_SHORT);
        assertEquals(ZIGGY_SHORT, getDataType((short) 2));

        testObject.array1 = new int[5];
        testObject.array2 = new int[5][5];
        testObject.array3 = new int[5][5][5];
        testObject.boxedScalar = Integer.valueOf(1);
        testObject.boxedArray1 = new Integer[5];
        testObject.boxedArray2 = new Integer[5][5];
        testObject.boxedArray3 = new Integer[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_INT);
        assertEquals(ZIGGY_INT, getDataType(2));

        testObject.array1 = new long[5];
        testObject.array2 = new long[5][5];
        testObject.array3 = new long[5][5][5];
        testObject.boxedScalar = Long.valueOf(1);
        testObject.boxedArray1 = new Long[5];
        testObject.boxedArray2 = new Long[5][5];
        testObject.boxedArray3 = new Long[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_LONG);
        assertEquals(ZIGGY_LONG, getDataType((long) 2));

        testObject.array1 = new float[5];
        testObject.array2 = new float[5][5];
        testObject.array3 = new float[5][5][5];
        testObject.boxedScalar = Float.valueOf(1);
        testObject.boxedArray1 = new Float[5];
        testObject.boxedArray2 = new Float[5][5];
        testObject.boxedArray3 = new Float[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_FLOAT);
        assertEquals(ZIGGY_FLOAT, getDataType((float) 2));

        testObject.array1 = new double[5];
        testObject.array2 = new double[5][5];
        testObject.array3 = new double[5][5][5];
        testObject.boxedScalar = Double.valueOf(1);
        testObject.boxedArray1 = new Double[5];
        testObject.boxedArray2 = new Double[5][5];
        testObject.boxedArray3 = new Double[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_DOUBLE);
        assertEquals(ZIGGY_DOUBLE, getDataType((double) 2));

        testObject.array1 = new String[5];
        testObject.array2 = new String[5][5];
        testObject.array3 = new String[5][5][5];
        testObject.boxedScalar = "whatever";
        testObject.boxedArray1 = new String[5];
        testObject.boxedArray2 = new String[5][5];
        testObject.boxedArray3 = new String[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_STRING);

        testObject.array1 = new EnumTest[5];
        testObject.array2 = new EnumTest[5][5];
        testObject.array3 = new EnumTest[5][5][5];
        testObject.boxedScalar = EnumTest.FIRST;
        testObject.boxedArray1 = new EnumTest[5];
        testObject.boxedArray2 = new EnumTest[5][5];
        testObject.boxedArray3 = new EnumTest[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_ENUM);

        testObject.array1 = new PersistableSample1[5];
        testObject.array2 = new PersistableSample1[5][5];
        testObject.array3 = new PersistableSample1[5][5][5];
        testObject.boxedScalar = new PersistableSample1();
        testObject.boxedArray1 = new PersistableSample1[5];
        testObject.boxedArray2 = new PersistableSample1[5][5];
        testObject.boxedArray3 = new PersistableSample1[5][5][5];
        testObject.exerciseDataTypeDetection(ZIGGY_PERSISTABLE);

        List<Integer> intList = new ArrayList<>();
        assertNull(getDataType(intList));
    }

    public void exerciseDataTypeDetection(ZiggyDataType type)
        throws IllegalArgumentException, IllegalAccessException {

        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            Object testValue = field.get(this);
            assertEquals(type, getDataType(testValue));
        }
        Object boxedList = Arrays.asList(boxedArray1);
        assertEquals(type, getDataType(boxedList));

    }

    /**
     * Tests that getHdf5Type throws the correct exception when presented with an object that cannot
     * be persisted.
     */
    @Test(expected = PipelineException.class)
    public void testNonPersistable() {
        ZiggyDataTypeTest testObject = new ZiggyDataTypeTest();
        getDataType(testObject);
    }

    /**
     * Tests that getHdf5Type produces the correct results for fields of a class.
     *
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testGetHdf5DataTypeFromField() throws NoSuchFieldException, SecurityException {
        Class<?> clazz = FieldTypeTestObject.class;
        assertEquals(ZIGGY_INT, getDataType(clazz.getDeclaredField("intScalar")));
        assertEquals(ZIGGY_BOOLEAN, getDataType(clazz.getDeclaredField("boolArray1")));
        assertEquals(ZIGGY_SHORT, getDataType(clazz.getDeclaredField("shortArray2")));
        assertEquals(ZIGGY_LONG, getDataType(clazz.getDeclaredField("longArray3")));
        assertEquals(ZIGGY_DOUBLE, getDataType(clazz.getDeclaredField("boxedScalar")));
        assertEquals(ZiggyDataType.ZIGGY_BYTE, getDataType(clazz.getDeclaredField("boxedArray1")));
        assertEquals(ZiggyDataType.ZIGGY_FLOAT, getDataType(clazz.getDeclaredField("boxedArray2")));
        assertEquals(ZiggyDataType.ZIGGY_INT, getDataType(clazz.getDeclaredField("intList")));
        assertEquals(ZiggyDataType.ZIGGY_STRING, getDataType(clazz.getDeclaredField("stringList")));
        assertEquals(ZiggyDataType.ZIGGY_PERSISTABLE,
            getDataType(clazz.getDeclaredField("pTest1Scalar")));
        assertEquals(ZiggyDataType.ZIGGY_PERSISTABLE,
            getDataType(clazz.getDeclaredField("pTest2Array1")));
        assertEquals(ZiggyDataType.ZIGGY_PERSISTABLE,
            getDataType(clazz.getDeclaredField("pTest1Array3")));
        assertEquals(ZiggyDataType.ZIGGY_PERSISTABLE, getDataType(clazz.getDeclaredField("pList")));
        assertEquals(ZiggyDataType.ZIGGY_ENUM, getDataType(clazz.getDeclaredField("enumTest")));
    }

    /**
     * Tests that an invalid field will produce the correct error when checking HDF5 type.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test(expected = PipelineException.class)
    public void testInvalidField() throws NoSuchFieldException, SecurityException {
        Class<?> clazz = FieldTypeTestObject.class;
        getDataType(clazz.getDeclaredField("invalidField"));
    }

    @Test
    public void testClassNameTruncation() {
        int[] intArray1 = new int[5];
        String truncatedName = truncateClassName(intArray1.getClass().getName());
        assertEquals("I", truncatedName);
        double[][] doubleArray2 = new double[5][5];
        truncatedName = truncateClassName(doubleArray2.getClass().getName());
        assertEquals("D", truncatedName);
        Boolean z = true;
        truncatedName = truncateClassName(z.getClass().getName());
        assertEquals("java.lang.Boolean", truncatedName);
        Float[] boxedFloatArray = new Float[5];
        truncatedName = truncateClassName(boxedFloatArray.getClass().getName());
        assertEquals("java.lang.Float", truncatedName);
        PersistableSample1[][] pTest1Array2 = new PersistableSample1[5][5];
        truncatedName = truncateClassName(pTest1Array2.getClass().getName());
        assertEquals("gov.nasa.ziggy.module.hdf5.PersistableSample1", truncatedName);
        EnumTest[] testEnumArray1 = new EnumTest[5];
        truncatedName = truncateClassName(testEnumArray1.getClass().getName());
        assertEquals("gov.nasa.ziggy.module.hdf5.EnumTest", truncatedName);
    }

    @Test
    public void testUnboxAndCast() {
        Double[] boxedDoubleArray1 = { (double) 5, (double) 4, (double) 3, (double) 2, (double) 1 };
        byte[] byteArray1 = new byte[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedDoubleArray1, byteArray1);
        assertTrue(Arrays.equals(byteArray1,
            new byte[] { (byte) 5, (byte) 4, (byte) 3, (byte) 2, (byte) 1 }));

        short[] shortArray1 = new short[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedDoubleArray1, shortArray1);
        assertTrue(Arrays.equals(shortArray1,
            new short[] { (short) 5, (short) 4, (short) 3, (short) 2, (short) 1 }));

        int[] intArray1 = new int[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedDoubleArray1, intArray1);
        assertTrue(Arrays.equals(intArray1, new int[] { 5, 4, 3, 2, 1 }));

        long[] longArray1 = new long[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedDoubleArray1, longArray1);
        assertTrue(Arrays.equals(longArray1, new long[] { 5, 4, 3, 2, 1 }));

        float[] floatArray1 = new float[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedDoubleArray1, floatArray1);
        assertTrue(Arrays.equals(floatArray1, new float[] { 5, 4, 3, 2, 1 }));

        Float[] boxedFloatArray1 = { (float) 5, (float) 4, (float) 3, (float) 2, (float) 1 };
        double[] doubleArray1 = new double[5];
        ZiggyDataType.castBoxedNumericToUnboxedNumeric(boxedFloatArray1, doubleArray1);
        assertTrue(Arrays.equals(doubleArray1, new double[] { 5, 4, 3, 2, 1 }));
    }

    public class FieldTypeTestObject {
        int intScalar;
        boolean[] boolArray1;
        short[][] shortArray2;
        long[][][] longArray3;
        Double boxedScalar;
        Byte[] boxedArray1;
        Float[][] boxedArray2;
        List<Integer> intList;
        List<String> stringList;
        PersistableSample1 pTest1Scalar;
        PersistableSample2[] pTest2Array1;
        PersistableSample1[][][] pTest1Array3;
        List<PersistableSample1> pList;
        ZiggyDataTypeTest invalidField;
        EnumTest enumTest;
    }
}
