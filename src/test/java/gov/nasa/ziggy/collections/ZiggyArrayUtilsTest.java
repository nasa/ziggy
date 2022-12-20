package gov.nasa.ziggy.collections;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BOOLEAN;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_DOUBLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_ENUM;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_FLOAT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_SHORT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.hdf5.EnumTest;

/**
 * Test class for ZiggyArrayUtils class.
 *
 * @author PT
 */
public class ZiggyArrayUtilsTest {

    double testDouble;
    float[] testFloat;
    List<Byte> byteList;
    Integer boxedInt;
    String[] stringArray;
    Long[][] boxedLongArray;

    @Test
    public void testBoxAndUnbox1dArray() {

        boolean[] zArray1 = { true, false, false, true };
        Boolean[] ZArray1 = ArrayUtils.toObject(zArray1);
        Object zArray2 = ZiggyArrayUtils.unbox(ZArray1);
        assertTrue(Arrays.equals(zArray1, (boolean[]) zArray2));
        Object ZArray2 = ZiggyArrayUtils.box(zArray1);
        assertTrue(Arrays.deepEquals(ZArray1, (Boolean[]) ZArray2));

        byte[] bArray1 = { (byte) 1, (byte) 2, (byte) 3, (byte) 4 };
        Byte[] BArray1 = ArrayUtils.toObject(bArray1);
        Object bArray2 = ZiggyArrayUtils.unbox(BArray1);
        assertTrue(Arrays.equals(bArray1, (byte[]) bArray2));
        Object BArray2 = ZiggyArrayUtils.box(bArray1);
        assertTrue(Arrays.deepEquals(BArray1, (Byte[]) BArray2));

        short[] sArray1 = { (short) 1, (short) 2, (short) 3, (short) 4 };
        Short[] SArray1 = ArrayUtils.toObject(sArray1);
        Object sArray2 = ZiggyArrayUtils.unbox(SArray1);
        assertTrue(Arrays.equals(sArray1, (short[]) sArray2));
        Object SArray2 = ZiggyArrayUtils.box(sArray1);
        assertTrue(Arrays.deepEquals(SArray1, (Short[]) SArray2));

        int[] iArray1 = { 1, 2, 3, 4 };
        Integer[] IArray1 = ArrayUtils.toObject(iArray1);
        Object iArray2 = ZiggyArrayUtils.unbox(IArray1);
        assertTrue(Arrays.equals(iArray1, (int[]) iArray2));
        Object IArray2 = ZiggyArrayUtils.box(iArray1);
        assertTrue(Arrays.deepEquals(IArray1, (Integer[]) IArray2));

        long[] jArray1 = { 1, 2, 3, 4 };
        Long[] JArray1 = ArrayUtils.toObject(jArray1);
        Object jArray2 = ZiggyArrayUtils.unbox(JArray1);
        assertTrue(Arrays.equals(jArray1, (long[]) jArray2));
        Object JArray2 = ZiggyArrayUtils.box(jArray1);
        assertTrue(Arrays.deepEquals(JArray1, (Long[]) JArray2));

        float[] fArray1 = { 1, 2, 3, 4 };
        Float[] FArray1 = ArrayUtils.toObject(fArray1);
        Object fArray2 = ZiggyArrayUtils.unbox(FArray1);
        assertTrue(Arrays.equals(fArray1, (float[]) fArray2));
        Object FArray2 = ZiggyArrayUtils.box(fArray1);
        assertTrue(Arrays.deepEquals(FArray1, (Float[]) FArray2));

        double[] dArray1 = { 1, 2, 3, 4 };
        Double[] DArray1 = ArrayUtils.toObject(dArray1);
        Object dArray2 = ZiggyArrayUtils.unbox(DArray1);
        assertTrue(Arrays.equals(dArray1, (double[]) dArray2));
        Object DArray2 = ZiggyArrayUtils.box(dArray1);
        assertTrue(Arrays.deepEquals(DArray1, (Double[]) DArray2));

    }

    @Test
    public void testBoxedArrayDetector() throws NoSuchFieldException, SecurityException {
        assertFalse(ZiggyArrayUtils.isBoxedPrimitive(new int[5]));
        assertTrue(ZiggyArrayUtils.isBoxedPrimitive(new Integer[5]));
        assertTrue(ZiggyArrayUtils.isBoxedPrimitive(new Boolean[5][5]));

        assertFalse(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("testDouble")));
        assertFalse(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("testFloat")));
        assertTrue(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("byteList")));
        assertTrue(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("boxedInt")));
        assertFalse(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("stringArray")));
        assertTrue(ZiggyArrayUtils
            .isBoxedPrimitive(ZiggyArrayUtilsTest.class.getDeclaredField("boxedLongArray")));
    }

    @Test
    public void testUnboxAndBox() throws ClassNotFoundException {
        Float[][][] boxedFloatArray = { { { (float) 1, (float) 2 }, { (float) 3, (float) 4 } },
            { { (float) 5, (float) 6 }, { (float) 7, (float) 8 } } };
        float[][][] testArray = { { { 1, 2 }, { 3, 4 } }, { { 5, 6 }, { 7, 8 } } };
        Object unboxedArray = ZiggyArrayUtils.unbox(boxedFloatArray);
        assertTrue(Arrays.deepEquals((Object[]) unboxedArray, (Object[]) testArray));
        assertEquals(Class.forName("[[[F"), unboxedArray.getClass());
        Object reboxedFloatArray = ZiggyArrayUtils.box(unboxedArray);
        assertTrue(Arrays.deepEquals((Object[]) boxedFloatArray, (Object[]) reboxedFloatArray));
        assertEquals(Class.forName("[[[Ljava.lang.Float;"), reboxedFloatArray.getClass());
    }

    @Test
    public void testGetArrayClassToConstruct() {
        assertEquals("[[Z", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_BOOLEAN, false));
        assertEquals("[[Ljava.lang.Boolean;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_BOOLEAN, true));
        assertEquals("[[B", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_BYTE, false));
        assertEquals("[[Ljava.lang.Byte;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_BYTE, true));
        assertEquals("[[S", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_SHORT, false));
        assertEquals("[[Ljava.lang.Short;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_SHORT, true));
        assertEquals("[[I", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_INT, false));
        assertEquals("[[Ljava.lang.Integer;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_INT, true));
        assertEquals("[[J", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_LONG, false));
        assertEquals("[[Ljava.lang.Long;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_LONG, true));
        assertEquals("[[F", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_FLOAT, false));
        assertEquals("[[Ljava.lang.Float;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_FLOAT, true));
        assertEquals("[[D", ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_DOUBLE, false));
        assertEquals("[[Ljava.lang.Double;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_DOUBLE, true));
        assertEquals("[[Ljava.lang.String;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_STRING, false));
        assertEquals("[[Ljava.lang.String;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_STRING, true));
        assertEquals("[[Ljava.lang.Enum;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_ENUM, false));
        assertEquals("[[Ljava.lang.Enum;",
            ZiggyArrayUtils.arrayClassToConstruct(3, ZIGGY_ENUM, true));
    }

    @Test
    public void testConstructPrimitiveArray() {
        Object[] returnObject;
        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_BOOLEAN);
        assertEquals("[[[Z", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_BYTE);
        assertEquals("[[[B", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_SHORT);
        assertEquals("[[[S", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_INT);
        assertEquals("[[[I", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_LONG);
        assertEquals("[[[J", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_FLOAT);
        assertEquals("[[[F", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_DOUBLE);
        assertEquals("[[[D", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_STRING);
        assertEquals("[[[Ljava.lang.String;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructPrimitiveArray(3, 2, ZIGGY_ENUM);
        assertEquals("[[[Ljava.lang.Enum;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

    }

    @Test
    public void testConstructBoxedArray() {
        Object[] returnObject;
        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_BOOLEAN);
        assertEquals("[[[Ljava.lang.Boolean;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_BYTE);
        assertEquals("[[[Ljava.lang.Byte;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_SHORT);
        assertEquals("[[[Ljava.lang.Short;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_INT);
        assertEquals("[[[Ljava.lang.Integer;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_LONG);
        assertEquals("[[[Ljava.lang.Long;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_FLOAT);
        assertEquals("[[[Ljava.lang.Float;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_DOUBLE);
        assertEquals("[[[Ljava.lang.Double;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_STRING);
        assertEquals("[[[Ljava.lang.String;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

        returnObject = (Object[]) ZiggyArrayUtils.constructBoxedArray(3, 2, ZIGGY_ENUM);
        assertEquals("[[[Ljava.lang.Enum;", returnObject.getClass().getName());
        assertEquals(2, returnObject.length);
        assertNull(returnObject[0]);
        assertNull(returnObject[1]);

    }

    @Test
    public void testConstructFullArrays() {
        Object[] returnObject;

        returnObject = (Object[]) ZiggyArrayUtils
            .constructFullPrimitiveArray(new long[] { 3, 4, 5 }, ZIGGY_INT);
        assertEquals("[[[I", returnObject.getClass().getName());
        assertEquals(3, returnObject.length);
        for (int i = 0; i < 3; i++) {
            Object[] level2Array = (Object[]) returnObject[i];
            assertEquals(4, level2Array.length);
            for (int j = 0; j < 4; j++) {
                int[] level3Array = (int[]) level2Array[j];
                assertEquals(5, level3Array.length);
            }
        }

        returnObject = (Object[]) ZiggyArrayUtils.constructFullBoxedArray(new long[] { 3, 4, 5 },
            ZIGGY_INT);
        assertEquals("[[[Ljava.lang.Integer;", returnObject.getClass().getName());
        assertEquals(3, returnObject.length);
        for (int i = 0; i < 3; i++) {
            Object[] level2Array = (Object[]) returnObject[i];
            assertEquals(4, level2Array.length);
            for (int j = 0; j < 4; j++) {
                Object[] level3Array = (Object[]) level2Array[j];
                assertEquals(5, level3Array.length);
            }
        }

        returnObject = (Object[]) ZiggyArrayUtils
            .constructFullPrimitiveArray(new long[] { 0, 0, 0 }, ZIGGY_INT);
        assertEquals("[[[I", returnObject.getClass().getName());
    }

    @Test
    public void testCastNumericToNumeric() {
        double[] doubleArray1 = { 6, 7, 8, 9, 10 };
        Object floatArray1 = ZiggyArrayUtils.castNumericToNumeric(doubleArray1, ZIGGY_FLOAT);
        float[] expected = { 6, 7, 8, 9, 10 };
        assertTrue(Arrays.equals(expected, (float[]) floatArray1));
    }

    @Test
    public void testBooleanNumericCasting() {
        float[] floatArray1 = { 0, 1, 0, 3, 5 };
        Object booleanArray1 = ZiggyArrayUtils.castNumericToBoolean(floatArray1);
        boolean[] expected = { false, true, false, true, true };
        assertTrue(Arrays.equals(expected, (boolean[]) booleanArray1));
        Object intArray1 = ZiggyArrayUtils.castBooleanToNumeric(booleanArray1, ZIGGY_INT);
        int[] expected2 = { 0, 1, 0, 1, 1 };
        assertTrue(Arrays.equals(expected2, (int[]) intArray1));
    }

    @Test
    public void testCastEnumToString() {
        EnumTest[] enumArray1 = { EnumTest.FIRST, EnumTest.SECOND, EnumTest.SECOND, EnumTest.THIRD,
            EnumTest.FIRST };
        Object stringArray1 = ZiggyArrayUtils.castEnumToString(enumArray1);
        String[] expectedResult = { "FIRST", "SECOND", "SECOND", "THIRD", "FIRST" };
        assertTrue(Arrays.equals(expectedResult, (String[]) stringArray1));
    }

    @Test
    public void testCastStringToEnum() {
        String[][] stringArray1 = { { "FIRST", "SECOND", "THIRD" },
            { "SECOND", "FIRST", "SECOND" } };
        Object enumArray1 = ZiggyArrayUtils.castArray(stringArray1, EnumTest.class);
        assertTrue(
            Arrays
                .deepEquals(
                    new EnumTest[][] { { EnumTest.FIRST, EnumTest.SECOND, EnumTest.THIRD },
                        { EnumTest.SECOND, EnumTest.FIRST, EnumTest.SECOND } },
                    (Object[]) enumArray1));
    }

    @Test(expected = PipelineException.class)
    public void testCastError() {
        ZiggyArrayUtils.castArray(new EnumTest[] { EnumTest.FIRST, EnumTest.FIRST }, ZIGGY_INT);
    }

    @Test
    public void testCastMainFunction() {
        double[][] doubleArray2 = { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 } };
        Object o1 = ZiggyArrayUtils.castArray(doubleArray2, ZIGGY_INT);
        int[][] expectedValues = { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 } };
        assertTrue(Arrays.deepEquals((Object[]) o1, (Object[]) expectedValues));
    }

    @Test
    public void testArrayOfTrue() {
        long[] size = { 3L, 4L, 5L };
        boolean[][][] a1 = (boolean[][][]) ZiggyArrayUtils.arrayOfTrue(size);
        for (boolean[][] b1 : a1) {
            for (boolean[] b2 : b1) {
                for (boolean b3 : b2) {
                    assertTrue(b3);
                }
            }
        }
        int[] size2 = ZiggyArrayUtils.longToInt1d(size);
        boolean[][][] a2 = (boolean[][][]) ZiggyArrayUtils.arrayOfTrue(size2);
        assertTrue(Arrays.deepEquals(a1, a2));
    }

    @Test
    public void testFill() {
        byte[][][] byteArray = new byte[2][3][2];
        short[][][] shortArray = new short[2][3][2];
        int[][][] intArray = new int[2][3][2];
        long[][][] longArray = new long[2][3][2];
        float[][][] floatArray = new float[2][3][2];
        double[][][] doubleArray = new double[2][3][2];
        String[][][] stringArray = new String[2][3][2];
        boolean[][][] booleanArray = new boolean[2][3][2];

        ZiggyArrayUtils.fill(byteArray, 1);
        ZiggyArrayUtils.fill(shortArray, 2);
        ZiggyArrayUtils.fill(intArray, 3);
        ZiggyArrayUtils.fill(longArray, 4);
        ZiggyArrayUtils.fill(floatArray, 4.1);
        ZiggyArrayUtils.fill(doubleArray, 4.2);
        ZiggyArrayUtils.fill(stringArray, "value");
        ZiggyArrayUtils.fill(booleanArray, true);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                for (int k = 0; k < 2; k++) {
                    assertEquals(1, byteArray[i][j][k]);
                    assertEquals(2, shortArray[i][j][k]);
                    assertEquals(3, intArray[i][j][k]);
                    assertEquals(4, longArray[i][j][k]);
                    assertEquals(4.1, floatArray[i][j][k], 1e-6);
                    assertEquals(4.2, doubleArray[i][j][k], 0);
                    assertTrue(booleanArray[i][j][k]);
                    assertTrue(stringArray[i][j][k].equals("value"));
                }
            }
        }

        ZiggyArrayUtils.fill(stringArray, 1);
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                for (int k = 0; k < 2; k++) {
                    assertTrue(stringArray[i][j][k].equals("1"));
                }
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFillFailure() {
        EnumTest[] enumArray1 = new EnumTest[5];
        ZiggyArrayUtils.fill(enumArray1, EnumTest.FIRST);
    }

    @Test
    public void testRaggedArrayDetetion() {
        int[] testArray1 = new int[3];
        assertFalse(ZiggyArrayUtils.isRaggedArray(testArray1));
        int[][] testArray2 = new int[3][4];
        assertFalse(ZiggyArrayUtils.isRaggedArray(testArray2));
        testArray2[1] = new int[3];
        assertTrue(ZiggyArrayUtils.isRaggedArray(testArray2));
        int[][][][] testArray4 = new int[3][4][5][6];
        assertFalse(ZiggyArrayUtils.isRaggedArray(testArray4));
        testArray4[1][0] = new int[5][4];
        assertTrue(ZiggyArrayUtils.isRaggedArray(testArray4));
        testArray4[1][1] = new int[5][4];
        testArray4[1][2] = new int[5][4];
        testArray4[1][3] = new int[5][4];
        assertTrue(ZiggyArrayUtils.isRaggedArray(testArray4));
    }

    @Test
    public void testGetValue() {
        // boolean
        assertTrue(Boolean.TRUE.equals(
            ZiggyArrayUtils.getValue(new boolean[] { false, true, false }, new int[] { 1 })));
        // byte
        Number expectedValue = Byte.valueOf((byte) 62);
        assertTrue(expectedValue.equals(ZiggyArrayUtils
            .getValue(new byte[] { (byte) 10, (byte) 62, (byte) 0 }, new int[] { 1 })));
        // short
        expectedValue = Short.valueOf((short) 3);
        assertTrue(expectedValue.equals(ZiggyArrayUtils
            .getValue(new short[] { (short) 10, (short) 3, (short) -12 }, new int[] { 1 })));
        // int
        expectedValue = Integer.valueOf(101010);
        assertTrue(expectedValue
            .equals(ZiggyArrayUtils.getValue(new int[] { 252, 101010, -12 }, new int[] { 1 })));
        // long
        expectedValue = Long.valueOf(3_000_000_000L);
        assertTrue(expectedValue.equals(
            ZiggyArrayUtils.getValue(new long[] { 10L, 3_000_000_000L, -12L }, new int[] { 1 })));
        // float
        expectedValue = Float.valueOf(3.5F);
        assertTrue(expectedValue
            .equals(ZiggyArrayUtils.getValue(new float[] { 100F, 3.5F, -100F }, new int[] { 1 })));
        // double
        expectedValue = Double.valueOf(3.5);
        assertTrue(expectedValue
            .equals(ZiggyArrayUtils.getValue(new double[] { 100, 3.5, -100 }, new int[] { 1 })));
        // string
        String expectedString = "success!";
        assertTrue(expectedString.equals(ZiggyArrayUtils
            .getValue(new String[] { "failure", "success!", "disaster" }, new int[] { 1 })));

        // boxed
        double[] dArray = { 100, 3.5, -100 };
        Object boxedDArray = ZiggyArrayUtils.box(dArray);
        expectedValue = Double.valueOf(3.5);
        assertTrue(expectedValue.equals(ZiggyArrayUtils.getValue(boxedDArray, new int[] { 1 })));

        // multi-dimensional
        int[][][] intMultiArray = new int[3][4][5];
        intMultiArray[2][1][3] = 1001;
        expectedValue = Integer.valueOf(1001);
        assertTrue(
            expectedValue.equals(ZiggyArrayUtils.getValue(intMultiArray, new int[] { 2, 1, 3 })));
    }

    @Test
    public void testSetValues() {
        int[][][] intMultiArray = new int[3][4][5];
        ZiggyArrayUtils.setValue(intMultiArray, new int[] { 2, 1, 3 }, 100);
        assertEquals(100, intMultiArray[2][1][3]);
    }
}
