package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BOOLEAN;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_DOUBLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_ENUM;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_FLOAT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.BOOLEAN_ARRAY_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.FIELD_DATA_TYPE_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.OBJECT_ARRAY_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.OBJECT_ARRAY_DIMS_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.STRING_ARRAY_ATT_NAME;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.module.hdf5.AbstractHdf5Array.ReturnAs;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class PrimitiveHdf5ArrayTest {

    double testDouble;
    float[] testFloat;
    List<Byte> byteList;
    Integer boxedInt;
    String[] stringArray;
    Long[][] boxedLongArray;

    public long fileId;
    public File hdf5File;
    int[][][] intArrayTestField;
    boolean[] booleanArrayTestField;
    String[] stringArrayTestField;
    String stringTestField;
    double doubleScalar;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() throws HDF5LibraryException, NullPointerException, IOException {

        // create an HDF5 file in the temporary folder
        hdf5File = directoryRule.directory().resolve("hdf5TestFile.h5").toFile();
        fileId = H5.H5Fcreate(hdf5File.getAbsolutePath(), HDF5Constants.H5F_ACC_TRUNC, H5P_DEFAULT,
            H5P_DEFAULT);
    }

    @After
    public void tearDown() throws HDF5LibraryException {

        // close the file
        H5.H5Fclose(fileId);
    }

    @Test
    public void testObjectConstructor() {

        // Note that this also tests the setArray() method, which is called during
        // the object constructor; and the convertToPrimitiveArray() method, which
        // is called during the object constructor.

        // instantiate with boxed primitive scalar value
        PrimitiveHdf5Array a1 = new PrimitiveHdf5Array(Double.valueOf(41.5));
        double[] scalarArray = { 41.5 };
        assertTrue(Arrays.equals(scalarArray, (double[]) a1.getArrayObject()));
        assertTrue(Arrays.equals(new long[] { 1 }, a1.getDimensions()));
        assertEquals(ZIGGY_DOUBLE, a1.getHdf5DataType());
        assertTrue(a1.isScalar());
        assertNull(a1.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a1.getReturnAs());
        assertEquals(ZIGGY_DOUBLE, a1.getDataTypeToSave());

        // instantiate with list of values
        float[] floatArray = { (float) 10.0, (float) 10.5, (float) 11.0, (float) 11.5 };
        Float[] boxedFloatArray = ArrayUtils.toObject(floatArray);
        List<Float> floatList = Arrays.asList(boxedFloatArray);
        PrimitiveHdf5Array a2 = new PrimitiveHdf5Array(floatList);
        assertEquals(ZIGGY_FLOAT, a2.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 4 }, a2.getDimensions()));
        assertFalse(a2.isScalar());
        assertTrue(Arrays.equals((float[]) a2.getArrayObject(), floatArray));
        assertNull(a2.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a2.getReturnAs());
        assertEquals(ZIGGY_FLOAT, a2.getDataTypeToSave());

        // instantiate with array of boxed balues
        Integer[][] boxedIntArray = { { 1, 4 }, { 3, 100 } };
        int[][] expectedArray = { { 1, 4 }, { 3, 100 } };
        PrimitiveHdf5Array a3 = new PrimitiveHdf5Array(boxedIntArray);
        assertFalse(a3.isScalar());
        assertTrue(Arrays.deepEquals((Object[]) a3.getArrayObject(), (Object[]) expectedArray));
        assertTrue(Arrays.equals(new long[] { 2, 2 }, a3.getDimensions()));
        assertEquals(ZIGGY_INT, a3.getHdf5DataType());
        assertNull(a3.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a3.getReturnAs());
        assertEquals(ZIGGY_INT, a3.getDataTypeToSave());

        // instantiate with an array of primitive values
        int[][] intArray = { { 1, 4 }, { 3, 100 }, { 7, 12 } };
        PrimitiveHdf5Array a4 = new PrimitiveHdf5Array(intArray);
        assertFalse(a4.isScalar());
        assertEquals(ZIGGY_INT, a4.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 3, 2 }, a4.getDimensions()));
        assertEquals(intArray, a4.getArrayObject());
        assertNull(a4.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a4.getReturnAs());
        assertEquals(ZIGGY_INT, a4.getDataTypeToSave());

        // instantiate with a string scalar
        PrimitiveHdf5Array a5 = new PrimitiveHdf5Array("test string");
        assertTrue(a5.isScalar());
        assertEquals(ZIGGY_STRING, a5.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 1 }, a5.getDimensions()));
        assertTrue(Arrays.equals((Object[]) new String[] { "test string" },
            (String[]) a5.getArrayObject()));
        assertNull(a5.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a5.getReturnAs());
        assertEquals(ZIGGY_STRING, a5.getDataTypeToSave());

        // instantiate with a string list
        List<String> stringList = new ArrayList<>();
        stringList.add("the");
        stringList.add("wild");
        stringList.add("boys");
        PrimitiveHdf5Array a6 = new PrimitiveHdf5Array(stringList);
        assertFalse(a6.isScalar());
        assertEquals(ZIGGY_STRING, a6.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 3 }, a6.getDimensions()));
        assertTrue(Arrays.equals((Object[]) new String[] { "the", "wild", "boys" },
            (Object[]) a6.getArrayObject()));
        assertNull(a6.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a6.getReturnAs());
        assertEquals(ZIGGY_STRING, a6.getDataTypeToSave());

        // instantiate with a string array
        String[] stringArray = { "stringer", "bell", "avon", "barksdale" };
        PrimitiveHdf5Array a7 = new PrimitiveHdf5Array(stringArray);
        assertFalse(a7.isScalar());
        assertTrue(Arrays.equals((String[]) a7.getArrayObject(), stringArray));
        assertTrue(Arrays.equals(new long[] { 4 }, a7.getDimensions()));
        assertEquals(ZIGGY_STRING, a7.getHdf5DataType());
        assertNull(a7.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a7.getReturnAs());
        assertEquals(ZIGGY_STRING, a7.getDataTypeToSave());

        // instantiate with a scalar Enum
        EnumTest enumScalar = EnumTest.SECOND;
        PrimitiveHdf5Array a8 = new PrimitiveHdf5Array(enumScalar);
        assertTrue(a8.isScalar());
        assertEquals(ZIGGY_ENUM, a8.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 1 }, a8.dimensions));
        EnumTest[] arrayObject = (EnumTest[]) a8.getArrayObject();
        assertEquals(enumScalar, arrayObject[0]);
        assertEquals(EnumTest.class, a8.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a8.getReturnAs());
        assertEquals(ZIGGY_STRING, a8.getDataTypeToSave());

        // instantiate with a list of Enums
        List<EnumTest> enumList = new ArrayList<>();
        enumList.add(EnumTest.SECOND);
        enumList.add(EnumTest.THIRD);
        PrimitiveHdf5Array a9 = new PrimitiveHdf5Array(enumList);
        assertFalse(a9.isScalar());
        assertTrue(Arrays.equals(new long[] { 2 }, a9.getDimensions()));
        assertEquals(ZIGGY_ENUM, a9.getHdf5DataType());
        arrayObject = (EnumTest[]) a9.getArrayObject();
        assertTrue(Arrays.equals(arrayObject, new EnumTest[] { EnumTest.SECOND, EnumTest.THIRD }));
        assertEquals(EnumTest.class, a9.getAuxiliaryClass());
        assertEquals(ZIGGY_STRING, a9.getDataTypeToSave());
        assertEquals(ReturnAs.UNKNOWN, a9.getReturnAs());

        // instantiate with an array of Enums
        EnumTest[] enumArray = { EnumTest.FIRST, EnumTest.THIRD, EnumTest.THIRD };
        PrimitiveHdf5Array a10 = new PrimitiveHdf5Array(enumArray);
        assertFalse(a10.isScalar());
        assertTrue(Arrays.equals(new long[] { 3 }, a10.getDimensions()));
        arrayObject = (EnumTest[]) a10.getArrayObject();
        assertTrue(Arrays.equals(arrayObject, enumArray));
        assertEquals(EnumTest.class, a10.getAuxiliaryClass());
        assertEquals(ReturnAs.UNKNOWN, a10.getReturnAs());
        assertEquals(ZIGGY_ENUM, a10.getHdf5DataType());
        assertEquals(ZIGGY_STRING, a10.getDataTypeToSave());

        // instantiate with array of boolean
        boolean[] zArray1 = { true, false, true };
        PrimitiveHdf5Array a11 = new PrimitiveHdf5Array(zArray1);
        assertEquals(ZIGGY_BYTE, a11.getDataTypeToSave());
    }

    @Test
    public void testFieldConstructor() throws NoSuchFieldException, SecurityException {

        // note that this also exercises the isBoxedPrimitive method for fields
        // test with a scalar primitive field
        PrimitiveHdf5Array a1 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("doubleScalar"));
        assertNull(a1.getArrayObject());
        assertNull(a1.getAuxiliaryClass());
        assertNull(a1.getDimensions());
        assertNull(a1.getHdf5DataType());
        assertEquals(ReturnAs.SCALAR, a1.getReturnAs());
        assertEquals(ZIGGY_DOUBLE, a1.getDataTypeOfReturn());
        assertFalse(a1.boxReturn);
        assertEquals("doubleScalar", a1.getFieldName());

        // test with an array primitive field
        PrimitiveHdf5Array a2 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("booleanArray3"));
        assertNull(a2.getArrayObject());
        assertNull(a2.getAuxiliaryClass());
        assertNull(a2.getDimensions());
        assertNull(a2.getHdf5DataType());
        assertEquals(ReturnAs.ARRAY, a2.getReturnAs());
        assertEquals(ZIGGY_BOOLEAN, a2.getDataTypeOfReturn());
        assertFalse(a2.boxReturn);
        assertEquals("booleanArray3", a2.getFieldName());

        // test with a list of boxed primitives
        PrimitiveHdf5Array a3 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("intList"));
        assertNull(a3.getArrayObject());
        assertNull(a3.getAuxiliaryClass());
        assertNull(a3.getDimensions());
        assertNull(a3.getHdf5DataType());
        assertEquals(ReturnAs.LIST, a3.getReturnAs());
        assertEquals(ZIGGY_INT, a3.getDataTypeOfReturn());
        assertTrue(a3.boxReturn);
        assertEquals("intList", a3.getFieldName());

        // test with an enum scalar
        PrimitiveHdf5Array a4 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("enumScalar"));
        assertNull(a4.getArrayObject());
        assertNull(a4.getDimensions());
        assertNull(a4.getHdf5DataType());
        assertEquals(ReturnAs.SCALAR, a4.getReturnAs());
        assertEquals(ZIGGY_ENUM, a4.getDataTypeOfReturn());
        assertFalse(a4.boxReturn);
        assertEquals(EnumTest.class, a4.getAuxiliaryClass());
        assertEquals("enumScalar", a4.getFieldName());
    }

    @Test
    public void testToHdf5() {
        double[][] doubleArray2 = { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 } };
        PrimitiveHdf5Array a1 = new PrimitiveHdf5Array(doubleArray2);
        Object o1 = a1.toHdf5();
        assertTrue(Arrays.deepEquals((Object[]) o1, (Object[]) doubleArray2));

        PrimitiveHdf5Array a4 = new PrimitiveHdf5Array(EnumTest.FIRST);
        Object o2 = a4.toHdf5();
        assertTrue(Arrays.equals((String[]) o2, new String[] { "FIRST" }));

        PrimitiveHdf5Array a2 = new PrimitiveHdf5Array(new boolean[] { true, false });
        Object o3 = a2.toHdf5();
        assertTrue(Arrays.equals((byte[]) o3, new byte[] { 1, 0 }));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToJava() throws NoSuchFieldException, SecurityException {
        PrimitiveHdf5Array a1 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("doubleScalar"));
        a1.setArray(Double.valueOf(53.5));
        assertTrue(Arrays.equals((double[]) a1.getArrayObject(), new double[] { 53.5 }));
        Object o1 = a1.toJava();
        assertEquals(Double.class, o1.getClass());
        Double d1 = (Double) o1;
        assertEquals(53.5, d1, 0);

        PrimitiveHdf5Array a2 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("longArray2"));
        long[][] longArray2 = { { 1, 2 }, { 3, 4 } };
        a2.setArray(longArray2);
        Object o2 = a2.toJava();
        assertTrue(Arrays.deepEquals((Object[]) o2, (Object[]) longArray2));

        PrimitiveHdf5Array a3 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("booleanArray3"));
        boolean[][][] booleanArray3 = { { { true, false }, { true, false } },
            { { false, false }, { true, true } }, { { true, false }, { true, false } } };
        byte[][][] byteArray3 = { { { 1, 0 }, { 1, 0 } }, { { 0, 0 }, { 1, 1 } },
            { { 1, 0 }, { 1, 0 } } };
        a3.setArray(byteArray3);
        Object o3 = a3.toJava();
        assertTrue(Arrays.deepEquals((Object[]) o3, (Object[]) booleanArray3));

        PrimitiveHdf5Array a4 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("intList"));
        int[] intList = { 1, 2, 3, 4, 5 };
        a4.setArray(intList);
        Object o4 = a4.toJava();
        List<Integer> i4 = (List<Integer>) o4;
        for (int i = 0; i < intList.length; i++) {
            assertEquals(intList[i], i4.get(i).intValue());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEmptyToJava() throws NoSuchFieldException, SecurityException {
        PrimitiveHdf5Array a1 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("doubleScalar"));
        Object o1 = a1.toJava();
        assertEquals(Double.class, o1.getClass());
        Double d1 = (Double) o1;
        assertEquals(0.0, d1, 0);

        PrimitiveHdf5Array a2 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("floatArray1"));
        Object o2 = a2.toJava();
        assertEquals("[F", o2.getClass().getName());
        float[] floatArray = (float[]) o2;
        assertEquals(0, floatArray.length);

        PrimitiveHdf5Array a3 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("longArray2"));
        Object o3 = a3.toJava();
        assertEquals("[[J", o3.getClass().getName());
        long[][] longArray = (long[][]) o3;
        assertEquals(0, longArray.length);

        PrimitiveHdf5Array a4 = new PrimitiveHdf5Array(
            PersistableSample1.class.getDeclaredField("intList"));
        Object o4 = a4.toJava();
        assertTrue(List.class.isAssignableFrom(o4.getClass()));
        List<Object> objList = (List<Object>) o4;
        assertEquals(0, objList.size());

    }

    /**
     * Performs a basic test of the ability to write and read an array of numbers to/from HDF5.
     *
     * @throws NullPointerException
     * @throws HDF5Exception
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test
    public void testWriteAndReadNumericArray()
        throws NullPointerException, HDF5Exception, NoSuchFieldException, SecurityException {

        String fieldName = "intArrayTestField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId, ZIGGY_INT, fieldName);

        // create a 3-d array of integer values
        int[][][] intTestArray = { { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } },
            { { 10, 11, 12 }, { 13, 14, 15 }, { 16, 17, 18 } },
            { { 19, 20, 21 }, { 22, 23, 24 }, { 25, 26, 27 } } };

        PrimitiveHdf5Array intArrayPersistable = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(intTestArray);

        // execute the method
        intArrayPersistable.write(fieldGroupId, fieldName);

        // make sure none of the attributes were added that shouldn't be added

        assertFalse(H5.H5Aexists(fieldGroupId, BOOLEAN_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, STRING_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, FIELD_DATA_TYPE_ATT_NAME));
        // assertEquals(HDF5_INT, Hdf5ModuleInterface.readDataTypeAttribute(fieldGroupId,
        // stringTestField));
        H5.H5Gclose(fieldGroupId);

        // now read using the module interface code
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        intArrayPersistable = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(this.getClass().getDeclaredField(fieldName));
        intArrayPersistable.read(fieldGroupId);
        assertTrue(
            Arrays.deepEquals((Object[]) intArrayPersistable.arrayObject, (Object[]) intTestArray));

    }

    /**
     * Tests the ability of the system to write a boolean array as a byte array to HDF5, and adds
     * the correct annotation to the group. Also tests the ability to read the array back and
     * convert back to boolean.
     *
     * @throws HDF5LibraryException
     * @throws NullPointerException
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test
    public void testWriteAndReadBooleanArray()
        throws HDF5LibraryException, NullPointerException, NoSuchFieldException, SecurityException {
        String fieldName = "booleanArrayTestField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // create an array of boolean values
        boolean[] booleanTestArray = { true, false, true };
        PrimitiveHdf5Array persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(booleanTestArray);
        // execute the method
        Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId, ZIGGY_BYTE, fieldName);
        persistableField.write(fieldGroupId, fieldName);

        // make sure the correct attributes were added
        assertTrue(H5.H5Aexists(fieldGroupId, BOOLEAN_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, STRING_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, FIELD_DATA_TYPE_ATT_NAME));
        assertEquals(ZIGGY_BYTE,
            Hdf5ModuleInterface.readDataTypeAttribute(fieldGroupId, stringTestField));
        H5.H5Gclose(fieldGroupId);

        // read the array back
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(this.getClass().getDeclaredField(fieldName));
        persistableField.read(fieldGroupId);
        Object returnedArray = persistableField.toJava();
        assertTrue(Arrays.equals((boolean[]) returnedArray, booleanTestArray));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));
        H5.H5Gclose(fieldGroupId);

    }

    /**
     * Tests the ability to write and read a string array to/from HDF5.
     *
     * @throws NullPointerException
     * @throws HDF5Exception
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test
    public void testWriteAndReadStringArray()
        throws NullPointerException, HDF5Exception, NoSuchFieldException, SecurityException {
        String fieldName = "stringArrayTestField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId, ZIGGY_STRING, fieldName);

        // create an array of Strings
        String[][] stringTestArray = { { "the", "wild", "boys", "were", "calling", "" },
            { "on", "their", "way", "back", "from", "the" } };
        PrimitiveHdf5Array persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(stringTestArray);

        // execute the method
        persistableField.write(fieldGroupId, fieldName);

        // make sure the correct attributes were added
        assertFalse(H5.H5Aexists(fieldGroupId, BOOLEAN_ARRAY_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, STRING_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, FIELD_DATA_TYPE_ATT_NAME));
        assertEquals(ZIGGY_STRING,
            Hdf5ModuleInterface.readDataTypeAttribute(fieldGroupId, stringTestField));
        H5.H5Gclose(fieldGroupId);

        // read the array back
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(this.getClass().getDeclaredField(fieldName));
        persistableField.read(fieldGroupId);
        assertTrue(
            Arrays.deepEquals((Object[]) persistableField.arrayObject, (Object[]) stringTestArray));
    }

    /**
     * Tests the ability to write a scalar string to HDF5.
     *
     * @throws NullPointerException
     * @throws HDF5Exception
     */
    @Test
    public void testWriteScalarString() throws NullPointerException, HDF5Exception {
        String fieldName = "stringTestField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // create a string
        String scalarString = "scalar";

        PrimitiveHdf5Array persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(scalarString);
        // execute the method
        persistableField.write(fieldGroupId, fieldName);

        // retrieve the string arrays
        long dataset = H5.H5Dopen(fieldGroupId, fieldName, H5P_DEFAULT);
        String[] stringValue = new String[1];
        long typeId = H5.H5Dget_type(dataset);
        H5.H5Dread_VLStrings(dataset, typeId, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
            H5P_DEFAULT, stringValue);
        assertEquals(scalarString, stringValue[0]);
        H5.H5Dclose(dataset);
        H5.H5Tclose(typeId);

        // make sure the correct attributes were added
        assertFalse(H5.H5Aexists(fieldGroupId, BOOLEAN_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, STRING_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));
        H5.H5Gclose(fieldGroupId);
    }

    /**
     * Tests the ability to read a scalar field (primitive or Persistable) back from HDF5.
     *
     * @throws HDF5LibraryException
     * @throws NullPointerException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testReadScalarField()
        throws HDF5LibraryException, NullPointerException, NoSuchFieldException, SecurityException {

        Double dScalar = 53.5;
        PrimitiveHdf5Array unboxedField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(this.getClass().getDeclaredField("doubleScalar"));
        PrimitiveHdf5Array persistableField = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(dScalar);

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, "doubleScalar", H5P_DEFAULT, H5P_DEFAULT,
            H5P_DEFAULT);
        Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId, ZIGGY_DOUBLE, "doubleScalar");
        persistableField.write(fieldGroupId, "doubleScalar");
        H5.H5Gclose(fieldGroupId);

        // read the double scalar back into an object
        fieldGroupId = H5.H5Gopen(fileId, "doubleScalar", H5P_DEFAULT);
        unboxedField.read(fieldGroupId);

        // the scalar is stored as a primitive array
        double[] doubleObject = (double[]) unboxedField.arrayObject;
        assertEquals(53.5, doubleObject[0], 0);

        // the scalar is returned to Java as a boxed scalar
        Double returnObject = (Double) unboxedField.toJava();
        assertEquals(53.5, returnObject, 0);
        H5.H5Gclose(fieldGroupId);

    }

    /**
     * Tests the ability to read a list field from HDF5.
     *
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws HDF5LibraryException
     * @throws NullPointerException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReadListField()
        throws NoSuchFieldException, SecurityException, HDF5LibraryException, NullPointerException {
        PersistableSample1 pTest1 = PersistableSample1.newInstance(3, 3, 3, 3, 3, 3, 3);
        PrimitiveHdf5Array iListPData = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(pTest1.intList);
        PrimitiveHdf5Array iListResult = (PrimitiveHdf5Array) AbstractHdf5Array
            .newInstance(pTest1.getClass().getDeclaredField("intList"));

        // create a parent group for the list
        long fieldGroupId = H5.H5Gcreate(fileId, "intList", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId, ZIGGY_INT, "intList");
        iListPData.write(fieldGroupId, "intList");
        H5.H5Gclose(fieldGroupId);

        // read back into a list field
        fieldGroupId = H5.H5Gopen(fileId, "intList", H5P_DEFAULT);
        iListResult.read(fieldGroupId);
        Object result = iListResult.toJava();
        List<Integer> iResult = (List<Integer>) result;
        // check values
        assertEquals(pTest1.intList.size(), iResult.size());
        for (int i = 0; i < iResult.size(); i++) {
            assertEquals(pTest1.intList.get(i).intValue(), iResult.get(i).intValue());
        }

        H5.H5Gclose(fieldGroupId);
    }

    @Test
    public void testGetHyperslab() {

        Random rd = new Random();
        int[][][] randomArray = new int[3][4][5];
        for (int i = 0; i < randomArray.length; i++) {
            for (int j = 0; j < randomArray[i].length; j++) {
                for (int k = 0; k < randomArray[i][j].length; k++) {
                    randomArray[i][j][k] = rd.nextInt();
                }
            }
        }
        PrimitiveHdf5Array a = new PrimitiveHdf5Array(randomArray);
        PrimitiveHdf5Array.Hyperslab hyperslab = a.new Hyperslab(new long[] { 1, 2, 5 },
            new long[] { 1, 2, 0 });
        int[][][] slabArray = (int[][][]) hyperslab.getHyperslab();
        assertTrue(Arrays.equals(new long[] { 1, 2, 5 }, ZiggyArrayUtils.getArraySize(slabArray)));
        assertTrue(Arrays.equals(randomArray[1][2], slabArray[0][0]));
        assertTrue(Arrays.equals(randomArray[1][3], slabArray[0][1]));
        hyperslab = a.new Hyperslab(new long[] { 1, 1, 2 }, new long[] { 2, 3, 1 });
        slabArray = (int[][][]) hyperslab.getHyperslab();
        assertTrue(Arrays.equals(new long[] { 1, 1, 2 }, ZiggyArrayUtils.getArraySize(slabArray)));
        assertEquals(randomArray[2][3][1], slabArray[0][0][0]);
        assertEquals(randomArray[2][3][2], slabArray[0][0][1]);

        hyperslab = a.new Hyperslab(new long[] { 1, 4, 5 }, new long[] { 1, 0, 0 });
        slabArray = (int[][][]) hyperslab.getHyperslab();
        assertTrue(Arrays.equals(new long[] { 1, 4, 5 }, ZiggyArrayUtils.getArraySize(slabArray)));
        assertTrue(Arrays.equals(randomArray[1][0], slabArray[0][0]));
        assertTrue(Arrays.equals(randomArray[1][1], slabArray[0][1]));
        assertTrue(Arrays.equals(randomArray[1][2], slabArray[0][2]));
        assertTrue(Arrays.equals(randomArray[1][3], slabArray[0][3]));

        hyperslab = a.new Hyperslab(new long[] { 3, 4, 5 }, new long[] { 0, 0, 0 });
        slabArray = (int[][][]) hyperslab.getHyperslab();
        assertTrue(Arrays.equals(randomArray, slabArray));
    }

    @Test
    public void testPutHyperslab() throws NoSuchFieldException, SecurityException {
        Random rd = new Random();
        int[][][] randomArray = new int[1][2][5];
        for (int i = 0; i < randomArray.length; i++) {
            for (int j = 0; j < randomArray[i].length; j++) {
                for (int k = 0; k < randomArray[i][j].length; k++) {
                    randomArray[i][j][k] = rd.nextInt();
                }
            }
        }

        PrimitiveHdf5Array a = new PrimitiveHdf5Array(
            HyperslabTestData.class.getDeclaredField("intArray3"));
        PrimitiveHdf5Array.Hyperslab hyperslab = a.new Hyperslab(
            ZiggyArrayUtils.getArraySize(randomArray), new long[] { 1, 2, 0 },
            new long[] { 3, 4, 5 });
        hyperslab.putHyperslab(randomArray);
        int[][][] aArray = (int[][][]) a.getArrayObject();
        assertNull(aArray[0]);
        assertNull(aArray[2]);
        assertNotNull(aArray[1]);
        assertNull(aArray[1][0]);
        assertNull(aArray[1][1]);
        assertNotNull(aArray[1][2]);
        assertNotNull(aArray[1][3]);
        assertTrue(Arrays.equals(aArray[1][2], randomArray[0][0]));
        assertTrue(Arrays.equals(aArray[1][3], randomArray[0][1]));

        int[][][] randomArray2 = new int[1][1][2];
        for (int i = 0; i < randomArray2.length; i++) {
            for (int j = 0; j < randomArray2[i].length; j++) {
                for (int k = 0; k < randomArray2[i][j].length; k++) {
                    randomArray2[i][j][k] = rd.nextInt();
                }
            }
        }
        hyperslab = a.new Hyperslab(ZiggyArrayUtils.getArraySize(randomArray2),
            new long[] { 2, 3, 1 }, new long[] { 3, 4, 5 });
        hyperslab.putHyperslab(randomArray2);
        aArray = (int[][][]) a.getArrayObject();
        assertNull(aArray[0]);
        assertNotNull(aArray[2]);
        assertNotNull(aArray[1]);
        assertNull(aArray[1][0]);
        assertNull(aArray[1][1]);
        assertNotNull(aArray[1][2]);
        assertNotNull(aArray[1][3]);
        assertTrue(Arrays.equals(aArray[1][2], randomArray[0][0]));
        assertTrue(Arrays.equals(aArray[1][3], randomArray[0][1]));
        assertNull(aArray[2][0]);
        assertNull(aArray[2][1]);
        assertNull(aArray[2][2]);
        assertNotNull(aArray[2][3]);
        assertEquals(0, aArray[2][3][0]);
        assertEquals(randomArray2[0][0][0], aArray[2][3][1]);
        assertEquals(randomArray2[0][0][1], aArray[2][3][2]);
        assertEquals(0, aArray[2][3][3]);
        assertEquals(0, aArray[2][3][4]);

        int[][][] randomArray3 = new int[1][4][5];
        for (int i = 0; i < randomArray3.length; i++) {
            for (int j = 0; j < randomArray3[i].length; j++) {
                for (int k = 0; k < randomArray3[i][j].length; k++) {
                    randomArray3[i][j][k] = rd.nextInt();
                }
            }
        }
        a = new PrimitiveHdf5Array(HyperslabTestData.class.getDeclaredField("intArray3"));
        hyperslab = a.new Hyperslab(ZiggyArrayUtils.getArraySize(randomArray3),
            new long[] { 1, 0, 0 }, new long[] { 3, 4, 5 });
        hyperslab.putHyperslab(randomArray3);
        aArray = (int[][][]) a.getArrayObject();
        assertTrue(Arrays.equals(randomArray3[0], aArray[1]));

        int[][][] randomArray4 = new int[3][4][5];
        for (int i = 0; i < randomArray4.length; i++) {
            for (int j = 0; j < randomArray4[i].length; j++) {
                for (int k = 0; k < randomArray4[i][j].length; k++) {
                    randomArray4[i][j][k] = rd.nextInt();
                }
            }
        }
        a = new PrimitiveHdf5Array(HyperslabTestData.class.getDeclaredField("intArray3"));
        hyperslab = a.new Hyperslab(ZiggyArrayUtils.getArraySize(randomArray4),
            new long[] { 0, 0, 0 }, new long[] { 3, 4, 5 });
        hyperslab.putHyperslab(randomArray4);
        aArray = (int[][][]) a.getArrayObject();
        assertTrue(Arrays.equals(randomArray4, aArray));

    }

    @Test
    public void testHyperslabIteratorConstructors() throws NoSuchFieldException, SecurityException,
        IllegalArgumentException, IllegalAccessException {

        // iterator for writing to HDF5
        int[][][] intArray = (int[][][]) ZiggyArrayUtils.constructFullArray(new long[] { 3, 4, 5 },
            ZIGGY_INT, false);
        PrimitiveHdf5Array a = new PrimitiveHdf5Array(intArray);
        PrimitiveHdf5Array.HyperslabIterator hI = a.new HyperslabIterator();
        assertEquals(Hdf5ModuleInterface.MAX_BYTES_PER_HYPERSLAB / 4,
            hI.getMaxElementsPerHyperRectangle());
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getSize()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getLastSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 1 }, hI.getnRectangles()));
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hI.getCounter()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getFullArraySize()));

        // iterator for reading from HDF5
        a = new PrimitiveHdf5Array(HyperslabTestData.class.getDeclaredField("intArray3"));
        hI = a.new HyperslabIterator(new long[] { 3, 4, 5 }, ZIGGY_INT);
        assertEquals(Hdf5ModuleInterface.MAX_BYTES_PER_HYPERSLAB / 4,
            hI.getMaxElementsPerHyperRectangle());
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getSize()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getLastSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 1 }, hI.getnRectangles()));
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hI.getCounter()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getFullArraySize()));

        // now a test in which the # of bytes is limited, so we can really see whether
        // the iterator's slab parameters are correctly set
        hI = a.new HyperslabIterator(new long[] { 3, 4, 5 }, ZIGGY_INT, 16);
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hI.getCounter()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getFullArraySize()));
        assertEquals(4, hI.getMaxElementsPerHyperRectangle());
        assertTrue(Arrays.equals(new int[] { 3, 4, 2 }, hI.getnRectangles()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 4 }, hI.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 1 }, hI.getLastSize()));

        // Test the case of a String array.
        hI = a.new HyperslabIterator(new long[] { 3, 4, 5 }, ZIGGY_STRING, 16);
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hI.getCounter()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, hI.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 3, 2, 1 }, hI.getnRectangles()));
        assertEquals(16, hI.getMaxElementsPerHyperRectangle());
        assertTrue(Arrays.equals(new int[] { 1, 3, 5 }, hI.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 5 }, hI.getLastSize()));
    }

    @Test
    public void testHyperslabIteration() throws NoSuchFieldException, SecurityException,
        IllegalArgumentException, IllegalAccessException {

        PrimitiveHdf5Array a = new PrimitiveHdf5Array(
            HyperslabTestData.class.getDeclaredField("intArray3"));
        PrimitiveHdf5Array.HyperslabIterator hI = a.new HyperslabIterator(new long[] { 3, 4, 5 },
            ZIGGY_INT, 60);
        assertTrue(Arrays.equals(new int[] { 3, 2, 1 }, hI.getnRectangles()));
        assertTrue(Arrays.equals(new int[] { 1, 3, 5 }, hI.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 5 }, hI.getLastSize()));
        assertTrue(hI.hasNext());

        PrimitiveHdf5Array.Hyperslab h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 3, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 0, 0, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));
        h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 1, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 0, 3, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));

        h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 3, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 1, 0, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));
        h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 1, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 1, 3, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));

        h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 3, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 2, 0, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));
        h = hI.next();
        assertTrue(Arrays.equals(new long[] { 1, 1, 5 }, h.hyperslabBlock()));
        assertTrue(Arrays.equals(new long[] { 2, 3, 0 }, h.hyperslabStart()));
        assertTrue(Arrays.equals(new int[] { 3, 4, 5 }, h.getFullArraySize()));

        assertFalse(hI.hasNext());
    }

    class HyperslabTestData {
        int[][][] intArray3;
    }
}
