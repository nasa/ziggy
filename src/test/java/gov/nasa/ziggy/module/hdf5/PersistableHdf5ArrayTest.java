package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_PERSISTABLE;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.BOOLEAN_ARRAY_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.OBJECT_ARRAY_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.OBJECT_ARRAY_DIMS_ATT_NAME;
import static gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface.STRING_ARRAY_ATT_NAME;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.module.hdf5.AbstractHdf5Array.ReturnAs;
import gov.nasa.ziggy.module.io.Persistable;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class PersistableHdf5ArrayTest {

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

        // scalar
        PersistableSample1 p1 = PersistableSample1.newInstance(1, 1, 1, 1, 1, 1, 1);
        PersistableHdf5Array a1 = new PersistableHdf5Array(p1);
        assertEquals(PersistableSample1.class, a1.getAuxiliaryClass());
        Object o1 = a1.getArrayObject();
        assertTrue(o1.getClass().isArray());
        Object[] oa1 = (Object[]) o1;
        assertEquals(1, oa1.length);
        assertEquals(p1, oa1[0]);
        assertTrue(a1.isScalar());
        assertEquals(ReturnAs.UNKNOWN, a1.getReturnAs());
        assertEquals(ZIGGY_PERSISTABLE, a1.getHdf5DataType());
        assertTrue(Arrays.equals(new long[] { 1 }, a1.getDimensions()));

        // array
        PersistableSample1[][] p2 = new PersistableSample1[3][2];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 2; j++) {
                p2[i][j] = PersistableSample1.newInstance(1, 1, 1, 1, 1, 1, 1);
            }
        }
        PersistableHdf5Array a2 = new PersistableHdf5Array(p2);
        Object o2 = a2.getArrayObject();
        assertTrue(o1.getClass().isArray());
        assertTrue(Arrays.equals(new long[] { 3, 2 }, a2.getDimensions()));
        assertFalse(a2.isScalar());
        assertEquals(ReturnAs.UNKNOWN, a2.getReturnAs());
        assertEquals(ZIGGY_PERSISTABLE, a2.getHdf5DataType());
        assertEquals(PersistableSample1.class, a2.getAuxiliaryClass());
        PersistableSample1[][] oa2 = (PersistableSample1[][]) o2;
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 2; j++) {
                assertEquals(p2[i][j], oa2[i][j]);
            }
        }

        // list
        List<PersistableSample1> p3 = new ArrayList<>();
        p3.add(p2[0][0]);
        p3.add(p2[2][1]);
        PersistableHdf5Array a3 = new PersistableHdf5Array(p3);
        Object o3 = a3.getArrayObject();
        assertTrue(o3.getClass().isArray());
        assertTrue(Arrays.equals(new long[] { 2 }, a3.getDimensions()));
        assertEquals(ReturnAs.UNKNOWN, a2.getReturnAs());
        assertEquals(ZIGGY_PERSISTABLE, a2.getHdf5DataType());
        assertEquals(PersistableSample1.class, a3.getAuxiliaryClass());
        PersistableSample1[] oa3 = (PersistableSample1[]) o3;
        assertEquals(p3.get(0), oa3[0]);
        assertEquals(p3.get(1), oa3[1]);
        assertFalse(a3.isScalar());
    }

    @Test
    public void testFieldConstructor() throws NoSuchFieldException, SecurityException {

        // scalar
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        assertNull(a1.getArrayObject());
        assertEquals(ReturnAs.SCALAR, a1.getReturnAs());
        assertEquals(PersistableSample1.class, a1.getAuxiliaryClass());
        assertNull(a1.getHdf5DataType());
        assertNull(a1.getDimensions());
        assertEquals("persistableScalar1", a1.getFieldName());

        // array
        PersistableHdf5Array a2 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableArray2"));
        assertNull(a2.getArrayObject());
        assertEquals(ReturnAs.ARRAY, a2.getReturnAs());
        assertEquals(PersistableSample1.class, a2.getAuxiliaryClass());
        assertNull(a2.getHdf5DataType());
        assertNull(a2.getDimensions());
        assertEquals("persistableArray2", a2.getFieldName());

        // list
        PersistableHdf5Array a3 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableList"));
        assertNull(a3.getArrayObject());
        assertEquals(ReturnAs.LIST, a3.getReturnAs());
        assertEquals(PersistableSample1.class, a3.getAuxiliaryClass());
        assertNull(a3.getHdf5DataType());
        assertNull(a3.getDimensions());
        assertEquals("persistableList", a3.getFieldName());
    }

    @Test
    public void testArrayClassToConstruct() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        String arrayClass = a1.arrayClassToConstruct(1);
        assertEquals("gov.nasa.ziggy.module.hdf5.PersistableSample1", arrayClass);
        arrayClass = a1.arrayClassToConstruct(2);
        assertEquals("[Lgov.nasa.ziggy.module.hdf5.PersistableSample1;", arrayClass);
    }

    @Test
    public void TestConstructArray() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        Object o1 = a1.constructArray(3, 4);
        assertEquals("[[[Lgov.nasa.ziggy.module.hdf5.PersistableSample1;", o1.getClass().getName());
        Object[] oa1 = (Object[]) o1;
        assertEquals(4, oa1.length);
        for (int i = 0; i < 4; i++) {
            assertNull(oa1[i]);
        }
    }

    @Test
    public void testSetDimensions() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        long[] dims = { 3, 4, 5 };
        a1.setDimensions(dims);
        assertTrue(Arrays.equals(dims, a1.getDimensions()));
        Object o1 = a1.getArrayObject();
        assertEquals("[[[Lgov.nasa.ziggy.module.hdf5.PersistableSample1;", o1.getClass().getName());
        Object[] oa1 = (Object[]) o1;
        assertEquals(3, oa1.length);
        for (int i = 0; i < 3; i++) {
            Object[] oa2 = (Object[]) oa1[i];
            assertEquals(4, oa2.length);
            for (int j = 0; j < 4; j++) {
                Object[] oa3 = (Object[]) oa2[j];
                assertEquals(5, oa3.length);
                for (int k = 0; k < 5; k++) {
                    assertNull(oa3[k]);
                }
            }
        }
    }

    @Test
    public void testSetAndGetArrayMembers() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableArray2"));
        a1.setDimensions(new long[] { 3, 3 });
        PersistableSample1[][] p1 = new PersistableSample1[3][3];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                p1[i][j] = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
            }
        }
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                a1.setArrayMember(p1[i][j], new long[] { i, j });
            }
        }
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                Object p2 = a1.getArrayMember(new long[] { i, j });
                PersistableSample1 pt1 = (PersistableSample1) p2;
                assertEquals(pt1, p1[i][j]);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToJava() throws NoSuchFieldException, SecurityException {

        // scalar return
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        a1.setDimensions(new long[] { 1 });
        PersistableSample1 p1 = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
        a1.setArrayMember(p1, new long[] { 0 });
        Object o1 = a1.toJava();
        assertEquals(p1, o1);

        // array return
        PersistableHdf5Array a2 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableArray2"));
        a2.setDimensions(new long[] { 3, 3 });
        PersistableSample1[][] p2 = new PersistableSample1[3][3];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                p2[i][j] = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
            }
        }
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                a2.setArrayMember(p2[i][j], new long[] { i, j });
            }
        }
        Object o2 = a2.toJava();
        PersistableSample1[][] oa2 = (PersistableSample1[][]) o2;
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                assertEquals(p2[i][j], oa2[i][j]);
            }
        }

        // list return
        PersistableHdf5Array a3 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableList"));
        a3.setDimensions(new long[] { 3 });
        PersistableSample1[] p3 = new PersistableSample1[3];
        for (int i = 0; i < 3; i++) {
            p3[i] = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
            a3.setArrayMember(p3[i], new long[] { i });
        }
        Object o3 = a3.toJava();
        List<PersistableSample1> oa3 = (List<PersistableSample1>) o3;
        for (int i = 0; i < 3; i++) {
            assertEquals(p3[i], oa3.get(i));
        }
    }

    @Test
    public void testArrayCounter() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a2 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableArray2"));
        a2.setDimensions(new long[] { 3, 3 });
        long[] location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 0, 0 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 1, 0 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 2, 0 }, location));

        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 0, 1 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 1, 1 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 2, 1 }, location));

        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 0, 2 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 1, 2 }, location));
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 2, 2 }, location));

        location = a2.nextArrayLocation();
        assertNull(location);

        a2.resetArrayLocationCounter();
        location = a2.nextArrayLocation();
        assertTrue(Arrays.equals(new long[] { 0, 0 }, location));
    }

    @Test
    public void testNewPersistableObject() throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a1 = new PersistableHdf5Array(
            PersistableSample2.class.getDeclaredField("persistableScalar1"));
        Object newObject = a1.newPersistableObject();
        assertEquals(PersistableSample1.class, newObject.getClass());
    }

    @Test
    public void testDetectionOfPrimitiveScalarFields()
        throws NoSuchFieldException, SecurityException {
        PersistableHdf5Array a2 = new PersistableHdf5Array(new PersistableSample3());
        assertTrue(a2.areAllFieldsPrimitiveScalar());

        PersistableSample3[] p3 = new PersistableSample3[3];
        p3[0] = new PersistableSample3();
        p3[1] = new PersistableSample3();
        p3[2] = new PersistableSample3();
        a2 = new PersistableHdf5Array(p3);
        assertTrue(a2.areAllFieldsPrimitiveScalar());

        PersistableSample1 p1 = PersistableSample1.newInstance(1, 1, 1, 1, 1, 1, 1);
        a2 = new PersistableHdf5Array(p1);
        assertFalse(a2.areAllFieldsPrimitiveScalar());

        a2 = new PersistableHdf5Array(
            PersistableSample4.class.getDeclaredField("persistableTest3Array"));
        assertTrue(a2.areAllFieldsPrimitiveScalar());
        a2 = new PersistableHdf5Array(
            PersistableSample4.class.getDeclaredField("persistableTest2"));
        assertFalse(a2.areAllFieldsPrimitiveScalar());
    }

    /**
     * Tests the ability to read a scalar Persistable field back from HDF5.
     *
     * @throws HDF5LibraryException
     * @throws NullPointerException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testReadScalarField()
        throws HDF5LibraryException, NullPointerException, NoSuchFieldException, SecurityException {

        // now for the possibility of a scalar that is a Persistable object

        PersistableSample2 pTest3 = generatePersistableTest2Object();
        PersistableHdf5Array pField = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(pTest3.persistableScalar1);
        PersistableHdf5Array pField2 = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(pTest3.getClass().getDeclaredField("persistableScalar1"));

        long fieldGroupId = H5.H5Gcreate(fileId, "persistableScalar1", H5P_DEFAULT, H5P_DEFAULT,
            H5P_DEFAULT);
        pField.write(fieldGroupId, "persistableScalar1");
        H5.H5Gclose(fieldGroupId);

        fieldGroupId = H5.H5Gopen(fileId, "persistableScalar1", H5P_DEFAULT);
        pField2.read(fieldGroupId);

        PersistableSample1 pTest1Result = (PersistableSample1) pField2.toJava();
        PersistableSample1 pTest1Input = pTest3.persistableScalar1;
        assertEquals(pTest1Input, pTest1Result);

        H5.H5Gclose(fieldGroupId);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReadListField()
        throws NoSuchFieldException, SecurityException, HDF5LibraryException, NullPointerException {

        PersistableSample2 pTest3 = generatePersistableTest2Object();
        PersistableHdf5Array pListData = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(pTest3.persistableList);
        PersistableHdf5Array pListResult = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(pTest3.getClass().getDeclaredField("persistableList"));
        long fieldGroupId = H5.H5Gcreate(fileId, "persistableList", H5P_DEFAULT, H5P_DEFAULT,
            H5P_DEFAULT);
        pListData.write(fieldGroupId, "persistableList");
        H5.H5Gclose(fieldGroupId);

        fieldGroupId = H5.H5Gopen(fileId, "persistableList", H5P_DEFAULT);
        pListResult.read(fieldGroupId);
        List<PersistableSample1> pResult = (List<PersistableSample1>) pListResult.toJava();
        assertEquals(pTest3.persistableList.size(), pResult.size());
        for (int i = 0; i < pTest3.persistableList.size(); i++) {
            assertEquals(pTest3.persistableList.get(i), pResult.get(i));
        }
        H5.H5Gclose(fieldGroupId);
    }

    /**
     * Tests the ability to write a scalar Persistable object (with fields of primitive arrays) to
     * HDF5.
     *
     * @throws HDF5LibraryException
     * @throws NullPointerException
     */
    @Test
    public void testPersistableScalarInterface() throws HDF5LibraryException, NullPointerException {
        String fieldName = "persistableScalarField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // create an object of class PersistableTest1
        PersistableSample1 persistableTest1 = PersistableSample1.newInstance(4, 3, 5, 7, 9, 11, 13);
        List<Long> subGroupIds = new PersistableHdf5Array(persistableTest1)
            .writePersistableScalarObject(fieldGroupId, "/" + fieldName);

        // check that the # of groups that got generated matches expectations
        assertEquals(7, subGroupIds.size());
        H5.H5Gclose(fieldGroupId);
    }

    /**
     * Tests the ability to write a Persistable object that has arrays of persistable objects as
     * fields.
     *
     * @throws HDF5LibraryException
     * @throws NullPointerException
     */
    @Test
    public void testPersistableObjectInterface() throws HDF5LibraryException, NullPointerException {
        String fieldName = "persistableObjectField";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // create an object of class PersistableTest2
        PersistableSample2 persistableTest2 = generatePersistableTest2Object();
        PersistableHdf5Array persistableArray = new PersistableHdf5Array(persistableTest2);
        persistableArray.setCreateGroupsForMissingFields(false);
        List<Long> subGroupIds = persistableArray.writePersistableScalarObject(fieldGroupId,
            fieldName);

        // check that the # of groups that got generated matches expectations
        assertEquals(99, subGroupIds.size());
        H5.H5Gclose(fieldGroupId);

        // Now set up to create groups for null fields and do the test again

        fieldName = "newPersistableObjectField";
        fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        persistableArray = new PersistableHdf5Array(persistableTest2);
        persistableArray.setCreateGroupsForMissingFields(true);
        subGroupIds = persistableArray.writePersistableScalarObject(fieldGroupId, fieldName);
        assertEquals(100, subGroupIds.size());
        H5.H5Gclose(fieldGroupId);
    }

    /**
     * Tests the ability to write an array of Persistables to HDF5, and to read same back.
     *
     * @throws NullPointerException
     * @throws HDF5Exception
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test
    public void testWriteAndReadPersistableObjectArray()
        throws NullPointerException, HDF5Exception, NoSuchFieldException, SecurityException {
        String fieldName = "persistableArray2";

        // create a parent group for the array
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // create an object of class PersistableTest2
        PersistableSample2 persistableTest2 = generatePersistableTest2Object();
        PersistableHdf5Array persistableArray = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(persistableTest2.persistableArray2);
        persistableArray.write(fieldGroupId, fieldName);

        // check the attributes
        assertFalse(H5.H5Aexists(fieldGroupId, BOOLEAN_ARRAY_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, STRING_ARRAY_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_ATT_NAME));
        assertTrue(H5.H5Aexists(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME));
        assertFalse(H5.H5Aexists(fieldGroupId, EMPTY_FIELD_ATT_NAME));

        // retrieve the dimensions of the array and check
        long attributeId = H5.H5Aopen(fieldGroupId, OBJECT_ARRAY_DIMS_ATT_NAME, H5P_DEFAULT);
        long dataSpaceId = H5.H5Aget_space(attributeId);
        int nDims = H5.H5Sget_simple_extent_ndims(dataSpaceId);
        long[] dims = new long[nDims];
        long[] maxDims = new long[nDims];
        H5.H5Sget_simple_extent_dims(dataSpaceId, dims, maxDims);
        assertEquals(nDims, 1);
        assertEquals(dims[0], 2);
        long[] persistableDims = new long[(int) dims[0]];
        H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), persistableDims);
        assertTrue(Arrays.equals(new long[] { 3, 2 }, persistableDims));
        H5.H5Sclose(dataSpaceId);
        H5.H5Aclose(attributeId);
        H5.H5Gclose(fieldGroupId);

        // retrieve the array and check its contents

        PersistableHdf5Array persistableArray2 = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(PersistableSample2.class.getDeclaredField("persistableArray2"));
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableArray2.read(fieldGroupId);
        Object returnedPersistableArray = persistableArray2.toJava();
        assertTrue(Arrays.deepEquals((Object[]) returnedPersistableArray,
            (Object[]) persistableTest2.persistableArray2));
    }

    /**
     * Tests that an object with only scalar primitive fields is correctly written and read.
     */
    @Test
    public void testObjectWithScalarPrimitiveFields() {

        PersistableWithScalarPrimitives p = new PersistableWithScalarPrimitives();
        p.setBooleanScalar(true);
        p.setFloatScalar(92.7F);
        p.setIntScalar(37);
        p.setStringScalar("Allons-y");

        // Write the object
        String fieldName = "persistableObject";
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        PersistableHdf5Array persistableArray = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(p);
        persistableArray.write(fieldGroupId, fieldName);
        H5.H5Gclose(fieldGroupId);

        // retrieve the array and check its contents
        PersistableHdf5Array persistableArray2 = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(new PersistableWithScalarPrimitives());
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableArray2.read(fieldGroupId);
        PersistableWithScalarPrimitives returnedPersistableArray = (PersistableWithScalarPrimitives) persistableArray2
            .toJava();
        assertTrue(returnedPersistableArray.isBooleanScalar());
        assertEquals(92.7F, returnedPersistableArray.getFloatScalar(), 1e-7);
        assertEquals(37, returnedPersistableArray.getIntScalar());
        assertEquals("Allons-y", returnedPersistableArray.getStringScalar());
    }

    /**
     * Test that an object with arrays of primitives can be written to HDF5 and reconstituted in
     * Java.
     */
    @Test
    public void testObjectWithPrimitiveArrays() {

        PersistableWithPrimitiveArrays p = new PersistableWithPrimitiveArrays();
        int[] intArray = { 50, 49 };
        String[] stringArray = { "Allons-y", "Geronimo" };
        float[] floatArray = { 92.7F, 105.3F };
        boolean[] booleanArray = { true, false };
        p.setBooleanArray(booleanArray);
        p.setFloatArray(floatArray);
        p.setIntArray(intArray);
        p.setStringArray(stringArray);

        // Write the object
        String fieldName = "persistableObject";
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        PersistableHdf5Array persistableArray = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(p);
        persistableArray.write(fieldGroupId, fieldName);
        H5.H5Gclose(fieldGroupId);

        // retrieve the array and check its contents
        PersistableHdf5Array persistableArray2 = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(new PersistableWithPrimitiveArrays());
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableArray2.read(fieldGroupId);
        PersistableWithPrimitiveArrays returnedPersistableArray = (PersistableWithPrimitiveArrays) persistableArray2
            .toJava();
        assertTrue(Arrays.equals(booleanArray, returnedPersistableArray.getBooleanArray()));
        assertTrue(Arrays.equals(stringArray, returnedPersistableArray.getStringArray()));
        assertTrue(Arrays.equals(intArray, returnedPersistableArray.getIntArray()));
        assertTrue(Arrays.equals(floatArray, returnedPersistableArray.getFloatArray()));
    }

    @Test
    public void testObjectWithParallelizableArray() {

        PersistableWithParallelizableArray p = new PersistableWithParallelizableArray();
        PersistableWithScalarPrimitives[][] s = new PersistableWithScalarPrimitives[3][4];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 4; j++) {
                s[i][j] = new PersistableWithScalarPrimitives();
            }
        }
        s[1][2].setBooleanScalar(true);
        s[0][0].setFloatScalar(92.7F);
        s[2][0].setIntScalar(37);
        s[2][3].setStringScalar("Allons-y");
        p.setStructArray(s);

        // Write the object
        String fieldName = "persistableObject";
        long fieldGroupId = H5.H5Gcreate(fileId, fieldName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        PersistableHdf5Array persistableArray = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(p);
        persistableArray.write(fieldGroupId, fieldName);
        H5.H5Gclose(fieldGroupId);
        // retrieve the array and check its contents
        PersistableHdf5Array persistableArray2 = (PersistableHdf5Array) AbstractHdf5Array
            .newInstance(new PersistableWithParallelizableArray());
        fieldGroupId = H5.H5Gopen(fileId, fieldName, H5P_DEFAULT);
        persistableArray2.read(fieldGroupId);
        PersistableWithParallelizableArray returnedPersistableArray = (PersistableWithParallelizableArray) persistableArray2
            .toJava();
        PersistableWithScalarPrimitives[][] returnedArray = returnedPersistableArray
            .getStructArray();
        assertEquals(3, returnedArray.length);
        for (PersistableWithScalarPrimitives[] z : returnedArray) {
            assertEquals(4, z.length);
        }
        assertTrue(returnedArray[1][2].isBooleanScalar());
        assertEquals(92.7F, returnedArray[0][0].getFloatScalar(), 1e-7);
        assertEquals(37, s[2][0].getIntScalar());
        assertEquals("Allons-y", s[2][3].getStringScalar());
    }

    public PersistableSample2 generatePersistableTest2Object() {
        PersistableSample2 persistableTest2 = new PersistableSample2();
        persistableTest2.intScalar = 500;
        persistableTest2.persistableArray2 = new PersistableSample1[3][2];
        for (int i = 0; i < persistableTest2.persistableArray2.length; i++) {
            persistableTest2.persistableArray2[i][0] = PersistableSample1.newInstance(5, 6, 7, 8, 9,
                10, 11);
            persistableTest2.persistableArray2[i][1] = PersistableSample1.newInstance(12, 13, 14,
                15, 16, 17, 18);
        }
        persistableTest2.persistableList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            persistableTest2.persistableList
                .add(PersistableSample1.newInstance(1, 2, 3, 4, 5, 6, 7));
        }
        persistableTest2.persistableScalar1 = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
        return persistableTest2;
    }

    /**
     * Class with all fields scalar primitives.
     *
     * @author PT
     */
    static class PersistableWithScalarPrimitives implements Persistable {

        private int intScalar;
        private String stringScalar;
        private float floatScalar;
        private boolean booleanScalar;

        public int getIntScalar() {
            return intScalar;
        }

        public void setIntScalar(int intScalar) {
            this.intScalar = intScalar;
        }

        public String getStringScalar() {
            return stringScalar;
        }

        public void setStringScalar(String stringScalar) {
            this.stringScalar = stringScalar;
        }

        public float getFloatScalar() {
            return floatScalar;
        }

        public void setFloatScalar(float floatScalar) {
            this.floatScalar = floatScalar;
        }

        public boolean isBooleanScalar() {
            return booleanScalar;
        }

        public void setBooleanScalar(boolean booleanScalar) {
            this.booleanScalar = booleanScalar;
        }
    }

    /**
     * Class with all fields arrays of primitives.
     *
     * @author PT
     */
    static class PersistableWithPrimitiveArrays implements Persistable {

        private int[] intArray;
        private String[] stringArray;
        private float[] floatArray;
        private boolean[] booleanArray;

        public int[] getIntArray() {
            return intArray;
        }

        public void setIntArray(int[] intArray) {
            this.intArray = intArray;
        }

        public String[] getStringArray() {
            return stringArray;
        }

        public void setStringArray(String[] stringArray) {
            this.stringArray = stringArray;
        }

        public float[] getFloatArray() {
            return floatArray;
        }

        public void setFloatArray(float[] floatArray) {
            this.floatArray = floatArray;
        }

        public boolean[] getBooleanArray() {
            return booleanArray;
        }

        public void setBooleanArray(boolean[] booleanArray) {
            this.booleanArray = booleanArray;
        }
    }

    /**
     * Class with an array of objects that can be stored as parallel arrays of primitives.
     *
     * @author PT
     */
    static class PersistableWithParallelizableArray implements Persistable {

        private PersistableWithScalarPrimitives[][] structArray;

        public PersistableWithScalarPrimitives[][] getStructArray() {
            return structArray;
        }

        public void setStructArray(PersistableWithScalarPrimitives[][] structArray) {
            this.structArray = structArray;
        }
    }
}
