package gov.nasa.ziggy.collections;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BOOLEAN;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_DOUBLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_ENUM;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static gov.nasa.ziggy.collections.ZiggyDataType.box1dArray;
import static gov.nasa.ziggy.collections.ZiggyDataType.castBoxedNumericToUnboxedNumeric;
import static gov.nasa.ziggy.collections.ZiggyDataType.fill1dArray;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataTypeFromClass;
import static gov.nasa.ziggy.collections.ZiggyDataType.truncateClassName;
import static gov.nasa.ziggy.collections.ZiggyDataType.unbox1dArray;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.primitives.Longs;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Utility methods for single- and multi-dimensional arrays, with a few utilities for primitive
 * scalars thrown in. For the time being these methods are focused on the numeric classes (byte,
 * short, int, long, float, double) plus boolean and String. Support for arrays of objects may
 * happen if a need presents itself.
 *
 * @author PT
 */
public class ZiggyArrayUtils {

    private ZiggyArrayUtils() {
    }

    /**
     * Shortcut method to cast a 1-d long array to 1-d int
     *
     * @param long1d 1-d array of longs
     * @return long1d, converted to 1-d array of ints
     */
    public static int[] longToInt1d(long[] long1d) {
        return (int[]) castArray(long1d, ZIGGY_INT);
    }

    /**
     * Shortcut method to cast a 1-d int array to 1-d long
     *
     * @param int1d 1-d array of ints
     * @return int1d, converted to 1-d array of longs
     */
    public static long[] intToLong1d(int[] int1d) {
        return (long[]) castArray(int1d, ZIGGY_LONG);
    }

    /**
     * Detects fields of boxed primitive values.
     *
     * @param field field of a class that might be for boxed primitive values. This field can be
     * scalar or list as well as array.
     * @return
     */
    public static boolean isBoxedPrimitive(Field field) {
        String className = null;
        if (field.getType().isArray()) {
            return isBoxedPrimitive(field.getType());
        }
        if (field.getType().getName().equals("java.util.List")) {
            Type genericType = field.getGenericType();
            ParameterizedType pt = (ParameterizedType) genericType;
            className = pt.getActualTypeArguments()[0].getTypeName();
        } else {
            className = field.getType().getName();
        }
        if (className.contains("Boolean") || className.contains("Byte")
            || className.contains("Short") || className.contains("Integer")
            || className.contains("Long") || className.contains("Float")
            || className.contains("Double")) {
            return true;
        }
        return false;
    }

    /**
     * Detects arrays of boxed primitive values.
     *
     * @param arrayObject Object to be examined. Must be an array, not a scalar or list.
     * @return
     */
    public static boolean isBoxedPrimitive(Object arrayObject) {
        return isBoxedPrimitive(arrayObject.getClass());
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static boolean isBoxedPrimitive(Class<?> clazz) {
        Class<?> testClass = null;
        String className = truncateClassName(clazz.getName());
        if (className.length() == 1) { // primitive array
            return false;
        }
        try {
            testClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            // This can never occur. The name comes from the Class instance passed
            // as argument to the method.
            throw new AssertionError(e);
        }
        Class<?> numberClass = Number.class;
        return testClass.equals(numberClass) || testClass.equals(Boolean.class)
            || numberClass.isAssignableFrom(testClass);
    }

    /**
     * Determine the size of an array. The number of dimensions of the array are initially unknown.
     *
     * @param dataObject The array that is to be sized.
     * @return long[] with the size of each dimension in the array.
     */
    public static long[] getArraySize(Object dataObject) {
        List<Long> arrayDimensionList = new ArrayList<>();
        Class<? extends Object> currentClass = dataObject.getClass();
        Object loopObject = dataObject;
        boolean isArray = currentClass.isArray();
        if (!isArray) {
            return new long[] { 0L };
        }

        // This needs to be done in this somewhat squirrely fashion because on the last
        // iteration, if loopObject is a 1-d array of primitives, it will still have
        // isArray() == true, but an attempt to cast it from Object to Object[] will fail
        while (isArray) {
            arrayDimensionList.add((long) Array.getLength(loopObject));
            String componentType = loopObject.getClass().getComponentType().getName();
            if (componentType.startsWith("[")) {
                Object[] arrayObject = (Object[]) loopObject;
                loopObject = arrayObject[0];
                isArray = true;
            } else {
                isArray = false;
            }
        }

        return Longs.toArray(arrayDimensionList);
    }

    /**
     * Unbox a multi-dimensinonal array of boxed values.
     *
     * @param boxedObject Array of boxed values. Array must be a hyper-rectangle (i.e., not ragged),
     * but otherwise any size or shape is acceptable.
     * @return Array containing the values from boxedObject, converted to their primitive
     * equivalents. Size and shape will match those of boxedObject.
     */
    public static Object unbox(Object boxedObject) {
        long[] dimensions = getArraySize(boxedObject);
        Object destinationArray = constructPrimitiveArray(dimensions.length, (int) dimensions[0],
            getDataType(boxedObject));
        Object[] destinationArray1 = dimensions.length > 1 ? (Object[]) destinationArray : null;
        ArrayRecursionLevel nextLevel = (obj, i) -> destinationArray1[i] = unbox(obj);
        ArrayRecursionLevel lowestLevel = (arrayObject, i) -> {
            Object unboxedArray = unbox1dArray(arrayObject);
            System.arraycopy(unboxedArray, 0, destinationArray, 0, (int) dimensions[0]);
        };
        arrayLevelRecursionMaster(boxedObject, nextLevel, lowestLevel);
        return destinationArray;
    }

    /**
     * Convert a multi-dimensional array of primitive values to the equivalent array of boxed values
     *
     * @param unboxedObject Array of primitive values. Array must be a hyper-rectangle (i.e., not
     * ragged), but otherwise any size or shape will be accepted.
     * @return contents of unboxedObject in an array of boxed values. The boxed type will be the
     * appropriate type for the type of primitive in unboxedObject. The size and shape of the
     * returned array will match those of unboxedObject.
     */
    public static Object box(Object unboxedObject) {
        long[] dimensions = getArraySize(unboxedObject);
        Object destinationArray = constructBoxedArray(dimensions.length, (int) dimensions[0],
            getDataType(unboxedObject));
        Object[] destinationArray1 = dimensions.length > 1 ? (Object[]) destinationArray : null;
        ArrayRecursionLevel nextLevel = (obj, i) -> destinationArray1[i] = box(obj);
        ArrayRecursionLevel lowestLevel = (arrayObject, i) -> {
            Object boxedArray = box1dArray(arrayObject);
            System.arraycopy(boxedArray, 0, destinationArray, 0, (int) dimensions[0]);
        };
        arrayLevelRecursionMaster(unboxedObject, nextLevel, lowestLevel);
        return destinationArray;
    }

    /**
     * Cast a multi-dimensional array of primitives to a different type. Permitted casts are: from
     * any numeric class to any other numeric class; numeric to boolean or boolean to numeric;
     * String to Enum or Enum to String.
     *
     * @param sourceArray
     * @param castType type desired for cast. This can be a ZiggyDataType for the typical primitive
     * types plus string, or it can be the Class object of an Enum class (i.e., MyEnum.Class) for
     * casts to Enum.
     * @return cast of SourceArray to desired type
     */
    public static Object castArray(Object sourceArray, Object castType) {
        ZiggyDataType hType = castDataType(castType);
        long[] arraySize = getArraySize(sourceArray);
        Object destinationArray = constructPrimitiveArray(arraySize.length, (int) arraySize[0],
            hType);
        Object[] destArray1 = arraySize.length > 1 ? (Object[]) destinationArray : null;
        ArrayRecursionLevel nextLevel = (obj, i) -> destArray1[i] = castArray(obj, castType);
        ArrayRecursionLevel lowestLevel = new ArrayRecursionLevel() {
            // TODO Try to eliminate this SuppressWarnings annotation
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public void apply(Object sourceArray1, int i) {
                ZiggyDataType hType = castDataType(castType);
                Object destinationArray1 = null;
                ZiggyDataType sourceDataType = getDataType(sourceArray1);

                // there are a few options here:
                if (sourceDataType.isNumeric() && hType.equals(ZIGGY_BOOLEAN)) {
                    destinationArray1 = castNumericToBoolean(sourceArray1);
                }
                if (sourceDataType.equals(ZIGGY_BOOLEAN) && hType.isNumeric()) {
                    destinationArray1 = castBooleanToNumeric(sourceArray1, hType);
                }
                if (sourceDataType.equals(ZIGGY_ENUM) && hType.equals(ZIGGY_STRING)) {
                    destinationArray1 = castEnumToString(sourceArray1);
                }
                if (sourceDataType.equals(ZIGGY_STRING) && hType.equals(ZIGGY_ENUM)) {
                    destinationArray1 = castStringToEnum(sourceArray1, (Class) castType);
                }
                if (sourceDataType.isNumeric() && hType.isNumeric()) {
                    destinationArray1 = castNumericToNumeric(sourceArray1, hType);
                }
                if (destinationArray1 == null) {
                    throw new PipelineException("Unable to cast array from "
                        + sourceDataType.getJavaClassName() + " to " + hType.getJavaClassName());
                }
                System.arraycopy(destinationArray1, 0, destinationArray, 0, (int) arraySize[0]);
            }
        };
        arrayLevelRecursionMaster(sourceArray, nextLevel, lowestLevel);
        return destinationArray;
    }

    /**
     * Utility method that converts an Enum.class object to an Enum DataType object, but otherwise
     * assumes that its argument is a DataType and returns it.
     */
    static ZiggyDataType castDataType(Object castType) {
        if (isEnumClass(castType)) {
            return ZiggyDataType.ZIGGY_ENUM;
        }
        return (ZiggyDataType) castType;
    }

    /**
     * Cast a numeric 1-d array to boolean.
     *
     * @param sourceArray1d 1-d array of boolean
     * @return contents of sourceArray1, cast to boolean, where zero values are mapped to false and
     * nonzero to true.
     */
    static Object castNumericToBoolean(Object sourceArray1d) {

        // perform the ultimate in widening conversions
        Object doubleObject = castNumericToNumeric(sourceArray1d, ZIGGY_DOUBLE);
        double[] doubleArray = (double[]) doubleObject;
        boolean[] booleanArray = new boolean[doubleArray.length];
        for (int i = 0; i < doubleArray.length; i++) {
            booleanArray[i] = doubleArray[i] != 0;
        }
        return booleanArray;
    }

    /**
     * Cast a 1-d array of boolean values to a numeric class.
     *
     * @param sourceArray1 1-d array of boolean values
     * @param ziggyDataType ZiggyDataType of desired cast outcome
     * @return contents of sourceArray1d, cast to the type indicated by hType, where false values
     * are mapped to 0 and true values are mapped to 1.
     */
    static Object castBooleanToNumeric(Object sourceArray1d, ZiggyDataType ziggyDataType) {
        boolean[] booleanArray = (boolean[]) sourceArray1d;
        byte[] byteArray = new byte[booleanArray.length];
        for (int i = 0; i < booleanArray.length; i++) {
            byteArray[i] = booleanArray[i] ? (byte) 1 : (byte) 0;
        }
        return castNumericToNumeric(byteArray, ziggyDataType);
    }

    /**
     * Cast a 1-d array of Enums to String.
     *
     * @param sourceArrayCurrentLevel 1-d array of Enums
     * @return contents of sourceArrayCurrentLevel, cast to Strings
     */
    @SuppressWarnings("unchecked")
    static <E> Object castEnumToString(Object sourceArrayCurrentLevel) {
        E[] sourceArray = (E[]) sourceArrayCurrentLevel;
        String[] destinationArray = new String[sourceArray.length];
        for (int i = 0; i < sourceArray.length; i++) {
            destinationArray[i] = ((Enum<?>) sourceArray[i]).name();
        }
        return destinationArray;
    }

    /**
     * Cast a 1-d array of strings to a specified Enum class.
     *
     * @param sourceArrayCurrentLevel 1-d array of strings
     * @param enumClass Class of desired Enum storage type
     * @return contents of sourceArrayCurrentLevel, cast to Enum type indicated by enumClass
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <E extends Enum<E>> Object castStringToEnum(Object sourceArrayCurrentLevel,
        Class<? extends Enum<E>> enumClass) {
        String[] stringArray = (String[]) sourceArrayCurrentLevel;
        Class<? extends Enum> E = enumClass;
        Enum[] genericEnumArray = new Enum[stringArray.length];
        E[] enumArray = (E[]) genericEnumArray;
        for (int i = 0; i < stringArray.length; i++) {
            enumArray[i] = (E) Enum.valueOf(E, stringArray[i]);
        }
        return enumArray;
    }

    /**
     * Cast a numeric 1-d array to a numeric array of a different data type.
     *
     * @param sourceArray1d 1-d numeric array
     * @param ziggyDataType data type of desired cast
     * @return contents of sourceArray1d, cast to type indicated by ziggyDataType
     */
    static Object castNumericToNumeric(Object sourceArray1d, ZiggyDataType ziggyDataType) {
        if (ziggyDataType.equals(getDataType(sourceArray1d))) {
            return sourceArray1d;
        }
        Number[] boxedSourceArray = (Number[]) box(sourceArray1d);
        Object destinationArray = Array.newInstance(ziggyDataType.getJavaClass(),
            boxedSourceArray.length);
        castBoxedNumericToUnboxedNumeric(boxedSourceArray, destinationArray);
        return destinationArray;
    }

    /**
     * Construct a complete multi-dimensional array of a specified primitive type.
     *
     * @param dimensions array with the dimensions of the desired array.
     * @param ziggyDataType data type for the desired array type.
     * @return Constructed, fully-populated array. For example, a call with dimensions == {3, 4, 5}
     * and ZiggyDataType == ZIGGY_FLOAT will return an array of float[3][4][5].
     */
    public static Object constructFullPrimitiveArray(long[] dimensions,
        ZiggyDataType ziggyDataType) {
        return constructFullArray(dimensions, ziggyDataType, false);
    }

    /**
     * Construct a complete multi-dimensional array of a specified boxed type.
     *
     * @param dimensions array with the dimensions of the desired array.
     * @param ziggyDataType data type for the desired array type.
     * @return Constructed, fully-populated array. For example, a call with dimensions == {3, 4, 5}
     * and ZiggyDataType == ZIGGY_FLOAT will return an array of Float[3][4][5].
     */
    public static Object constructFullBoxedArray(long[] dimensions, ZiggyDataType ziggyDataType) {
        return constructFullArray(dimensions, ziggyDataType, true);
    }

    /**
     * Master builder for multi-dimensional full arrays
     *
     * @param dimensions dimensions of the array
     * @param ziggyDataType data type for the desired storage class.
     * @param box if true return array of boxed values, otherwise primitives.
     * @return Array of the desired dimensions and storage class.
     */
    public static Object constructFullArray(long[] dimensions, ZiggyDataType ziggyDataType,
        boolean box) {

        // build the outermost array (so for example float[3][][])
        Object returnObject = constructArray(dimensions.length, (int) dimensions[0], ziggyDataType,
            box);

        // If the array is not 1-d, populate the next level down (i.e.,
        // make each of the array[i] equal to float[4][]). This uses a
        // recursive call to this method.
        if (dimensions.length > 1) {
            Object[] returnArray = (Object[]) returnObject;
            for (int i = 0; i < returnArray.length; i++) {
                returnArray[i] = constructFullArray(
                    Arrays.copyOfRange(dimensions, 1, dimensions.length), ziggyDataType, box);
            }
        }
        return returnObject;
    }

    /**
     * Construct an empty array for a primitive type at some level of dimensionality.
     *
     * @param dimensionCount Number of dimensions (i.e., number of [] at the end)
     * @param length array length
     * @param ziggyDataType data type to be used for the array
     * @return An array with the dimensionality and primitive type requested, i.e., dimensionCount
     * == 3, length = 4, data type == ZIGGY_DOUBLE, will return an array of double[4][][]. Note that
     * the lower-level arrays are null. To achieve an array that is fully populated, use
     * constructFullPrimitiveArray.
     */
    public static Object constructPrimitiveArray(int dimensionCount, int length,
        ZiggyDataType ziggyDataType) {
        return constructArray(dimensionCount, length, ziggyDataType, false);
    }

    /**
     * Construct an empty array for a boxed type at some level of dimensionality.
     *
     * @param dimensionCount Number of dimensions (i.e., number of [] at the end)
     * @param length array length
     * @param ziggyDataType data type to be used for the array
     * @return An array with the dimensionality and primitive type requested, i.e., dimensionCount
     * == 3, length = 4, data type == ZIGGY_DOUBLE, will return an array of Double[4][][]. Note that
     * the lower-level arrays are null. To achieve an array that is fully populated, use
     * constructFullBoxedArray.
     */
    public static Object constructBoxedArray(int dimensionCount, int length,
        ZiggyDataType ziggyDataType) {
        return constructArray(dimensionCount, length, ziggyDataType, true);
    }

    /**
     * Master array constructor for numeric, string, and enum types.
     *
     * @param dimensionCount Number of dimensions for the array.
     * @param length Length of the highest level of the array (i.e., number of "rows")
     * @param ziggyDataType DataType for the array
     * @param box if true, return an array of boxed types, otherwise primitives
     * @return Array of the desired type and size.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    static Object constructArray(int dimensionCount, int length, ZiggyDataType ziggyDataType,
        boolean box) {
        checkArgument(length >= 0, "Negative array length");
        String arrayClass = arrayClassToConstruct(dimensionCount, ziggyDataType, box);
        try {

            if (arrayClass.length() > 1) {

                // in this case, we have the correct name for the class
                return Array.newInstance(Class.forName(arrayClass), length);
            }
            // in this case we want a 1-d array of primitives, so we
            // cannot use the class name...
            return Array.newInstance(ziggyDataType.getJavaClass(), length);
        } catch (NegativeArraySizeException | ClassNotFoundException e) {
            // This can never occur. The array size is checked as an argument, and
            // the class comes from an instance of ZiggyDataType.
            throw new AssertionError(e);
        }
    }

    /**
     * Determines the name of the array class for construction.
     *
     * @param dimensionCount Number of dimensions desired.
     * @param ziggyDataType data type for the desired array storage class.
     * @param box if true return array of boxed values, otherwise primitives.
     * @return String containing the name of the array class to be constructed.
     */
    static String arrayClassToConstruct(int dimensionCount, ZiggyDataType ziggyDataType,
        boolean box) {
        StringBuilder arrayStringBuilder = new StringBuilder();
        for (int i = 1; i < dimensionCount; i++) {
            arrayStringBuilder.append("[");
        }
        switch (ziggyDataType) {
            case ZIGGY_STRING:
                arrayStringBuilder.append("java.lang.String");
                if (dimensionCount > 1) {
                    arrayStringBuilder.insert(dimensionCount - 1, "L");
                    arrayStringBuilder.append(";");
                }
                break;
            case ZIGGY_ENUM:
                arrayStringBuilder.append("java.lang.Enum");
                if (dimensionCount > 1) {
                    arrayStringBuilder.insert(dimensionCount - 1, "L");
                    arrayStringBuilder.append(";");
                }
                break;
            default:
                if (box) {
                    arrayStringBuilder.append(ziggyDataType.getJavaBoxedClassName());
                    if (dimensionCount > 1) {
                        arrayStringBuilder.insert(dimensionCount - 1, "L");
                        arrayStringBuilder.append(";");
                    }
                } else {
                    arrayStringBuilder.append(ziggyDataType.getJavaTypeCharacter());
                }
        }
        return arrayStringBuilder.toString();
    }

    @SuppressWarnings("rawtypes")
    static boolean isEnumClass(Object obj) {
        if (obj instanceof Class) {
            return getDataTypeFromClass((Class) obj).equals(ZIGGY_ENUM);
        }
        return false;
    }

    /**
     * Constructs a multi-dimensional boolean array and populates it with trues
     *
     * @param size desired size of the array
     * @return boolean array of desired size, all values set to true
     */
    public static Object arrayOfTrue(long[] size) {
        Object array = constructFullPrimitiveArray(size, ZIGGY_BOOLEAN);
        populateWithTrue(array);
        return array;
    }

    /**
     * Constructs a multi-dimensional boolean array and populates it with trues
     *
     * @param size desired size of the array
     * @return boolean array of desired size, all values set to true
     */
    public static Object arrayOfTrue(int[] size) {
        return arrayOfTrue(intToLong1d(size));
    }

    /**
     * Recursively populates a multi-dimensional boolean array with true values.
     *
     * @param array Array to be populated.
     */
    static void populateWithTrue(Object array) {
        ArrayRecursionLevel nextLevel = (obj, i) -> populateWithTrue(obj);
        ArrayRecursionLevel lowestLevel = (array1, c) -> {
            boolean[] bools = (boolean[]) array1;
            Arrays.fill(bools, true);
        };
        arrayLevelRecursionMaster(array, nextLevel, lowestLevel);
    }

    /**
     * Fill a multi-dimensional array with a numeric fill value. Note that the array must be a
     * primitive array of numeric type (byte, short, int, long, float, double).
     *
     * @param array Array to be filled
     * @param fillValue numeric value to be used in the fill
     */
    public static void fill(Object array, Number fillValue) {
        fillRecursively(array, fillValue);
    }

    /**
     * Fill a multi-dimensional array with a Java Object fill value. Note that the array must be
     * either a prmitive array of numeric type, a String array, or a boolean array.
     *
     * @param array Array to be filled
     * @param fillValue Object value to be used in the fill. If the array is numeric, fillValue must
     * be a Number; if boolean, it must be a Boolean; if the array is Strings, fillValue must
     * implement the toString() method appropriately.
     */
    public static void fill(Object array, Object fillValue) {
        fillRecursively(array, fillValue);
    }

    /**
     * Recursive array filler.
     *
     * @param array Array to be filled.
     * @param fillValue Object that supplies the fill value.
     */
    static void fillRecursively(Object array, Object fillValue) {
        ArrayRecursionLevel nextLevel = (array1, i) -> fillRecursively(array1, fillValue);
        ArrayRecursionLevel lowestLevel = (array1, i) -> {
            fill1dArray(array1, fillValue);
        };
        arrayLevelRecursionMaster(array, nextLevel, lowestLevel);
    }

    /**
     * Determines whether an array is "ragged," that is to say, whether the array is a proper
     * hyper-rectangle (not ragged) or has sub-arrays that differ in size (ragged).
     *
     * @param array Array to be analyzed
     * @return false if array is a hyper-rectangle, true if it is ragged.
     */
    public static boolean isRaggedArray(Object array) {

        // the ragged return value needs to be both accessed and mutated by
        // the nextLevel object, but it also has to be effectively final; the
        // only way I've found to do this is to make it a boolean[1] array
        boolean[] ragged = { false };
        long[] fullArraySize = getArraySize(array);
        int nDims = fullArraySize.length;
        long[] subarraySize = ArrayUtils.subarray(fullArraySize, 1, nDims);
        ArrayRecursionLevel nextLevel = (subArray, i) -> {
            ragged[0] = ragged[0] || !Arrays.equals(subarraySize, getArraySize(subArray));
            ragged[0] = ragged[0] || isRaggedArray(subArray);
        };
        ArrayRecursionLevel lowestLevel = (lowestArray, i) -> {
        };
        arrayLevelRecursionMaster(array, nextLevel, lowestLevel);
        return ragged[0];
    }

    /**
     * Gets a selected value out of a multi-dimensional array. The array can be any boxed or unboxed
     * numeric type, boxed or unboxed boolean, or String.
     *
     * @param array
     * @param location int array that holds the desired location: for example, to retrieve
     * array[3][2][4], location should be {3, 2, 4}.
     * @return The value from the selected location, wrapped if the array is of primitive values.
     */
    public static Object getValue(Object array, int[] location) {
        Object obj = null;
        if (location.length > 1) {
            Object[] objArray = (Object[]) array;
            obj = getValue(objArray[location[0]],
                ArrayUtils.subarray(location, 1, location.length));
        } else {
            obj = Array.get(array, location[0]);
        }
        return obj;
    }

    public static void setValue(Object array, int[] location, Object value) {
        long[] arraySize = getArraySize(array);
        if (arraySize.length > 1) {
            Object[] objArray = (Object[]) array;
            setValue(objArray[location[0]], ArrayUtils.subarray(location, 1, location.length),
                value);
        } else {
            Array.set(array, location[0], value);
        }
    }

    /**
     * Recurses through array levels. If the recursion has not yet reached the lowest level, execute
     * a next-level method. At the lowest level, execute a lowest-level method. The next-level and
     * lowest-level methods are supplied via the ArrayRecursionLevel interface, so methods can call
     * arrayLevelRecursionMaster() with a pair of anonymous instances of ArrayRecursionLevel.
     *
     * @param array array to be operated on, as an Object
     * @param nextLevel ArrayRecursionLevel instance to be used for next-lowest level processing;
     * usually this is a call to the method that calls arrayLevelRecursionMaster(), but additional
     * processing may also be needed
     * @param lowestLevel ArrayRecursionLevel instance to be used for lowest-level processing
     */
    static void arrayLevelRecursionMaster(Object array, ArrayRecursionLevel nextLevel,
        ArrayRecursionLevel lowestLevel) {
        long[] arraySize = getArraySize(array);
        if (arraySize.length > 1) {
            Object[] objectArray = (Object[]) array;
            for (int i = 0; i < objectArray.length; i++) {
                nextLevel.apply(objectArray[i], i);
            }
        } else {
            lowestLevel.apply(array, 0);
        }
    }

    /**
     * Interface that supports recursion through array levels. The interface requires a single
     * method, go(), that takes as argument a Java Object and an int. This permits anonymous classes
     * based on ArrayRecursionLevel to perform operations on Java arrays or array segments that are
     * passed as Objects.
     *
     * @author PT
     */
    interface ArrayRecursionLevel {
        void apply(Object obj, int counter);
    }

    /**
     * This can copy any kind of array even primitive arrays, hence this is not parameterized.
     *
     * @param src The source array.
     * @param componentType The element type of the destination array. This may be different from
     * the src component type if the element of the src array are assignable to the componentType.
     * @return An array object which has the specified component type.
     */
    public static Object copyArray(Object src, Class<?> componentType) {
        // Class<?> originalArrayComponentType =
        // originalArray.getClass().getComponentType();
        int originalArrayLength = Array.getLength(src);

        Object newArray = Array.newInstance(componentType, originalArrayLength);

        for (int i = 0; i < originalArrayLength; i++) {
            Array.set(newArray, i, Array.get(src, i));
        }

        return newArray;
    }

    /**
     * Converts a 1-d array to a comma-separated string of values.
     */
    public static String arrayToString(Object arrayObject) {
        ZiggyDataType dataType = ZiggyDataType.getDataType(arrayObject);
        long[] arraySize = ZiggyArrayUtils.getArraySize(arrayObject);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (int) arraySize[0]; i++) {
            sb.append(dataType.array1dMemberToString(arrayObject, i));
            if (i < arraySize[0] - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    public static Object stringToArray(String commaSeparatedValues, ZiggyDataType dataType) {

        if (StringUtils.isBlank(commaSeparatedValues)) {
            return constructFullPrimitiveArray(new long[] { 0 }, dataType);
        }

        // Convert the string into an array of strings by breaking at commas.
        String[] stringValues = commaSeparatedValues.split(",");

        // Construct the array.
        Object array = constructFullPrimitiveArray(new long[] { stringValues.length }, dataType);

        // Populate the array.
        for (int arrayIndex = 0; arrayIndex < stringValues.length; arrayIndex++) {
            setValue(array, new int[] { arrayIndex },
                dataType.typedValue(stringValues[arrayIndex].trim()));
        }
        return array;
    }
}
