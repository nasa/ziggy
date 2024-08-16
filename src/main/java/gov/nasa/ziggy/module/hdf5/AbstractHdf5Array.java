package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_PERSISTABLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.get1dArrayMember;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static gov.nasa.ziggy.collections.ZiggyDataType.set1dArrayMember;
import static gov.nasa.ziggy.collections.ZiggyDataType.truncateClassName;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.ModuleParameters;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

/**
 * Abstract superclass for the PrimitiveHdf5Array and the PersistableHdf5Array. These classes
 * provide the impedance match between the Java-side representation of module interfaces (in the
 * form of Persistable classes) and the HDF5 storage of the interface data.
 *
 * @author PT
 */
public abstract class AbstractHdf5Array {

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    enum ReturnAs {
        UNKNOWN, SCALAR, ARRAY, LIST;
    }

    protected ZiggyDataType hdf5DataType = null;
    protected long[] dimensions = null;
    protected Object arrayObject = null;
    protected Class<? extends Object> auxiliaryClass = null;
    protected boolean scalar;
    protected ReturnAs returnAs = ReturnAs.UNKNOWN;
    protected String fieldName = null;
    protected boolean allowMissingFields = false;
    protected boolean missingFieldsDetected = false;
    protected boolean createGroupsForMissingFields = false;

    /**
     * Factory method that returns a correct, instantiated object for the object provided as an
     * argument.
     *
     * @param object Any sort of data that is valid for persistance in HDF5 format
     * @return An instance of one of the concrete classes that extend AbstractHdf5Array, populated
     * correctly for the argument object, or null if the object is itself null or is an empty list.
     */
    public static AbstractHdf5Array newInstance(Object object) {
        if (AbstractHdf5Array.isEmpty(object)) {
            return null;
        }
        if (object instanceof Field) {
            return AbstractHdf5Array.getFieldInstance((Field) object);
        }
        return AbstractHdf5Array.getObjectInstance(object);
    }

    /**
     * Factory method that returns a correct Hdf5Array object provided with the field that will
     * eventually be used to store the data in the array.
     *
     * @param field Field object from the data object that will store the contents of the array.
     * This is used to determine properties of the data that are needed for output.
     * @return an instance of one of the concrete classes that extend AbstractHdf5Array, populated
     * correctly for the argument field.
     */
    static AbstractHdf5Array getFieldInstance(Field field) {
        AbstractHdf5Array returnObject = null;
        ZiggyDataType hdf5Type = getDataType(field);

        if (field.getType().equals(ModuleParameters.class)) {
            returnObject = new ModuleParametersHdf5Array(field);
        } else if (hdf5Type.equals(ZIGGY_PERSISTABLE)) {
            returnObject = new PersistableHdf5Array(field);
        } else {
            returnObject = new PrimitiveHdf5Array(field);
        }
        return returnObject;
    }

    /**
     * Return an HDF5 array for a non-Field object
     *
     * @param object non-Field object which can be scalar, array, or list, but has data contents
     * that are either primitives, strings, enums, or objects that implement Persistable
     * @return appropriate AbstractHdf5Array object, instantiated with the contents of the argument
     * object.
     */
    static AbstractHdf5Array getObjectInstance(Object object) {

        ZiggyDataType hdf5Type = getDataType(object);
        AbstractHdf5Array returnObject = null;

        // special handling for ModuleParameters scalar instances
        if (object.getClass().equals(ModuleParameters.class)) {
            returnObject = new ModuleParametersHdf5Array(object);
        } else if (hdf5Type.equals(ZIGGY_PERSISTABLE)) {
            returnObject = new PersistableHdf5Array(object);
        } else {
            returnObject = new PrimitiveHdf5Array(object);
        }
        return returnObject;
    }

    /**
     * Determines whether an object is empty.
     *
     * @param object Object to be analyzed.
     * @return true if the object is null or is an empty list, false otherwise.
     */
    public static boolean isEmpty(Object object) {
        boolean isEmpty = false;
        if (object == null || object.getClass() == null) {
            isEmpty = true;
        } else if (List.class.isAssignableFrom(object.getClass())) {
            List<?> objList = (List<?>) object;
            if (objList.isEmpty()) {
                isEmpty = true;
            }
        }
        return isEmpty;
    }

    private static boolean isEmptyList(Object object) {
        if (List.class.isAssignableFrom(object.getClass())) {
            List<?> objList = (List<?>) object;
            if (objList.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isEmptyHdf5Array(AbstractHdf5Array array) {

        // Array is null or its contents are null.
        // Array contents are an empty list.
        if (array == null || array.getArrayObject() == null
            || isEmptyList(array.getArrayObject())) {
            return true;
        }

        // Array contents are an empty Java array.
        if (array.getArrayObject().getClass().isArray()) {
            long[] dims = array.getDimensions();
            for (long dim : dims) {
                if (dim == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Populates an Hdf5Array with an array object. The resulting arrayObject member of the class is
     * guaranteed to be an array of an unboxed primitive type, String, Enum, or a Persistable class.
     * Lists, scalars, and boxed primitives are converted to this format. The setArray method is
     * responsible for ensuring that the full Hdf5Array object is self-consistent. Once the
     * arrayObject has been set, the contents of the Hdf5Array object cannot be changed.
     *
     * @param arrayObject Data object to be used to populate the array.
     */
    public abstract void setArray(Object arrayObject);

    /**
     * Returns the contents of the object, suitably formatted for storage in a Java object.
     *
     * @return Contents of the object. The returned object is formatted in whatever way is needed
     * for storage (transformed to scalar or list, cast, etc).
     */
    public abstract Object toJava();

    /**
     * Write the contents of the class to HDF5
     *
     * @param fieldGroupId HDF5 ID of the parent group
     * @param fieldName Name of the field
     * @return List of HDF5 IDs of groups written by the method.
     */
    public abstract List<Long> write(long fieldGroupId, String fieldName);

    /**
     * Read the contents of an HDF5 group into an array
     *
     * @param fieldGroupId HDF5 ID of the group to be read
     */
    public abstract void read(long fieldGroupId);

    /**
     * Constructor to be used when constructing with an initial array. This is typically the case
     * when building an Hdf5Array object to write an HDF5 file from a Java-side object. Constructor
     * is package-private so that the class can only be instantiated via the
     * {{@link #newInstance(Object)} method, which provides some additional protection.
     *
     * @param arrayObject data to be handled by the object.
     */
    AbstractHdf5Array(Object arrayObject) {
        setArray(arrayObject);
        dimensions = ZiggyArrayUtils.getArraySize(getArrayObject());
        hdf5DataType = getDataType(arrayObject);
    }

    public abstract ZiggyDataType getDataTypeToSave();

    /**
     * Constructor to be used when constructing with a final output field. This is typically the
     * case when building an Hdf5Array object to read from an HDF5 file and write to a Java-side
     * object. Constructor is package-private so that the class can only be instantiated via the
     * Hdf5ArrayFactory methods, which provide some additional protection.
     *
     * @param field field of the object that will eventually store the data.
     */
    AbstractHdf5Array(Field field) {
        // set the type of return that's going to be used
        Class<?> clazz = field.getType();
        if (clazz.isArray()) {
            returnAs = ReturnAs.ARRAY;
        } else if (List.class.isAssignableFrom(clazz)) {
            returnAs = ReturnAs.LIST;
        } else {
            returnAs = ReturnAs.SCALAR;
        }
        fieldName = field.getName();
    }

    public String getFieldName() {
        return fieldName;
    }

    void setFieldName(String name) {
        fieldName = name;
    }

    boolean isScalar(Object object) {
        Class<?> clazz = object.getClass();
        return !clazz.isArray() && !List.class.isAssignableFrom(clazz);
    }

    /**
     * Get the specific class for an array of objects that either extend Enum or implement
     * Persistable.
     *
     * @param arrayObject Array of objects that extend Enum or implement Persistable.
     * @return The specific class of object that is present in the array.
     */
    Class<?> getClassForEnumOrPersistable(Object arrayObject) {
        return getClassForEnumOrPersistable(arrayObject.getClass());
    }

    /**
     * Get the specific class for fields that will store objects that either extend Enum or
     * implement Persistable. These can be scalars, arrays of such objects, or lists of such
     * objects.
     *
     * @param field Field from a Persistable object that will ultimately contain the data.
     * @return The class of the data type that the field is designed to contain.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    Class<?> getClassForEnumOrPersistable(Field field) {
        Class<?> clazz = field.getType();
        String typeName;
        if (List.class.isAssignableFrom(clazz)) {
            Type genericType = field.getGenericType();
            ParameterizedType pt = (ParameterizedType) genericType;
            typeName = pt.getActualTypeArguments()[0].getTypeName();
            try {
                clazz = Class.forName(typeName);
            } catch (ClassNotFoundException e) {
                // This can never occur. By construction the argument to forName() comes
                // from an instance of Class that is available to the code.
                throw new AssertionError(e);
            }
        }
        return getClassForEnumOrPersistable(clazz);
    }

    /**
     * Get the specific class for an object whose data extends Enum or implements Persistable
     *
     * @param clazz Class of the object or field
     * @return Specific Enum or Persistable class
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    Class<?> getClassForEnumOrPersistable(Class<?> clazz) {
        String className = clazz.getName();
        try {
            return Class.forName(truncateClassName(className));
        } catch (ClassNotFoundException e) {
            // This can never occur. By construction, the name used in the forName() call
            // comes from the getName() of a class that's available.
            throw new AssertionError(e);
        }
    }

    /**
     * Set a new array element into the data array.
     *
     * @param arrayMember Element that is to be inserted.
     * @param location location in the array for the insertion, for example to insert at
     * array[3][4][5], location == {3, 4, 5}.
     */
    void setArrayMember(Object arrayMember, long[] location) {
        Object arrayCurrentLevel = arrayObject;
        setArrayMember(arrayMember, arrayCurrentLevel, location);
    }

    /**
     * Recursively travel through a multi-dimensional array to place a new element
     *
     * @param arrayMember Element that is to be inserted.
     * @param arrayCurrentLevel multi-dimensional array. On each pass through the recursive calls,
     * the array's levels are stepped through. So for example, to place a new element at
     * array[3][4][5], on the first call the full array is passed in; on the next, array[3] is
     * passed; etc.
     * @param location location in the array for the insertion. On each pass through the recursive
     * calls, the leading element is truncated. For example, if the initial call has location == {3,
     * 4, 5}, the second call will have location == {4, 5}, etc.
     */
    static void setArrayMember(Object arrayMember, Object arrayCurrentLevel, long[] location) {
        if (location.length == 1) {
            setArrayMember(arrayMember, arrayCurrentLevel, location[0]);
        } else {
            Object[] objectArray = (Object[]) arrayCurrentLevel;
            long[] newLocation = Arrays.copyOfRange(location, 1, location.length);
            setArrayMember(arrayMember, objectArray[(int) location[0]], newLocation);
        }
    }

    /**
     * Perform the lowest-level array member set operation. This has to be an abstract method
     * because the PrimitiveHdf5Array and the PersistableHdf5Array require wildly different
     * approaches to performing this activity.
     *
     * @param arrayMember The object that is to be set into the array.
     * @param arrayLowestLevel The correct 1-d array for the set, so for example if the set is going
     * into position [3][4][5], then arrayLowestLevel is the [3][4] 1-d array.
     * @param location position in the 1-d array for the set, if the set is going into position
     * [3][4][5], then location == 5.
     */
    static void setArrayMember(Object arrayMember, Object array, long location) {
        if (arrayMember == null) {
            Object[] oArray = (Object[]) array;
            oArray[(int) location] = null;
            return;
        }
        set1dArrayMember(arrayMember, array, (int) location);
    }

    /**
     * Return a member from a multi-dimensional array.
     *
     * @param location The location in the array, for example to return array[3][4][5], location ==
     * {3, 4, 5}.
     * @return The scalar Persistable object at the specified location.
     */
    Object getArrayMember(long[] location) {
        Object arrayCurrentLevel = arrayObject;
        return getArrayMember(arrayCurrentLevel, location);
    }

    /**
     * Writes a string attribute. The string is converted to a byte array to work around what
     * appears to be a bug in the HDF5 Java API handling of variable-length string attributes.
     *
     * @param groupId ID of the group to get the attribute.
     * @param attributeName Desired name of the attribute.
     * @param attributeValue Contents of the attribute.
     */
    protected void writeStringAttribute(long groupId, String attributeName, String attributeValue) {

        // convert the attribute value to a byte string for storage -- this is necessary
        // because the Java API for storing variable-length String arrays appears to be
        // broken
        byte[] attributeValueBytes = attributeValue.getBytes(CHARSET);

        // create the dataspace
        long attributeSpace = H5.H5Screate_simple(1, new long[] { attributeValueBytes.length },
            null);
        long attributeId = H5.H5Acreate(groupId, attributeName, ZIGGY_BYTE.getHdf5Type(),
            attributeSpace, H5P_DEFAULT, H5P_DEFAULT);
        H5.H5Awrite(attributeId, ZIGGY_BYTE.getHdf5Type(), attributeValueBytes);
        H5.H5Aclose(attributeId);
        H5.H5Sclose(attributeSpace);
    }

    /**
     * Reads a string attribute. The string is stored as a byte array to work around what appears to
     * be a but in the HDF5 Java API handling of arrays of variable-length strings.
     */
    protected String readStringAttribute(long groupId, String attributeName) {

        long attributeId = H5.H5Aopen(groupId, attributeName, H5P_DEFAULT);
        long dataSpaceId = H5.H5Aget_space(attributeId);
        int nDims = H5.H5Sget_simple_extent_ndims(dataSpaceId);
        long[] dims = new long[nDims];
        long[] maxDims = new long[nDims];
        H5.H5Sget_simple_extent_dims(dataSpaceId, dims, maxDims);
        byte[] attributeValueBytes = new byte[(int) dims[0]];
        H5.H5Aread(attributeId, ZIGGY_BYTE.getHdf5Type(), attributeValueBytes);
        H5.H5Sclose(dataSpaceId);
        H5.H5Aclose(attributeId);

        return new String(attributeValueBytes, CHARSET);
    }

    /**
     * Writes the field order attribute into a group.
     */
    protected void writeFieldOrderAttribute(long groupId, int fieldOrder) {

        long orderAttributeSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
        long orderAttributeId = H5.H5Acreate(groupId, Hdf5ModuleInterface.FIELD_ORDER_ATT_NAME,
            ZIGGY_INT.getHdf5Type(), orderAttributeSpace, H5P_DEFAULT, H5P_DEFAULT);
        H5.H5Awrite(orderAttributeId, ZIGGY_INT.getHdf5Type(), new int[] { fieldOrder });
        H5.H5Aclose(orderAttributeId);
        H5.H5Sclose(orderAttributeSpace);
    }

    /**
     * Recursively retrieve a member from a multi-dimensional array.
     *
     * @param arrayCurrentLevel multi-dimensional array. On each pass through the recursive calls,
     * the array dimension is reduced, i.e., if the caller wants array[3][4][5], then on the first
     * call the full array is passed; on the second call, array[3] is passed; on the third,
     * array[3][4] is passed.
     * @param location The location of the desired element. On each pass through the recursive
     * calls, the leading entry in this array is removed. For example, if on the first call,
     * location is {3, 4, 5}, on the second call it is {4, 5}, etc.
     * @return The desired array member.
     */
    static Object getArrayMember(Object arrayCurrentLevel, long[] location) {
        if (location.length == 1) {
            return getArrayMember(arrayCurrentLevel, (int) location[0]);
        }
        long[] newLocation = Arrays.copyOfRange(location, 1, location.length);
        Object[] objectArray = (Object[]) arrayCurrentLevel;
        return getArrayMember(objectArray[(int) location[0]], newLocation);
    }

    /**
     * Retrieve an entry from an array based on its location. This is the lowest-level operation at
     * the bottom of the iterative getArrayMember methods, and must be implemented separately in the
     * primitive and Persistable HDF5 array objects
     *
     * @param arrayLowestLevel one-dimensional array
     * @param location index of array location
     * @return value of array element at desired index
     */
    static Object getArrayMember(Object arrayLowestLevel, long location) {
        int loc = (int) location;
        return get1dArrayMember(arrayLowestLevel, loc);
    }

    public ZiggyDataType getHdf5DataType() {
        return hdf5DataType;
    }

    public long[] getDimensions() {
        return dimensions;
    }

    public Object getArrayObject() {
        return arrayObject;
    }

    public void setArrayObject(Object arrayObject) {
        if (this.arrayObject != null) {
            throw new PipelineException("Contents of Hdf5Array cannot be changed");
        }
        setArray(arrayObject);
    }

    public Class<? extends Object> getAuxiliaryClass() {
        return auxiliaryClass;
    }

    public boolean isAllowMissingFields() {
        return allowMissingFields;
    }

    public void setAllowMissingFields(boolean allowMissingFields) {
        this.allowMissingFields = allowMissingFields;
    }

    public boolean isMissingFieldsDetected() {
        return missingFieldsDetected;
    }

    public void setMissingFieldsDetected(boolean missingFieldsDetected) {
        this.missingFieldsDetected = missingFieldsDetected;
    }

    public boolean isCreateGroupsForMissingFields() {
        return createGroupsForMissingFields;
    }

    public void setCreateGroupsForMissingFields(boolean createGroupsForMissingFields) {
        this.createGroupsForMissingFields = createGroupsForMissingFields;
    }

    public boolean isScalar() {
        return scalar;
    }

    public ReturnAs getReturnAs() {
        return returnAs;
    }

    /**
     * Iterates through the locations in a multi-dimensional array. For example, for an array with
     * dimensions [3][4][5], the first value returned by next() will be {0, 0, 0}; the second, {0,
     * 0, 1}; the sixth {0, 1, 0}; and so on throughout the array.
     *
     * @author PT
     */
    static class ArrayIterator implements Iterator<long[]> {

        long[] currentLocation;
        long[] dimensions;

        ArrayIterator(long[] dimensions) {
            this.dimensions = dimensions;
            currentLocation = new long[dimensions.length];
            currentLocation[0] = -1;
        }

        @Override
        public boolean hasNext() {
            boolean endOfRange = true;
            for (int i = 0; i < dimensions.length; i++) {
                endOfRange = endOfRange && currentLocation[i] == dimensions[i] - 1;
            }
            return !endOfRange;
        }

        @Override
        public long[] next() {
            currentLocation[0]++;
            for (int i = 0; i < currentLocation.length - 1; i++) {
                if (currentLocation[i] == dimensions[i]) {
                    currentLocation[i] = 0;
                    currentLocation[i + 1]++;
                }
            }
            return Arrays.copyOf(currentLocation, currentLocation.length);
        }
    }
}
