package gov.nasa.ziggy.module.hdf5;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ReflectionUtils;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class PersistableHdf5Array extends AbstractHdf5Array {

    private static final Logger log = LoggerFactory.getLogger(PersistableHdf5Array.class);

    ArrayIterator arrayIterator = null;
    boolean allFieldsPrimitiveScalar = false;

    /**
     * Instantiate an object for extraction of data from HDF5 and return to a Java object. This
     * constructor does not populate the array contents, but does set the organization for return of
     * data (array, scalar, or list).
     *
     * @param field Field in a Persistable object that will receive the data from this object. The
     * constructor is package-private
     */
    PersistableHdf5Array(Field field) {
        super(field);

        // capture the actual class of the Persistable field
        auxiliaryClass = getClassForEnumOrPersistable(field);
        detectPrimitiveScalarFields();
    }

    /**
     * Instantiate an object for extraction of data from Java and storage in HDF5.
     *
     * @param arrayObject the data object that is to be stored in HDF5.
     */
    PersistableHdf5Array(Object arrayObject) {
        super(arrayObject);

        // capture the actual class of the object
        auxiliaryClass = getClassForEnumOrPersistable(this.arrayObject);
        detectPrimitiveScalarFields();

        // initialize the array iterator
        resetArrayLocationCounter();
    }

    /**
     * Determines whether all the fields of the stored object are primitive scalars. In this case,
     * "scalar" means, "not an array or list," and "primitive" means, "not an object that implements
     * Persistable, but rather a primitive, String, or Enum object, or a boxed primitive."
     */
    void detectPrimitiveScalarFields() {
        allFieldsPrimitiveScalar = Hdf5ModuleInterface
            .allFieldsParallelizable(ReflectionUtils.getAllFields(auxiliaryClass, false));
    }

    /**
     * Populate the data contents of this object.
     *
     * @param arrayObject the data contents of the object. If the arrayObject is a scalar or a list,
     * it is converted to an appropriate array.
     */
    @Override
    public void setArray(Object arrayObject) {

        Object convertedObject = null;

        // determine whether the argument is a scalar
        scalar = isScalar(arrayObject);

        // if scalar, convert the object to a 1-d array
        if (scalar) {
            Class<?> clazz = arrayObject.getClass();
            Object[] scalarArray = (Object[]) Array.newInstance(clazz, 1);
            scalarArray[0] = arrayObject;
            convertedObject = scalarArray;
        }

        // if list, convert to array (with the usual gymnastics to get
        // the correct parameterized type)
        if (List.class.isAssignableFrom(arrayObject.getClass())) {
            List<?> listObject = (List<?>) arrayObject;
            convertedObject = listObject.toArray();

            // Special case: instances of the Parameters interface
            Object object0 = listObject.get(0);
            Class<?> arrayClass = object0.getClass();
            Object[] arrayFromList = (Object[]) Array.newInstance(arrayClass, listObject.size());
            for (int i = 0; i < listObject.size(); i++) {
                arrayFromList[i] = listObject.get(i);
            }
            convertedObject = arrayFromList;
        }

        // if we got this far without assigning convertedObject, then arrayObject
        // must be an actual array
        if (convertedObject == null) {
            convertedObject = arrayObject;
        }
        this.arrayObject = convertedObject;
    }

    /**
     * Return the contents of this object to Java for storage.
     *
     * @return the data contents of the object, converted to a scalar or a list if that is what the
     * receiving Persistable object's field expects.
     */
    @Override
    public Object toJava() {

        Object javaStorageObject = arrayObject;

        // If the return is a scalar, pull it out of the 1-d, unit
        // length array
        if (javaStorageObject != null && returnAs.equals(ReturnAs.SCALAR)) {
            Object[] javaStorageArray = (Object[]) javaStorageObject;
            javaStorageObject = javaStorageArray[0];
        }

        // if the return is to be a list, handle that now
        if (returnAs.equals(ReturnAs.LIST)) {
            Object[] javaStorageArray = (Object[]) javaStorageObject;
            List<Object> javaStorageList;
            if (javaStorageArray == null) {
                javaStorageList = new ArrayList<>();
            } else {
                javaStorageList = Arrays.asList(javaStorageArray);
            }
            javaStorageObject = javaStorageList;
        }

        // it the return is an array, but the object is empty, instantiate
        // as a zero-length array of the correct type

        if (returnAs.equals(ReturnAs.ARRAY) && javaStorageObject == null) {
            javaStorageObject = Array.newInstance(auxiliaryClass, 0);
        }
        return javaStorageObject;
    }

    /**
     * Sets the expected size and shape of the array. This is used during capture of arrays of
     * Persistable objects from HDF5, in which it is necessary to determine the array size and
     * shape, and to construct the array that will hold the objects, before we actually read in the
     * objects themselves.
     *
     * @param dimensions expected size and shape of the array of Persistable objects
     */
    void setDimensions(long[] dimensions) {
        if (this.dimensions != null || arrayObject != null) {
            throw new PipelineException("Array dimensions and/or object are not null");
        }
        if (auxiliaryClass == null) {
            throw new PipelineException("Final Persistable class has not been set");
        }
        this.dimensions = dimensions;
        arrayObject = constructFullArray();
        resetArrayLocationCounter();
    }

    /**
     * Return the location of the next array member to be read or written. The indices are
     * incremented from first to last (so in column-major order).
     *
     * @return long array showing the next location, or null if the end of the array has been
     * reached.
     */
    public long[] nextArrayLocation() {
        if (arrayIterator.hasNext()) {
            return arrayIterator.next();
        }
        return null;
    }

    /**
     * Resets the array location counter, in the unlikely event that a user wishes to iterate
     * through the array more than once.
     */
    void resetArrayLocationCounter() {
        arrayIterator = new ArrayIterator(dimensions);
    }

    /**
     * Construct a fully-dimensioned array of the appropriate Persistable class, using the
     * dimensions specified in this object, so that the array can be populated from one-at-a-time
     * recovery of Persistable objects from HDF5.
     *
     * @return Array in which the Persistable objects are null, but actual array objects are not
     * null, with the size, shape, and class needed for capturing the expected array of
     * Persistables.
     */
    Object constructFullArray() {
        return constructFullArray(dimensions);
    }

    /**
     * Recursively construct a full array of the appropriate Persistable class.
     *
     * @param dimensions the desired dimensions of the array. On the first call to the method, the
     * full dimensions are passed. On subsequent calls, the dimensions array is truncated from the
     * leading value to trailing values.
     * @return An array of the appropriate Persistable type. All the sub-arrays are instantiated,
     * but the actual Persistable objects are all null.
     */
    Object constructFullArray(long[] dimensions) {

        // build the outermost array (so for example PaInputs[3][][])
        Object returnObject = constructArray(dimensions.length, (int) dimensions[0]);

        // If the array is not 1-d, populate the next level down (i.e.,
        // make each of the array[i] equal to PaInputs[4][]). This uses a
        // recursive call to this method.
        Object[] returnArray = (Object[]) returnObject;
        if (dimensions.length > 1) {
            for (int i = 0; i < returnArray.length; i++) {
                returnArray[i] = constructFullArray(
                    Arrays.copyOfRange(dimensions, 1, dimensions.length));
            }
        }
        return returnObject;
    }

    /**
     * Construct an empty, n'th dimensional array of appropriate Persistable class.
     *
     * @param nDims Number of dimensions for the array, for example, nDims == 3 will produce an
     * array[length][][].
     * @param length Length of the array.
     * @return An array of the appropriate class and dimension (i.e., array[], array[][], etc) in
     * which all the array members are null (i.e., not populated with lower-level arrays).
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    Object constructArray(int nDims, int length) {
        checkArgument(length >= 0, "array length");

        // determine the array class
        String arrayClass = arrayClassToConstruct(nDims);

        // build it!
        try {
            return Array.newInstance(Class.forName(arrayClass), length);
        } catch (NegativeArraySizeException | ClassNotFoundException e) {
            // This can never occur. By construction, the class for the array is always
            // available and the size is never negative.
            throw new AssertionError(e);
        }
    }

    /**
     * Determines the correct component type for instantiation by constructArray
     *
     * @param nDims Number of dimensions for the array, for example nDims == 3 means that array[][]
     * is desired (because the component type for array[][][] is array[][]).
     * @return A string that specifies the component type for the array, for example
     * "[[Lgov.nasa.ziggy.common.persistable.Persistable;" would indicate that the components are
     * Persistable[][], which is the component type to use for an array of Persistable[][][].
     */
    String arrayClassToConstruct(int nDims) {
        String arrayClass = null;
        if (nDims == 1) {
            arrayClass = auxiliaryClass.getName();
        } else {
            StringBuilder arrayClassBuilder = new StringBuilder();
            for (int i = 1; i < nDims; i++) {
                arrayClassBuilder.append("[");
            }
            arrayClassBuilder.append("L");
            arrayClassBuilder.append(auxiliaryClass.getName());
            arrayClassBuilder.append(";");
            arrayClass = arrayClassBuilder.toString();
        }
        return arrayClass;
    }

    /**
     * Writes a scalar object (i.e., not an array or list of objects) that implements the
     * Persistable interface into an HDF5 group
     *
     * @param dataObject the Persistable object
     * @param fileId the HDF5 group
     * @return List of additional groups created during the write of this object *(typically these
     * come from writing objects that are members of the dataObject)
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    List<Long> writePersistableScalarObject(long fileId, String groupName) {

        Persistable[] dataObjectAsPersistableArray = (Persistable[]) getArrayObject();
        Persistable dataObject = dataObjectAsPersistableArray[0];
        List<Long> groupIds = new ArrayList<>();
        Class<?> clazz = dataObject.getClass();
        List<Field> fields = ReflectionUtils.getAllFields(clazz, false);

        // loop over fields
        int iField = 0;
        for (Field field : fields) {

            if (field.getAnnotation(ProxyIgnore.class) != null
                || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            field.setAccessible(true);

            // every field gets its own group, with a group order attribute

            // convert the field's contents to an appropriate HDF5 array object
            AbstractHdf5Array persistableField;
            try {
                persistableField = AbstractHdf5Array.newInstance(field.get(dataObject));
            } catch (IllegalAccessException e) {
                // This can never occur. We set the field to be accessible prior
                // to this attempt call get() on it.
                throw new AssertionError(e);
            }
            if (!createGroupsForMissingFields && isEmptyHdf5Array(persistableField)) {
                log.debug("Not creating group for empty field {} in class {}", field.getName(),
                    clazz.getName());
                continue;
            }
            if (persistableField != null) {
                persistableField.setCreateGroupsForMissingFields(isCreateGroupsForMissingFields());
            }

            long fieldGroupId = H5.H5Gcreate(fileId, field.getName(), H5P_DEFAULT, H5P_DEFAULT,
                H5P_DEFAULT);
            groupIds.add(fieldGroupId);
            writeFieldOrderAttribute(fieldGroupId, iField);
            iField++;

            // If this is an empty field, but we were instructed to create a group
            // for it, mark it as empty and move on

            if (isEmptyHdf5Array(persistableField)) {
                log.debug("Creating group for empty field {} in class {}", field.getName(),
                    clazz.getName());
                long orderAttributeSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
                long orderAttributeId = H5.H5Acreate(fieldGroupId,
                    Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME, ZIGGY_BYTE.getHdf5Type(),
                    orderAttributeSpace, H5P_DEFAULT, H5P_DEFAULT);
                H5.H5Aclose(orderAttributeId);
                H5.H5Sclose(orderAttributeSpace);
                H5.H5Gclose(fieldGroupId);
                continue;
            }

            // add an attribute that contains the data type that will be
            // contained in the group, because I'm tired of trying to infer
            // it from the peculiar way that HDF5 stores data type information

            Hdf5ModuleInterface.writeDataTypeAttribute(fieldGroupId,
                persistableField.getDataTypeToSave(), field.getName());

            // Use the HDF5 array's write method

            if (persistableField != null) {
                List<Long> newGroupIds = persistableField.write(fieldGroupId, field.getName());
                groupIds.addAll(newGroupIds);
            }
            H5.H5Gclose(fieldGroupId);
        } // end of loop over fields

        return groupIds;
    }

    /**
     * Write the actual class name of a Parameters subclass as an attribute.
     */
    public void setParameterClassNameAttribute(long groupId, Class<?> clazz) {
        writeStringAttribute(groupId, Hdf5ModuleInterface.PARAMETER_CLASS_NAME_ATT_NAME,
            clazz.getName());
    }

    @Override
    public List<Long> write(long fieldGroupId, String fieldName) {
        List<Long> subGroupIds = new ArrayList<>();

        // If this is a scalar object, then we can simply call the scalar
        // object writer

        if (isScalar()) {
            subGroupIds = writePersistableScalarObject(fieldGroupId, fieldName);

            // If the fields of the Persistable class are all primitive scalar,
            // use the specialized code that writes them as parallel arrays
        } else if (areAllFieldsPrimitiveScalar()) {

            // add the parallel array attribute to the group so that downstream
            // users know how to reconstruct this
            long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            long parallelArrayAttribute = H5.H5Acreate(fieldGroupId,
                Hdf5ModuleInterface.PARALLEL_ARRAY_ATT_NAME, ZIGGY_BYTE.getHdf5Type(), scalarSpace,
                H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Aclose(parallelArrayAttribute);
            H5.H5Sclose(scalarSpace);

            // get the parallel primitive arrays from the Persistable array
            List<PrimitiveHdf5Array> parallelArrays = toParallelArrays();

            // write them to the HDF5 file
            int iField = 0;
            for (PrimitiveHdf5Array primitiveArray : parallelArrays) {
                long subGroupId = H5.H5Gcreate(fieldGroupId, primitiveArray.getFieldName(),
                    H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
                writeFieldOrderAttribute(subGroupId, iField);
                iField++;
                Hdf5ModuleInterface.writeDataTypeAttribute(subGroupId,
                    primitiveArray.getDataTypeToSave(), primitiveArray.getFieldName());
                primitiveArray.write(subGroupId, primitiveArray.getFieldName());
                H5.H5Gclose(subGroupId);
                subGroupIds.add(subGroupId);
            }
        } else {

            // if we're here then it's an array of unknown dimension, and
            // needs to be written, with each object in its own sub-group of
            // the main group for the array. We also need to
            // tell future users that this group is the top of an object
            // array

            long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            long objectArrayAttribute = H5.H5Acreate(fieldGroupId,
                Hdf5ModuleInterface.OBJECT_ARRAY_ATT_NAME, ZIGGY_BYTE.getHdf5Type(), scalarSpace,
                H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Aclose(objectArrayAttribute);
            H5.H5Sclose(scalarSpace);

            // add an attribute that allows Java to know in advance the
            // size and shape of the array

            long[] persistableArrayDims = getDimensions();

            long arraySpace = H5.H5Screate_simple(1, new long[] { persistableArrayDims.length },
                null);
            long objectArrayDimsAttribute = H5.H5Acreate(fieldGroupId,
                Hdf5ModuleInterface.OBJECT_ARRAY_DIMS_ATT_NAME, ZIGGY_LONG.getHdf5Type(),
                arraySpace, H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Awrite(objectArrayDimsAttribute, ZIGGY_LONG.getHdf5Type(), persistableArrayDims);
            H5.H5Aclose(objectArrayDimsAttribute);
            H5.H5Sclose(arraySpace);

            subGroupIds = writePersistableArray(fieldGroupId, fieldName);
        }
        return subGroupIds;
    }

    List<Long> writePersistableArray(long fieldGroupId, String fieldName) {

        List<Long> subGroupIds = new ArrayList<>();
        long[] arrayLocation = nextArrayLocation();
        while (arrayLocation != null) {

            String newFieldName = Hdf5ModuleInterface.getFieldName(fieldName, arrayLocation);
            // create the new group
            long subGroupId = H5.H5Gcreate(fieldGroupId, newFieldName, H5P_DEFAULT, H5P_DEFAULT,
                H5P_DEFAULT);
            subGroupIds.add(subGroupId);
            PersistableHdf5Array persistableObject = new PersistableHdf5Array(
                getArrayMember(arrayLocation));
            persistableObject.setCreateGroupsForMissingFields(isCreateGroupsForMissingFields());
            subGroupIds.addAll(persistableObject.write(subGroupId, newFieldName));

            H5.H5Gclose(subGroupId);
            arrayLocation = nextArrayLocation();
        }
        return subGroupIds;
    }

    long openGroupIfPresent(long fileId, String groupName) {
        long groupId = -1;
        if (H5.H5Lexists(fileId, groupName, H5P_DEFAULT)) {
            groupId = H5.H5Gopen(fileId, groupName, H5P_DEFAULT);
        } else {
            missingFieldsDetected = true;
            if (!allowMissingFields) {
                throw new PipelineException("Unable to detect group named " + groupName);
            }
        }
        return groupId;
    }

    /**
     * Reads a scalar data object that implements the Persistable interface from HDF5. This
     * algorithm uses the names of the Persistable object's members to determine the names of the
     * corresponding HDF5 groups.
     *
     * @param fileId The HDF5 group that contains the data for the object.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    void readPersistableScalarObject(long fileId) {

        Object[] dataArray = (Object[]) arrayObject;
        Object dataObject = dataArray[0];
        List<Field> fields = ReflectionUtils.getAllFields(getAuxiliaryClass(), false);
        for (Field field : fields) {

            if (field.getAnnotation(ProxyIgnore.class) != null) {
                continue;
            }

            field.setAccessible(true);
            long fieldGroupId;

            // If the field doesn't have a corresponding group in the HDF5
            // file, we can either move on to the next field (if missing
            // fields are permitted), or throw an exception (if missing
            // fields are forbidden)
            fieldGroupId = openGroupIfPresent(fileId, field.getName());
            if (fieldGroupId == -1) {
                continue;
            }

            // Build an appropriate object to capture the data

            AbstractHdf5Array persistableField = AbstractHdf5Array.newInstance(field);
            persistableField.setAllowMissingFields(allowMissingFields);

            // go get the data

            persistableField.read(fieldGroupId);
            missingFieldsDetected = missingFieldsDetected || persistableField.missingFieldsDetected;
            try {
                field.set(dataObject, persistableField.toJava());
            } catch (IllegalAccessException e) {
                // This can never occur. The field was set to accessible earlier in this
                // method.
                throw new AssertionError(e);
            }
            H5.H5Gclose(fieldGroupId);
        }
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public void read(long fieldGroupId) {
        // If the return is an empty field, detect and handle that now

        boolean hasArrayDimsAttribute;
        if (H5.H5Aexists(fieldGroupId, Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME)) {
            return;
        }
        hasArrayDimsAttribute = H5.H5Aexists(fieldGroupId,
            Hdf5ModuleInterface.OBJECT_ARRAY_DIMS_ATT_NAME);
        if (!hasArrayDimsAttribute
            && (getReturnAs() == null || getReturnAs() == ReturnAs.UNKNOWN)) {
            returnAs = ReturnAs.SCALAR;
        }

        // If the object is an instance of Parameters, handle that now
        if (H5.H5Aexists(fieldGroupId, Hdf5ModuleInterface.PARAMETER_CLASS_NAME_ATT_NAME)) {
            String parameterClassName = getParameterClassNameFromAttribute(fieldGroupId);
            try {
                auxiliaryClass = Class.forName(parameterClassName);
            } catch (ClassNotFoundException e) {
                // This can never occur. By construction the name of the class is available
                // to Java.
                throw new AssertionError(e);
            }
            setArray(newPersistableObject());
        }

        // we must first check to see whether the HDF5 object is a scalar object with
        // parallel arrays, in which case we need to convert it back to the equivalent
        // array of objects
        if (areAllFieldsPrimitiveScalar()
            && H5.H5Aexists(fieldGroupId, Hdf5ModuleInterface.PARALLEL_ARRAY_ATT_NAME)) {

            List<Field> allFields = ReflectionUtils.getAllFields(getAuxiliaryClass(), false);

            // load the primitive arrays from the file
            List<PrimitiveHdf5Array> primitiveHdf5Arrays = new ArrayList<>();
            for (Field field : allFields) {
                PrimitiveHdf5Array primitiveHdf5Array = new PrimitiveHdf5Array(field);
                primitiveHdf5Array.returnAs = ReturnAs.ARRAY;
                long primitiveGroupId = openGroupIfPresent(fieldGroupId, field.getName());
                if (primitiveGroupId == -1) {
                    continue;
                }
                primitiveHdf5Array.read(primitiveGroupId);
                H5.H5Gclose(primitiveGroupId);

                // cast as needed
                primitiveHdf5Arrays.add(new PrimitiveHdf5Array(primitiveHdf5Array.toJava()));
            }

            // convert the parallel arrays into the array of Persistable objects
            forParallelArrays(allFields, primitiveHdf5Arrays);
        } else if (getReturnAs().equals(ReturnAs.SCALAR) || !hasArrayDimsAttribute) {
            if (arrayObject == null) {
                setDimensions(new long[] { 1 });
                setArrayMember(newPersistableObject(), new long[] { 0 });
            }
            readPersistableScalarObject(fieldGroupId);
        } else {
            // array, so slightly different activity:

            // get the dimensions
            long attributeId = H5.H5Aopen(fieldGroupId,
                Hdf5ModuleInterface.OBJECT_ARRAY_DIMS_ATT_NAME, H5P_DEFAULT);
            long dataSpaceId = H5.H5Aget_space(attributeId);
            int nDims = H5.H5Sget_simple_extent_ndims(dataSpaceId);
            long[] dims = new long[nDims];
            long[] maxDims = new long[nDims];
            H5.H5Sget_simple_extent_dims(dataSpaceId, dims, maxDims);
            long[] persistableDims = new long[(int) dims[0]];
            H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), persistableDims);
            H5.H5Sclose(dataSpaceId);
            H5.H5Aclose(attributeId);

            // Note that the group can be present, but the contents can be
            // null! This is the case if the writing application's equivalent
            // of createGroupsForMissingFields is set to true. Handle that
            // case now
            if (H5.H5Aexists(fieldGroupId, Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME)) {
                return;
            }

            // set the dimensions and build the array
            setDimensions(persistableDims);

            // iterate over array members
            long[] location = nextArrayLocation();
            while (location != null) {
                String arrayFieldName = Hdf5ModuleInterface.getFieldName(fieldName, location);
                long subGroupId = openGroupIfPresent(fieldGroupId, arrayFieldName);
                Object newObject = null;
                if (H5.H5Aexists(subGroupId, Hdf5ModuleInterface.PARAMETER_CLASS_NAME_ATT_NAME)) {
                    String parameterClassName = getParameterClassNameFromAttribute(subGroupId);
                    newObject = newPersistableObject(parameterClassName);
                } else {
                    newObject = newPersistableObject();
                }
                PersistableHdf5Array newArray = new PersistableHdf5Array(newObject);
                newArray.readPersistableScalarObject(subGroupId);
                H5.H5Gclose(subGroupId);
                setArrayMember(newObject, location);
                location = nextArrayLocation();
            }
        }
    }

    String getParameterClassNameFromAttribute(long fieldGroupId) {
        return readStringAttribute(fieldGroupId, Hdf5ModuleInterface.PARAMETER_CLASS_NAME_ATT_NAME);
    }

    /**
     * Returns a new instance of the class that the PersistableHdf5Array object is configured to
     * store.
     *
     * @return new object of the class in auxiliaryClass
     */
    Object newPersistableObject() {
        return newPersistableObject(auxiliaryClass.getName());
    }

    /**
     * Returns a new instance of a class given its Class object. This signature is used when it is
     * necessary to return an instance of a Parameters class.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    Object newPersistableObject(String className) {
        try {
            Constructor<?> constructor = Class.forName(className).getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
            | InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
            // This can never occur. By construction, the class has a default constructor.
            throw new AssertionError(e);
        }
    }

    /**
     * Converts an array of Persistable objects to a list of PrimitiveHdf5Array objects in which (a)
     * there is one PrimitiveHdf5Array object per Persistable field, (b) the shape of the arrays in
     * the PrimitiveHdf5Array objects is the same as the shape of the Persistable array, and (c) the
     * contents of the Persistable array are copied into the PrimitiveHdf5Array objects. In this
     * way, an array of Persistable objects can be converted to a set of parallel arrays. This can
     * only be done if all of the fields in the Persistable array are scalar and primitive / boxed /
     * String / Enum types.
     *
     * @return List of PrimitiveHdf5Array objects that contain the contents of the Persistable
     * object array.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    List<PrimitiveHdf5Array> toParallelArrays() {
        if (!allFieldsPrimitiveScalar) {
            throw new PipelineException("Cannot convert array of objects of class "
                + auxiliaryClass.getName() + " to parallel arrays");
        }
        List<Field> allFields = ReflectionUtils.getAllFields(auxiliaryClass, false);
        List<PrimitiveHdf5Array> primitiveArrays = new ArrayList<>();

        // construct the primitiveHdf5Array objects with the correct dimensions
        for (Field field : allFields) {
            Object parallelArray = ZiggyArrayUtils.constructFullPrimitiveArray(dimensions,
                getDataType(field));
            PrimitiveHdf5Array primitiveArray = new PrimitiveHdf5Array(parallelArray);
            primitiveArray.setFieldName(field.getName());
            primitiveArrays.add(primitiveArray);
        }

        // loop over the objects in the PersistableHdf5Array
        resetArrayLocationCounter();
        while (arrayIterator.hasNext()) {
            long[] location = nextArrayLocation();
            Object arrayMember = getArrayMember(location);

            // loop over fields and set the values in the parallel arrays
            for (int i = 0; i < allFields.size(); i++) {
                Field field = allFields.get(i);
                PrimitiveHdf5Array primitiveArray = primitiveArrays.get(i);
                try {
                    field.setAccessible(true);
                    primitiveArray.setArrayMember(field.get(arrayMember), location);
                } catch (IllegalAccessException e) {
                    // This can never occur. The field was set to accessible before the
                    // get() was called.
                    throw new AssertionError(e);
                }
            }
        }

        return primitiveArrays;
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    void forParallelArrays(List<Field> allFields, List<PrimitiveHdf5Array> primitiveArrays) {

        // set dimensions
        setDimensions(primitiveArrays.get(0).getDimensions());

        // make the fields accessible
        for (Field field : allFields) {
            field.setAccessible(true);
        }

        // loop over object members
        resetArrayLocationCounter();
        long[] location;
        while (arrayIterator.hasNext()) {
            location = arrayIterator.next();
            Object newObject = newPersistableObject();

            // loop over fields and populate the object
            for (int i = 0; i < primitiveArrays.size(); i++) {
                Field field = allFields.get(i);
                Object value = primitiveArrays.get(i).getArrayMember(location);
                try {
                    field.set(newObject, value);
                } catch (IllegalAccessException e) {
                    // This can never occur. The field was set to accessible earlier
                    // in this same method.
                    throw new AssertionError(e);
                }
            }

            // insert the object into the array
            setArrayMember(newObject, location);
        }
    }

    @Override
    public ZiggyDataType getDataTypeToSave() {
        return hdf5DataType;
    }

    public boolean areAllFieldsPrimitiveScalar() {
        return allFieldsPrimitiveScalar;
    }
}
