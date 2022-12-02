package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyArrayUtils.intToLong1d;
import static gov.nasa.ziggy.collections.ZiggyArrayUtils.longToInt1d;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BOOLEAN;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_ENUM;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static gov.nasa.ziggy.collections.ZiggyDataType.elementSizeBytes;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import gov.nasa.ziggy.collections.HyperRectangle;
import gov.nasa.ziggy.collections.HyperRectangleIterator;
import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Main class that provides impedance matching as needed between Java storage of numeric, string,
 * and enumerated data, and the way that data is stored in HDF5. This includes casting of
 * multi-dimensional arrays, boxing and unboxing of multi-dimensional arrays, conversion of scalars
 * and lists to arrays for HDF5 and back to scalars and lists for Java.
 *
 * @author PT
 */
public class PrimitiveHdf5Array extends AbstractHdf5Array {

    /**
     * Constructs a PrimitiveHdf5Array that is optimized for reading the contents of a
     * DefaultParameters parameter.
     *
     * @param groupId HDF5 group of the parameter.
     * @param fieldName Name of the HDF5 group.
     */
    static final PrimitiveHdf5Array forReadingDefaultParameters(long groupId, String fieldName) {
        PrimitiveHdf5Array p = new PrimitiveHdf5Array();
        p.returnAs = ReturnAs.ARRAY;
        p.fieldName = fieldName;
        if (H5.H5Aexists(groupId, Hdf5ModuleInterface.BOOLEAN_ARRAY_ATT_NAME)) {
            p.dataTypeOfReturn = ZIGGY_BOOLEAN;
        }
        return p;
    }

    /**
     * indicates the data type of the return so that the toJava() method knows whether a cast is
     * needed.
     */
    ZiggyDataType dataTypeOfReturn = null;

    /**
     * In cases where the data has to be cast for HDF5, store the type to cast to here
     */
    ZiggyDataType dataTypeToSave = null;

    /**
     * indicates whether the data returned to Java is boxed values rather than primitives, so that
     * conversion can be performed.
     */
    boolean boxReturn = false;

    /**
     * Shows the number of dimensions needed for the returned array in the event that we need to
     * return an array that is 0 x 0 x ...
     */
    int nDimensionsToReturn = 0;

    /**
     * Constructs an object from a data object. The type and size of the data is captured, scalars
     * and lists are converted to arrays, arrays are unboxed. All fields related to the storage of
     * data in Java are left null. Constructor is package-private so that the class can only be
     * instantiated via the Hdf5ArrayFactory methods, which provide some additional protection.
     *
     * @param object data object to be stored in HDF5.
     */
    PrimitiveHdf5Array(Object object) {
        super(object);
        dataTypeToSave = hdf5DataType;
        if (hdf5DataType.equals(ZIGGY_ENUM)) {
            dataTypeToSave = ZIGGY_STRING;
        }
        if (hdf5DataType.equals(ZIGGY_BOOLEAN)) {
            dataTypeToSave = ZIGGY_BYTE;
        }
    }

    /**
     * Construct an object from the field of a Persistable object. This records the type and
     * dimensionality of what the field expects to see, whether it expects a list, scalar, or array,
     * boxed or unboxed values. The actual data contents of the object are left null, as are the
     * data type and dimensions. This information can be added later via the setArray() method.
     * Constructor is package-private so that the class can only be instantiated via the
     * Hdf5ArrayFactory methods, which provide some additional protection.
     *
     * @param field field that will eventually store the data from this object, in a Persistable
     * object.
     */
    PrimitiveHdf5Array(Field field) {
        super(field);

        // capture the Hdf5 data type of the return, since it may
        // be necessary to cast the array based on this
        dataTypeOfReturn = getDataType(field);

        // determine whether the returned data type needs to be boxed
        boxReturn = ZiggyArrayUtils.isBoxedPrimitive(field);

        // if we are to return Enums, capture the actual class
        if (dataTypeOfReturn.equals(ZIGGY_ENUM)) {
            auxiliaryClass = getClassForEnumOrPersistable(field.getType());
        }

        // capture the dimensionality of the array
        if (returnAs.equals(ReturnAs.ARRAY)) {
            nDimensionsToReturn = getReturnDimensions(field.getType().getName());
        }
    }

    private PrimitiveHdf5Array() {
        super(new int[1]);
    }

    int getReturnDimensions(String className) {
        int nDims = 0;
        while (className.substring(0, 1).equals("[")) {
            nDims++;
            className = className.substring(1, className.length());
        }
        return nDims;
    }

    /**
     * Package contents of the object appropriately for storage in HDF5.
     *
     * @return an array ready to be stored in HDF5.
     */
    public Object toHdf5() {
        return toHdf5(arrayObject);
    }

    /**
     * Main method for HDF5 output. This is a method that can be used by this class and the
     * Hyperslab class, but is otherwise off limits.
     *
     * @param array array to be packaged, must be of the same type as the arrayObject member of the
     * PrimitiveHdf5Array object.
     * @return array, cast to the correct type for HDF5 storage.
     */
    Object toHdf5(Object array) {
        Object hdf5StorageObject = array;
        if (!hdf5DataType.equals(dataTypeToSave)) {
            hdf5StorageObject = ZiggyArrayUtils.castArray(array, dataTypeToSave);
        }
        if (dataTypeToSave.equals(ZIGGY_STRING)) {
            hdf5StorageObject = flattenArray(hdf5StorageObject);
        }
        return hdf5StorageObject;
    }

    /**
     * Flattens an array of strings in column-major order for storage in HDF5.
     *
     * @param mdArray
     * @return
     */
    Object flattenArray(Object mdArray) {
        long arraySize = 1;
        dimensions = ZiggyArrayUtils.getArraySize(mdArray);
        for (long i : dimensions) {
            arraySize *= i;
        }
        Object oneDArray = ZiggyArrayUtils.constructFullPrimitiveArray(new long[] { arraySize },
            getDataType(mdArray));
        ArrayIterator oneDIterator = new ArrayIterator(new long[] { arraySize });
        ArrayIterator mdIterator = new ArrayIterator(dimensions);
        while (mdIterator.hasNext()) {
            long[] oneDLocation = oneDIterator.next();
            long[] mdLocation = mdIterator.next();
            AbstractHdf5Array.setArrayMember(AbstractHdf5Array.getArrayMember(mdArray, mdLocation),
                oneDArray, oneDLocation);
        }
        return oneDArray;
    }

    /**
     * Unflattens a 1-d array into the equivalent multi-dimensional array appropriate for this
     * PrimitiveHdf5Array. The 1-d array is stored in column-major order (i.e., "FORTRAN order").
     * Although general, in this case it is only expected to be used for String arrays.
     *
     * @param oneDArray
     * @return
     */
    Object unflattenArray(Object oneDArray) {
        Object mdArray = ZiggyArrayUtils.constructFullPrimitiveArray(dimensions,
            getDataType(oneDArray));
        unflattenArray(oneDArray, mdArray);
        return mdArray;
    }

    /**
     * Unflattens a 1-d array into a provided multi-dimensional array with the same total number of
     * elements.
     *
     * @param oneDArray
     * @param mdArray
     */
    void unflattenArray(Object oneDArray, Object mdArray) {
        ArrayIterator oneDIterator = new ArrayIterator(ZiggyArrayUtils.getArraySize(oneDArray));
        ArrayIterator mdIterator = new ArrayIterator(dimensions);
        while (mdIterator.hasNext()) {
            long[] oneDLocation = oneDIterator.next();
            long[] mdLocation = mdIterator.next();
            AbstractHdf5Array.setArrayMember(
                AbstractHdf5Array.getArrayMember(oneDArray, oneDLocation), mdArray, mdLocation);
        }
    }

    /**
     * Package contents of the object appropriately for storage in Java.
     *
     * @return an object ready to be stored in Java. This includes boxing unboxed values if
     * necessary, converting byte arrays to boolean if necessary, converting String arrays to Enums
     * if necessary, and converting arrays to scalars or lists if necessary.
     */
    @Override
    public Object toJava() {

        Object javaStorageObject = arrayObject;
        if (javaStorageObject == null) {
            return returnEmptyToJava();
        }

        // if a cast is needed, apply it now -- this should cover both
        // string-to-enum and byte-to-boolean, as well as any numeric casts

        if (hdf5DataType == null) {
            throw new PipelineException("hdf5DataType null pointer for field " + fieldName);
        }
        if (dataTypeOfReturn == null) {
            throw new PipelineException("dataTypeOfReturn null pointer for field " + fieldName);
        }
        if (!hdf5DataType.equals(dataTypeOfReturn)) {
            if (!dataTypeOfReturn.equals(ZIGGY_ENUM)) {
                javaStorageObject = ZiggyArrayUtils.castArray(arrayObject, dataTypeOfReturn);
            } else {
                javaStorageObject = ZiggyArrayUtils.castArray(arrayObject, auxiliaryClass);
            }
        }

        // if the return is boxed, handle that now -- note that scalar return
        // need to be boxed regardless of whether the Java field is boxed, and
        // Java will unbox the value at the appropriate time
        boolean needsPrimitiveBox = returnAs.equals(ReturnAs.SCALAR)
            && (dataTypeOfReturn.isNumeric() || dataTypeOfReturn.equals(ZIGGY_BOOLEAN));
        if (boxReturn || needsPrimitiveBox) {
            javaStorageObject = ZiggyArrayUtils.box(javaStorageObject);
        }
        if (returnAs.equals(ReturnAs.SCALAR)) {
            Object[] javaStorageArray = (Object[]) javaStorageObject;
            javaStorageObject = javaStorageArray[0];
        }

        // if the return is to be a list, handle that now -- note that
        // if the field was for a list, it will automatically have
        // configured this object to return boxed values

        if (returnAs.equals(ReturnAs.LIST)) {
            Object[] javaStorageArray = (Object[]) javaStorageObject;
            List<Object> javaStorageList = Arrays.asList(javaStorageArray);
            javaStorageObject = javaStorageList;
        }
        return javaStorageObject;
    }

    Object returnEmptyToJava() {
        Object emptyReturn = null;

        if (returnAs.equals(ReturnAs.SCALAR)) {
            if (dataTypeOfReturn == ZiggyDataType.ZIGGY_STRING
                || dataTypeOfReturn == ZiggyDataType.ZIGGY_ENUM) {
                emptyReturn = null;
            } else if (dataTypeOfReturn == ZiggyDataType.ZIGGY_CHAR) {
                emptyReturn = null;
            } else {
                emptyReturn = dataTypeOfReturn.boxedZero();
            }
        }
        if (returnAs.equals(ReturnAs.LIST)) {
            Object obj = ZiggyArrayUtils.constructFullBoxedArray(new long[] { 0 },
                dataTypeOfReturn);
            emptyReturn = Arrays.asList((Object[]) obj);
        }
        if (returnAs.equals(ReturnAs.ARRAY)) {
            long[] arraySize = new long[nDimensionsToReturn];
            for (int i = 0; i < arraySize.length; i++) {
                arraySize[i] = 0;
            }
            emptyReturn = ZiggyArrayUtils.constructFullArray(arraySize, dataTypeOfReturn,
                boxReturn);
        }
        return emptyReturn;
    }

    @Override
    public void setArray(Object arrayObject) {

        // determine the HDF5 type of the data
        hdf5DataType = getDataType(arrayObject);

        // determine whether the argument is a scalar
        scalar = isScalar(arrayObject);

        // convert from scalar or list to array and store
        this.arrayObject = convertToPrimitiveArray(arrayObject);

        // if this is an Enum, we need more information than that it's
        // an Enum, we also need to store the Enum class of the data

        if (hdf5DataType.equals(ZIGGY_ENUM)) {
            auxiliaryClass = getClassForEnumOrPersistable(this.arrayObject);
        }

        dimensions = ZiggyArrayUtils.getArraySize(this.arrayObject);
    }

    /**
     * Converts all potential forms of HDF5 instantiating objects to arrays of primitves.
     *
     * @param arrayObject Object used to instantiate the PrimitiveHdf5Array. This can include
     * scalars, arrays, lists, boxed and unboxed types.
     * @return The contents of arrayObject, converted to an array of a primitive data type, an array
     * of String type, or an array of Enum type. Scalars are converted to 1-element 1-dimensional
     * arrays, lists are converted to 1-dimensional arrays, and all boxed primitives are unboxed.
     */
    Object convertToPrimitiveArray(Object arrayObject) {

        Object convertedObject = null;

        // if the object is a scalar, convert to array -- note that as it is
        // an object, we know that it must be either an Enum, a String, or a boxed
        // primitive and cannot be an unboxed primitive
        if (scalar) {
            Class<?> clazz = arrayObject.getClass();
            Object[] scalarArray = (Object[]) Array.newInstance(clazz, 1);
            scalarArray[0] = arrayObject;
            convertedObject = scalarArray;
        }

        // if the object is a list, convert it to an array
        if (List.class.isAssignableFrom(arrayObject.getClass())) {
            List<?> listObject = (List<?>) arrayObject;

            Object[] arrayFromList = (Object[]) Array.newInstance(listObject.get(0).getClass(),
                listObject.size());
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

        // if this is an array of boxed primitives, unbox them
        if (ZiggyArrayUtils.isBoxedPrimitive(convertedObject)) {
            convertedObject = ZiggyArrayUtils.unbox(convertedObject);
        }
        return convertedObject;

    }

    public ZiggyDataType getDataTypeOfReturn() {
        return dataTypeOfReturn;
    }

    @Override
    public ZiggyDataType getDataTypeToSave() {
        return dataTypeToSave;
    }

    @Override
    public List<Long> write(long fieldGroupId, String fieldName) {

        // The primitive writer doesn't generate any additional, subsidiary groups, so
        // we can define an empty List to be returned
        List<Long> emptyGroupIdList = new ArrayList<>();

        // determine array size
        long[] arraySize = getDimensions();

        // determine total # of elements
        long nElements = 1;
        for (long iSize : arraySize) {
            nElements *= iSize;
        }

        try {
            // create data space
            long dataSpace = H5.H5Screate_simple(arraySize.length, arraySize, null);

            // set up compression if desired
            long deflateProperty;

            // if the array was originally a boolean array, but is now logical,
            // we need to create an attribute that signals this to future users
            // of the data
            if (getHdf5DataType() == ZIGGY_BOOLEAN) {
                long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
                long logicalAttribute = H5.H5Acreate(fieldGroupId,
                    Hdf5ModuleInterface.BOOLEAN_ARRAY_ATT_NAME, ZIGGY_BYTE.getHdf5Type(),
                    scalarSpace, H5P_DEFAULT, H5P_DEFAULT);
                H5.H5Aclose(logicalAttribute);
                H5.H5Sclose(scalarSpace);
            }

            // set the data type
            long dataType = getDataTypeToSave().getHdf5Type();

            // if this is a string array, there is considerable special preparation
            // that needs to be made: we need to create a variable-length datatype,
            // note whether the original data came from a string array

            if (getDataTypeToSave() == ZIGGY_STRING) {
                dataType = H5.H5Tcopy(ZIGGY_STRING.getHdf5Type());
                H5.H5Tset_size(dataType, HDF5Constants.H5T_VARIABLE);
                if (!isScalar()) {
                    long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
                    long stringArrayAttribute = H5.H5Acreate(fieldGroupId,
                        Hdf5ModuleInterface.STRING_ARRAY_ATT_NAME, ZIGGY_BYTE.getHdf5Type(),
                        scalarSpace, H5P_DEFAULT, H5P_DEFAULT);
                    H5.H5Aclose(stringArrayAttribute);
                    H5.H5Sclose(scalarSpace);
                }
            }

            // construct the dataset, write the data, close everything
            long dataset;
            if (getDataTypeToSave() == ZIGGY_STRING) {
                deflateProperty = Hdf5ModuleInterface.chunkAndDeflateProperty(nElements, arraySize);
                dataset = H5.H5Dcreate(fieldGroupId, fieldName, dataType, dataSpace, H5P_DEFAULT,
                    deflateProperty, H5P_DEFAULT);
                H5.H5Dwrite_VLStrings(dataset, dataType, HDF5Constants.H5S_ALL,
                    HDF5Constants.H5S_ALL, H5P_DEFAULT, (Object[]) toHdf5());
            } else {

                // here we need to iterate over hyperslabs to stay below the HDF5-Java limit of
                // 2.2 GB per hyperslab
                PrimitiveHdf5Array.HyperslabIterator hI = new HyperslabIterator();
                deflateProperty = Hdf5ModuleInterface.chunkAndDeflateProperty(nElements,
                    hI.chunkSize());
                dataset = H5.H5Dcreate(fieldGroupId, fieldName, dataType, dataSpace, H5P_DEFAULT,
                    deflateProperty, H5P_DEFAULT);
                while (hI.hasNext()) {
                    PrimitiveHdf5Array.Hyperslab h = hI.next();
                    H5.H5Sselect_hyperslab(dataSpace, HDF5Constants.H5S_SELECT_SET,
                        h.hyperslabStart(), h.hyperslabStride(), h.hyperslabCount(),
                        h.hyperslabBlock());
                    long memSpace = H5.H5Screate_simple(arraySize.length, h.hyperslabBlock(), null);
                    H5.H5Dwrite(dataset, dataType, memSpace, dataSpace, H5P_DEFAULT,
                        toHdf5(h.getHyperslab()));
                    H5.H5Sclose(memSpace);
                }
            }
            H5.H5Dclose(dataset);
            if (dataType != getDataTypeToSave().getHdf5Type()) {
                H5.H5Tclose(dataType);
            }
            H5.H5Pclose(deflateProperty);
            H5.H5Sclose(dataSpace);
        } catch (NullPointerException | IllegalArgumentException | HDF5Exception e) {
            throw new PipelineException(
                "Unable to write primitive array from field " + fieldName + " to HDF5 group", e);
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return emptyGroupIdList;
    }

    @Override
    public void read(long fieldGroupId) {
        // If the return is an empty field, detect and handle that now

        try {
            if (H5.H5Aexists(fieldGroupId, Hdf5ModuleInterface.EMPTY_FIELD_ATT_NAME)) {
                return;
            }
        } catch (HDF5LibraryException | NullPointerException e1) {
            throw new PipelineException(
                "HDF5 error occurred attempting to detect empty field attribute", e1);
        }
        try {
            // get the dataset for the numeric array, which will have the same name as the group,
            // which in turn is the same as the name of the field
            long dataSetId = H5.H5Dopen(fieldGroupId, getFieldName(), H5P_DEFAULT);

            // get the type and the dimensions of the HDF5 array
            long hdf5TypeInt = H5.H5Dget_type(dataSetId);

            long dataSpaceId = H5.H5Dget_space(dataSetId);
            int nDims = H5.H5Sget_simple_extent_ndims(dataSpaceId);
            long[] dimensions = new long[nDims];
            long[] maxDimensions = new long[nDims];
            H5.H5Sget_simple_extent_dims(dataSpaceId, dimensions, maxDimensions);

            // construct an array to capture the data in the dataset

            ZiggyDataType hType = Hdf5ModuleInterface.readDataTypeAttribute(fieldGroupId,
                getFieldName());

            // in the special case in which there is no data type to return set, set
            // it to match the data type of the HDF5 data
            if (dataTypeOfReturn == null) {
                dataTypeOfReturn = hType;
            }

            // get the values out of the dataspace
            if (hType == ZIGGY_STRING) {
                Object dataArray = ZiggyArrayUtils.constructFullArray(dimensions, hType, false);
                Object flattenedArray = flattenArray(dataArray);
                long typeId = H5.H5Dget_type(dataSetId);
                H5.H5Dread_VLStrings(dataSetId, typeId, HDF5Constants.H5S_ALL,
                    HDF5Constants.H5S_ALL, H5P_DEFAULT, (Object[]) flattenedArray);
                unflattenArray(flattenedArray, dataArray);
                setArray(dataArray);
            } else {
                PrimitiveHdf5Array.HyperslabIterator hI = new HyperslabIterator(dimensions, hType);
                while (hI.hasNext()) {
                    PrimitiveHdf5Array.Hyperslab h = hI.next();
                    H5.H5Sselect_hyperslab(dataSpaceId, HDF5Constants.H5S_SELECT_SET,
                        h.hyperslabStart(), h.hyperslabStride(), h.hyperslabCount(),
                        h.hyperslabBlock());
                    long memSpace = H5.H5Screate_simple(dimensions.length, h.hyperslabBlock(),
                        null);
                    Object dataArray = ZiggyArrayUtils.constructFullArray(h.hyperslabBlock(), hType,
                        false);
                    H5.H5Dread(dataSetId, hdf5TypeInt, memSpace, dataSpaceId, H5P_DEFAULT,
                        dataArray);
                    h.putHyperslab(dataArray);
                    H5.H5Sclose(memSpace);
                }
            }

            H5.H5Sclose(dataSpaceId);
            H5.H5Dclose(dataSetId);
        } catch (HDF5Exception e) {
            throw new PipelineException(
                "Unable to read numeric array from HDF5 for field " + getFieldName(), e);
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Class that provides management of hyperslabs (HDF5 word for hyper-rectangles). This class
     * allows the PrimitiveHdf5Class objects to read from and write to a sub-section of the object's
     * arrayObject member.
     *
     * @author PT
     */
    class Hyperslab extends HyperRectangle {

        /**
         * Constructor used to write a hyperslab to HDF5. In this case the arrayObject has been
         * populated so its size can be determined.
         *
         * @param size size of the hyperslab.
         * @param location location of the hyperslab in the destination array.
         */
        Hyperslab(long[] size, long[] location) {
            super(longToInt1d(ZiggyArrayUtils.getArraySize(arrayObject)), longToInt1d(size),
                longToInt1d(location));
//            this.size = size;
//            this.location = location;
//            this.arrayObjectSize = ZiggyArrayUtils.getArraySize(arrayObject);
//            if (!checkSlabParameters()) {
//                throw new PipelineException("Unable to obtain hyperslab of size "
//                    + Arrays.toString(size) + " and location " + Arrays.toString(location)
//                    + " from array of size " + Arrays.toString(arrayObjectSize));
//            }
        }

        /**
         * Constructor used to read a hyperslab from HDF5. In this case the arrayObject has not been
         * populated so the caller must supply its final size.
         *
         * @param size size of the hyperslab.
         * @param location location of the hyperslab in the destination array.
         * @param arrayObjectSize size of the final destination array for the hyperslab.
         */
        Hyperslab(long[] size, long[] location, long[] arrayObjectSize) {
            super(longToInt1d(arrayObjectSize), longToInt1d(size), longToInt1d(location));
//            this.size = size;
//            this.location = location;
//            this.arrayObjectSize = arrayObjectSize;
//            if (!checkSlabParameters()) {
//                throw new PipelineException("Unable to obtain hyperslab of size "
//                    + Arrays.toString(size) + " and location " + Arrays.toString(location)
//                    + " from array of size " + Arrays.toString(arrayObjectSize));
//            }
        }

        /**
         * Returns a value for the HDF5 hyperslab "start" parameter (equivalent to location)
         *
         * @return start parameter corresponding to this hyperslab
         */
        long[] hyperslabStart() {
            // return location;
            return intToLong1d(getOffset());
        }

        /**
         * Returns a value for the HDF5 hyperslab "stride" parameter (null in this case for stride
         * == 1 in all directions)
         *
         * @return stride parameter for this hyperslab
         */
        long[] hyperslabStride() {
            return null;
        }

        /**
         * Returns a value for the HDF5 hyperslab "count" parameter (all ones, there is only 1
         * block)
         *
         * @return count parameter for this hyperslab
         */
        long[] hyperslabCount() {
            long[] count = (long[]) ZiggyArrayUtils.constructPrimitiveArray(1, getOffset().length,
                ZIGGY_LONG);
            Arrays.fill(count, 1);
            return count;
        }

        /**
         * Returns a value for the HDF5 hyperslab "block" parameter (equivalent to size)
         *
         * @return block parameter for this hyperslab
         */
        long[] hyperslabBlock() {
            // return size;
            return intToLong1d(getSize());
        }

        /**
         * Gets a section of the arrayObject
         *
         * @return the section of the arrayObject determined by the size and location arguments; for
         * example if arrayObject has dimensions [3][4][5], the size argument is [1][2][5], and the
         * location argument is [1][2][0], then the returned array will be arrayObject[1][2:3][0:4].
         * The return will also be cast to the correct type for output to HDF5, if necessary.
         */
        Object getHyperslab() {
            return toHdf5(
                getHyperslab(intToLong1d(getSize()), intToLong1d(getOffset()), arrayObject));
        }

        /**
         * Performs the main work of producing the hyperslab, including recursion over the array
         * dimensions.
         *
         * @param slabSize size of the hyperslab to be returned in all dimensions
         * @param slabLocation location of the hyperslab to be returned in all dimensions
         * @param arrayObject source object to get the hyperslab from
         * @return the section of the arrayObject determined by the size and location arguments; for
         * example if arrayObject has dimensions [3][4][5], the size argument is [1][2][5], and the
         * location argument is [1][2][0], then the returned array will be arrayObject[1][2:3][0:4].
         * NB: in the case where size is an array of all 1's, and arraySize is NOT an array of all
         * 1's, this method will fail.
         */
        Object getHyperslab(long[] slabSize, long[] slabLocation, Object array) {

            // handle the trivial case first
            long[] arraySize = ZiggyArrayUtils.getArraySize(array);
            if (Arrays.equals(arraySize, slabSize)) {
                return array;
            }

            Object returnArray = null;
            // if the largest dimension is a singleton, we can construct the array for return, and
            // then use recursion to pull it out of the source array
            if (slabSize[0] == 1) {
                returnArray = ZiggyArrayUtils.constructPrimitiveArray(slabSize.length, 1,
                    getDataType(array));
                Object[] returnArray0 = (Object[]) returnArray;
                Object[] array0 = (Object[]) array;
                returnArray0[0] = getHyperslab(ArrayUtils.subarray(slabSize, 1, slabSize.length),
                    ArrayUtils.subarray(slabLocation, 1, slabSize.length),
                    array0[(int) slabLocation[0]]);
            } else {
                // otherwise, perform the assignment or copy operation for the appropriate
                // lower-level objects
                returnArray = ZiggyArrayUtils.constructPrimitiveArray(slabSize.length,
                    (int) slabSize[0], getDataType(array));
                if (slabSize.length > 1) { // subunits are arrays
                    Object[] returnArray0 = (Object[]) returnArray;
                    Object[] array0 = (Object[]) array;
                    for (int iPos = 0; iPos < slabSize[0]; iPos++) {
                        returnArray0[iPos] = array0[iPos + (int) slabLocation[0]];
                    }
                } else { // subunits are primitives, we are at the lowest level of the array
                    System.arraycopy(array, (int) slabLocation[0], returnArray, 0,
                        (int) slabSize[0]);
                }
            }

            return returnArray;
        }

        /**
         * Puts a hyperslab from HDF5 into the correct location in the PrimitiveHdf5Array's
         * arrayObject
         *
         * @param slabArray subsection of the final array to be placed
         */
        void putHyperslab(Object slabArray) {

            long[] size = intToLong1d(getSize());
            long[] arrayObjectSize = intToLong1d(getFullArraySize());
            long[] location = intToLong1d(getOffset());
            // check that the size is correct
            if (!Arrays.equals(size, ZiggyArrayUtils.getArraySize(slabArray))) {
                throw new PipelineException(
                    "Size of slab array " + ZiggyArrayUtils.getArraySize(slabArray).toString()
                        + " does not agree with size of Hyperslab object " + size.toString());
            }

            // the vast majority of the time, the slabArray will be the entire array that
            // we want to capture, so handle that special case

            if (Arrays.equals(size, arrayObjectSize)) {
                setArray(slabArray);
                return;
            }

            // the first time through we have to construct the outer level of the array and
            // supply the HDF5 data type
            if (arrayObject == null) {
                arrayObject = ZiggyArrayUtils.constructPrimitiveArray(arrayObjectSize.length,
                    (int) arrayObjectSize[0], getDataType(slabArray));
                hdf5DataType = getDataType(arrayObject);
            }

            // call the recursive method to fill in the array with the hyperslab
            putHyperslab(arrayObject, arrayObjectSize, location, slabArray);
        }

        /**
         * Recursively puts a hyperslab from HDF5 into the correct location in an array
         *
         * @param array the destination array
         * @param arraySize the desired size of the destination array
         * @param slabLocation the desired location of the hyperslab in the destination array
         * @param slabArray the array to be placed at the specified location of the destination
         * array
         */
        void putHyperslab(Object array, long[] arraySize, long[] slabLocation, Object slabArray) {

            long[] slabSize = ZiggyArrayUtils.getArraySize(slabArray);

            int location0 = (int) slabLocation[0];

            // if array size is a singleton, then: recursively call the next-level-down put
            // operation
            if (slabSize[0] == 1) {
                Object[] array0 = (Object[]) array;
                Object[] slab0 = (Object[]) slabArray;
                long[] subArraySize = ArrayUtils.subarray(arraySize, 1, arraySize.length);
                long[] subArrayLocation = ArrayUtils.subarray(slabLocation, 1, slabLocation.length);
                // if the next level down has not been constructed yet, take care of that now
                if (array0[location0] == null) {
                    array0[location0] = ZiggyArrayUtils.constructPrimitiveArray(subArraySize.length,
                        (int) subArraySize[0], getDataType(slab0));
                }
                putHyperslab(array0[location0], subArraySize, subArrayLocation, slab0[0]);
            } else {
                // non-singleton, need to copy / move the data over from the slab to the main array
                if (slabSize.length > 1) {
                    // subunits are arrays
                    Object[] array0 = (Object[]) array;
                    Object[] slab0 = (Object[]) slabArray;
                    for (int i = 0; i < slabSize[0]; i++) {
                        array0[location0 + i] = slab0[i];
                    }
                } else {
                    // subunits are primitives
                    System.arraycopy(slabArray, 0, array, location0, (int) slabSize[0]);
                }
            }
        }

    }

    /**
     * Iterator for Hyperslab class. This provides individual hyperslabs for use in reading from
     * HDF5 and writing to HDF5, in cases where division into hyperslabs is needed or desired.
     *
     * @author PT
     */
    class HyperslabIterator extends HyperRectangleIterator {

        /**
         * Constructor to use for writing to HDF5 -- in this case, we know the type and the size of
         * the array based on the arrayObject and dataTypeToSave members of the PrimitiveHdf5Array
         * object
         *
         * @throws SecurityException
         * @throws NoSuchFieldException
         * @throws IllegalAccessException
         * @throws IllegalArgumentException
         */
        HyperslabIterator() throws IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException {
            this(ZiggyArrayUtils.getArraySize(arrayObject), dataTypeToSave);
        }

        /**
         * Constructor to use for reading from HDF5 -- in this case, we need to supply the iterator
         * with the size and data type of the full array
         *
         * @param arraySize
         * @param dataType
         * @throws SecurityException
         * @throws NoSuchFieldException
         * @throws IllegalAccessException
         * @throws IllegalArgumentException
         */
        HyperslabIterator(long[] arraySize, ZiggyDataType dataType) throws IllegalArgumentException,
            IllegalAccessException, NoSuchFieldException, SecurityException {
            this(arraySize, dataType, Hdf5ModuleInterface.MAX_BYTES_PER_HYPERSLAB);
        }

        /**
         * Constructor that permits reduction in the desired maxBytesPerHyperslab value. This is
         * mainly useful for test purposes, so that the HyperslabIterator can be tested with
         * smallish arrays. It is not recommended for normal use.
         *
         * @param arraySize
         * @param dataType
         * @param maxBytes
         * @throws SecurityException
         * @throws NoSuchFieldException
         * @throws IllegalAccessException
         * @throws IllegalArgumentException
         */
        HyperslabIterator(long[] arraySize, ZiggyDataType dataType, long maxBytes)
            throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException {
            super(longToInt1d(arraySize), (int) maxBytes / elementSizeBytes(dataType));
        }

        /**
         * Returns the next hyperslab from the iterator, or throws a NoSuchElementException if there
         * is no next hyperslab.
         */
        @Override
        public Hyperslab next() {

            // Use the HyperRectangleIterator to get the next HyperRectangle, and then convert
            // same into a Hyperslab
            HyperRectangle hr = super.next();
            return new Hyperslab(intToLong1d(hr.getSize()), intToLong1d(hr.getOffset()),
                intToLong1d(hr.getFullArraySize()));
        }

        public long[] chunkSize() {
            return intToLong1d(getSize());
        }
    }

}
