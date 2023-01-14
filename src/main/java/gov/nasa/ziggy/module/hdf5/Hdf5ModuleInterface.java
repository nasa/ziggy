package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_PERSISTABLE;
import static gov.nasa.ziggy.collections.ZiggyDataType.getDataType;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.io.Persistable;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Handles conversion between Persistable data objects and HDF5 files. The latter are used for data
 * exchange between the pipeline and the processing applications.
 * <p>
 * The HDF5 Persistable / Module Interface standard requires that the top-level object be a scalar.
 * The members of that object can be any of the following:
 * <ul>
 * <li>numeric arrays or lists; (scalar is a special case)
 * <li>boolean arrays or lists; (scalar is a special case)
 * <li>String arrays or lists; (scalar is a special case)
 * <li>Enum arrays or lists; (scalar is a special case)
 * <li>Persistable objects;
 * <li>Arrays or lists of Persistable objects.
 * </ul>
 *
 * @author PT
 */
public class Hdf5ModuleInterface {

    // writing the methods as class methods, just in case it proves useful to have
    // configuration parameters that affect the way that files are written or read

    private static final int COMPRESSION_LEVEL = 0;
    private static final int MIN_COMPRESSION_ELEMENTS = 200;

    // max number of bytes that should be read or written in any HDF5 hyperslab
    // to avoid the 2^31 - 8 byte limit (yes, that's a byte limit, not an element
    // limit) -- set to 2 billion here

    public static final int MAX_BYTES_PER_HYPERSLAB = 2000000000;

    private static final Logger log = LoggerFactory.getLogger(Hdf5ModuleInterface.class);

    // names for various attributes we apply to groups so that users can tell what's
    // going on with them
    public static final String FIELD_ORDER_ATT_NAME = "FIELD_ORDER";
    public static final String BOOLEAN_ARRAY_ATT_NAME = "LOGICAL_BOOLEAN_ARRAY";
    public static final String STRING_ARRAY_ATT_NAME = "STRING_ARRAY";
    public static final String OBJECT_ARRAY_ATT_NAME = "STRUCT_OBJECT_ARRAY";
    public static final String OBJECT_ARRAY_DIMS_ATT_NAME = "STRUCT_OBJECT_ARRAY_DIMS";
    public static final String PARALLEL_ARRAY_ATT_NAME = "PARALLEL_ARRAY";
    public static final String EMPTY_FIELD_ATT_NAME = "EMPTY_FIELD";
    public static final String FIELD_DATA_TYPE_ATT_NAME = "DATA_TYPE";
    public static final String ORIG_DIMS_ATT_NAME = "ORIGINAL_DIMS";
    public static final String PARAMETER_CLASS_NAME_ATT_NAME = "PARAMETER_CLASS_NAME";
    public static final String PARAMETER_SET_ORIGINAL_NAME_ATT_NAME = "PARAMETER_SET_ORIGINAL_NAME";
    public static final String SCALAR_PARAMETER_ATT_NAME = "SCALAR_PARAMETER_ATT_NAME";

    /**
     * Writes an object that implements the Persistable interface to an HDF5 file.
     *
     * @param file desired destination file
     * @param dataObject Persistable object
     */
    public void writeFile(File file, Persistable dataObject, boolean createGroupsForMissingFields) {
        long fileId = 0;
        try {

            // limit the interface to use of HDF5 1.8 functionality, so that hopefully
            // MATLAB can read files written from here
            H5.H5Pset_libver_bounds(HDF5Constants.H5P_FILE_ACCESS_DEFAULT,
                HDF5Constants.H5F_LIBVER_V18, HDF5Constants.H5F_LIBVER_V18);
            fileId = H5.H5Fcreate(file.getAbsolutePath(), HDF5Constants.H5F_ACC_TRUNC, H5P_DEFAULT,
                H5P_DEFAULT);
        } catch (NullPointerException | HDF5Exception | IllegalArgumentException e) {
            throw new PipelineException("Unable to create HDF5 file " + file.getName()
                + " in directory " + file.getParent(), e);
        }
        AbstractHdf5Array hdf5Array = AbstractHdf5Array.newInstance(dataObject);
        hdf5Array.setCreateGroupsForMissingFields(createGroupsForMissingFields);
        hdf5Array.write(fileId, "/");
        try {
            testForUnclosedHdf5Objects(fileId);
            H5.H5Fclose(fileId);
        } catch (HDF5LibraryException e) {
            throw new PipelineException("Unable to close HDF5 file " + file.getName()
                + " in directory " + file.getParent() + ", HDF5 file ID " + fileId, e);
        }
    }

    /**
     * Tests to determine whether there are any open HDF5 objects in a selected file.
     *
     * @param fileId HDF5 identifier of the file to be tested.
     */
    static void testForUnclosedHdf5Objects(long fileId) {
        long nOpen = H5.H5Fget_obj_count(fileId, HDF5Constants.H5F_OBJ_ALL);
        if (nOpen == 1) {
            log.info("No unclosed HDF5 objects detected");
        } else {
            log.warn("Number of unclosed HDF5 objects detected: " + (nOpen - 1));
            log.warn("Number of unclosed groups: "
                + H5.H5Fget_obj_count(fileId, HDF5Constants.H5F_OBJ_GROUP));
            log.warn("Number of unclosed datasets: "
                + H5.H5Fget_obj_count(fileId, HDF5Constants.H5F_OBJ_DATASET));
            log.warn("Number of unclosed datatypes: "
                + H5.H5Fget_obj_count(fileId, HDF5Constants.H5F_OBJ_DATATYPE));
            log.warn("Number of unclosed attributes: "
                + H5.H5Fget_obj_count(fileId, HDF5Constants.H5F_OBJ_ATTR));
        }
    }

    public static boolean allFieldsParallelizable(List<Field> fields) {
        boolean allPrimitiveScalar = true;
        for (Field field : fields) {
            Class<?> clazz = field.getType();
            boolean condition1 = getDataType(field) != ZIGGY_PERSISTABLE;
            boolean condition2 = !clazz.isArray();
            boolean condition3 = !List.class.isAssignableFrom(clazz);
            allPrimitiveScalar = allPrimitiveScalar && condition1 && condition2 && condition3;
        }
        return allPrimitiveScalar;
    }

    /**
     * Construct an HDF5 property for deflation and chunking
     *
     * @param elementCount number of array elements, if > MIN_COMPRESSION_ELEMENTS then compression
     * will be applied
     * @param chunkSize size of the desired array chunks in HDF5, if null no chunking will be
     * applied
     * @return ID for the resulting HDF5 property
     */
    static long chunkAndDeflateProperty(long elementCount, long[] chunkSize) {
        long deflateProperty;
        // TODO Eliminate dead code, or make COMPRESSION_LEVEL a variable
        if (elementCount > MIN_COMPRESSION_ELEMENTS && COMPRESSION_LEVEL > 0) {
            deflateProperty = H5.H5Pcreate(HDF5Constants.H5P_DATASET_CREATE);
            H5.H5Pset_chunk(deflateProperty, chunkSize.length, chunkSize);
            H5.H5Pset_deflate(deflateProperty, COMPRESSION_LEVEL);
        } else {
            deflateProperty = H5.H5Pcopy(HDF5Constants.H5P_DEFAULT);
        }
        return deflateProperty;
    }

    /**
     * Add an attribute to a group that contains the data type stored in the group
     *
     * @param fieldGroupId Identifier for the group
     * @param ziggyDataType Data type to be written to the group
     * @param fieldName Name of the field
     */
    static void writeDataTypeAttribute(long fieldGroupId, ZiggyDataType ziggyDataType,
        String fieldName) {
        try {
            long dataTypeAttributeSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            long dataTypeAttributeId = H5.H5Acreate(fieldGroupId, FIELD_DATA_TYPE_ATT_NAME,
                ZIGGY_INT.getHdf5Type(), dataTypeAttributeSpace, H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Awrite(dataTypeAttributeId, ZIGGY_INT.getHdf5Type(),
                new int[] { ziggyDataType.getAttributeTypeInt() });
            H5.H5Aclose(dataTypeAttributeId);
            H5.H5Sclose(dataTypeAttributeSpace);
        } catch (NullPointerException | HDF5Exception e) {
            throw new PipelineException(
                "Unable to create data type attribute for field " + fieldName, e);
        }
    }

    static String getFieldName(String fieldName, long[] arrayLocation) {
        StringBuilder fieldNameBuilder = new StringBuilder();
        fieldNameBuilder.append(fieldName);
        for (long location : arrayLocation) {
            fieldNameBuilder.append("-");
            fieldNameBuilder.append(Long.toString(location));
        }
        return fieldNameBuilder.toString();
    }

    // ***************************************************************************************

    /**
     * Reads an HDF5 file into an object that implements the Persistable interface.
     *
     * @param file desired source file
     * @param dataObject Persistable object. All of the contents of the dataObject will be
     * overwritten with the contents of the HDF5 file.
     */
    public boolean readFile(File file, Persistable dataObject, boolean allowMissingFields) {
        long fileId;
        try {
            fileId = H5.H5Fopen(file.getAbsolutePath(), HDF5Constants.H5F_ACC_RDONLY, H5P_DEFAULT);
        } catch (HDF5LibraryException | NullPointerException e) {
            throw new PipelineException("Unable to open HDF5 file " + file.getName()
                + " in directory " + file.getParent() + " for reading", e);
        }
        AbstractHdf5Array hdf5Array = AbstractHdf5Array.newInstance(dataObject);
        hdf5Array.setAllowMissingFields(allowMissingFields);
        hdf5Array.read(fileId);
        try {
            testForUnclosedHdf5Objects(fileId);
            H5.H5Fclose(fileId);
        } catch (HDF5LibraryException e) {
            throw new PipelineException(
                "Unable to close HDF5 file " + file.getName() + " in directory " + file.getParent(),
                e);
        }
        hdf5Array.isMissingFieldsDetected();
        return hdf5Array.isMissingFieldsDetected();
    }

    static ZiggyDataType readDataTypeAttribute(long fieldGroupId, String fieldName) {
        ZiggyDataType ziggyDataType = null;
        try {
            long dataTypeAttributeId = H5.H5Aopen(fieldGroupId, FIELD_DATA_TYPE_ATT_NAME,
                H5P_DEFAULT);
            int[] dataTypeArray = new int[1];
            H5.H5Aread(dataTypeAttributeId, ZIGGY_LONG.getHdf5Type(), dataTypeArray);
            H5.H5Aclose(dataTypeAttributeId);
            ziggyDataType = ZiggyDataType.getDataTypeFromAttributeTypeInt(dataTypeArray[0]);
        } catch (NullPointerException | HDF5Exception e) {
            throw new PipelineException("Unable to read HDF5 data type of field " + fieldName, e);
        }
        return ziggyDataType;
    }
}
