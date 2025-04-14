package gov.nasa.ziggy.pipeline.step.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.step.AlgorithmParameters;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5O_token_t;

/**
 * HDF5 interface for the special case of a {@link AlgorithmParameters} scalar instance. The
 * {@link AlgorithmParameters} class introduces several special issues related to serialization:
 * <ol>
 * <li>{@link Parameter} instances permit names to contain whitespace, HDF5 group names do not.
 * <li>{@link Parameter} instances store their content as {@link String}s, with a special enum that
 * indicates the class and a boolean that indicates whether the parameter is a scalar or a 1-D
 * array. Thus each {@link Parameter} will require special handling for translation to and from HDF5
 * format.
 * <li>The {@link AlgorithmParameters} object stores {@link Parameter} instances in a {@link Map}
 * from parameter set name to parameter set. HDF5 doesn't even have Maps as such.
 * </ol>
 *
 * @author PT
 */
public class AlgorithmParametersHdf5Array extends AbstractHdf5Array {

    /**
     * Constructor based on a Field of a Persistable instance, used during HDF5 read operaions.
     * <p>
     * In this case, there is no special handling required over and above the superclass
     * constructor, as the field is guaranteed to be a scalar AlgorithmParameters field.
     */
    AlgorithmParametersHdf5Array(Field field) {
        super(field);
    }

    /**
     * Constructor based on an object, used during HDF5 write operations.
     * <p>
     * In this case, there is no special handling required over and above the superclass
     * constructor, as the object is guaranteed to be a scalar AlgorithmParameters instance.
     */
    AlgorithmParametersHdf5Array(Object object) {
        super(object);
    }

    /**
     * Sets the instance's Java object.
     * <p>
     * In this case, there is no special handling required because the object is guaranteed to be a
     * scalar AlgorithmParameters instance.
     */
    @Override
    public void setArray(Object arrayObject) {
        this.arrayObject = arrayObject;
    }

    /**
     * Returns the contents of the instance in a form ready to be inserted into the parent
     * Persistable object.
     * <p>
     * In this case, the contents of the instance are guaranteed to be a scalar AlgorithmParameters
     * instance and thus require no post-processing to be stored in the parent instance.
     */
    @Override
    public Object toJava() {
        return arrayObject;
    }

    /**
     * Writes the contents of the AlgorithmParameters to HDF5.
     * <p>
     * Classed parameter sets are written using the standard PersistableHdf5Array approach, but they
     * use the class simple name as the group name.
     * <p>
     * Parameters instances are written using the parameter set name as the group name, with
     * whitespace replaced with underscores. The name-value pairs are written individually using the
     * standard PrimitiveHdf5Array approach.
     */
    @Override
    public List<Long> write(long fieldGroupId, String fieldName) {
        List<Long> subGroupIds = new ArrayList<>();

        // The contents of this object are guaranteed to be a scalar AlgorithmParameters instance,
        // which in turn is guaranteed to contain a Map of ParameterSet instances by parameter
        // set name.
        AlgorithmParameters parametersObject = (AlgorithmParameters) arrayObject;

        int parameterSetGroupCounter = 0;
        long parameterSetGroupId;
        for (Map.Entry<String, ParameterSet> entry : parametersObject.getParameterSetsByName()
            .entrySet()) {

            List<Long> parameterSetSubgroups = writeParameterSet(fieldGroupId, entry.getValue());
            parameterSetGroupId = parameterSetSubgroups.get(0);

            writeFieldOrderAttribute(parameterSetGroupId, parameterSetGroupCounter);
            parameterSetGroupCounter++;
            H5.H5Gclose(parameterSetGroupId);
        }
        return subGroupIds;
    }

    private List<Long> writeParameterSet(long fieldGroupId, ParameterSet parameterSet) {
        List<Long> subgroupIds = new ArrayList<>();
        String originalGroupName = parameterSet.getParameterSetNameOrAlgorithmInterfaceName();

        // We need a group name that is acceptable as a variable name in all the
        // languages Ziggy supports; get that by replacing spaces in the parameter
        // set name with underscores
        String groupName = originalGroupName.replace(" ", "_");

        // create the parent group and put it at the top of the list of sub-group IDs
        long parametersInstanceGroupId = H5.H5Gcreate(fieldGroupId, groupName, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
        subgroupIds.add(parametersInstanceGroupId);

        // write the original parameter set name so it can be recovered later
        writeStringAttribute(parametersInstanceGroupId,
            Hdf5AlgorithmInterface.PARAMETER_SET_ORIGINAL_NAME_ATT_NAME, parameterSet.getName());
        if (parameterSet.getAlgorithmInterfaceName() != null) {
            writeStringAttribute(parametersInstanceGroupId,
                Hdf5AlgorithmInterface.PARAMETER_SET_ALGORITHM_INTERFACE_NAME_ATT_NAME,
                parameterSet.getAlgorithmInterfaceName());
        }
        // loop over parameters and, for each one, convert it to a name-value pair and
        // write it as a primitive array subgroup
        int parameterGroupCounter = 0;
        for (Parameter parameter : parameterSet.getParameters()) {
            subgroupIds.addAll(
                writeParameter(parametersInstanceGroupId, parameter, parameterGroupCounter));
            parameterGroupCounter++;
        }
        return subgroupIds;
    }

    /**
     * Write a single {@link Parameter} instance to HDF5 as an array in a group.
     */
    private List<Long> writeParameter(long parametersInstanceGroupId, Parameter parameter,
        int parameterGroupCounter) {
        List<Long> subGroupIds = new ArrayList<>();

        // Like the name of the parameter set, the parameter names must also have their
        // whitespace replaced.
        String parName = parameter.getName().replace(" ", "_");
        Object parValue = parameter.getValue();
        AbstractHdf5Array parArray = AbstractHdf5Array.newInstance(parValue);
        parArray.setCreateGroupsForMissingFields(createGroupsForMissingFields);
        parArray.setFieldName(parName);
        long typedParGroupId = H5.H5Gcreate(parametersInstanceGroupId, parName, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
        subGroupIds.addAll(parArray.write(typedParGroupId, parName));
        Hdf5AlgorithmInterface.writeDataTypeAttribute(typedParGroupId, parArray.getDataTypeToSave(),
            parName);
        writeFieldOrderAttribute(typedParGroupId, parameterGroupCounter);
        if (parameter.isScalar()) {
            long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            long scalarParameterAttribute = H5.H5Acreate(typedParGroupId,
                Hdf5AlgorithmInterface.SCALAR_PARAMETER_ATT_NAME, ZIGGY_BYTE.getHdf5Type(),
                scalarSpace, H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Aclose(scalarParameterAttribute);
            H5.H5Sclose(scalarSpace);
        }
        H5.H5Gclose(typedParGroupId);
        subGroupIds.add(typedParGroupId);
        return subGroupIds;
    }

    /**
     * Reads the contents of a AlgorithmParameters instance from HDF5.
     */
    @Override
    public void read(long fieldGroupId) {

        AlgorithmParameters algorithmParameters = new AlgorithmParameters();

        // get all the groups under the field group
        Set<String> parameterSetNames = getSubgroupNames(fieldGroupId);
        if (parameterSetNames.isEmpty()) {
            return;
        }

        for (String parameterSetName : parameterSetNames) {
            long parameterSetGroupId = H5.H5Gopen(fieldGroupId, parameterSetName, H5P_DEFAULT);
            Set<Parameter> parameters = new HashSet<>();
            ParameterSet parameterSet = new ParameterSet();
            parameterSet.setName(readStringAttribute(parameterSetGroupId,
                Hdf5AlgorithmInterface.PARAMETER_SET_ORIGINAL_NAME_ATT_NAME));
            parameterSet.setAlgorithmInterfaceName(readStringAttribute(parameterSetGroupId,
                Hdf5AlgorithmInterface.PARAMETER_SET_ALGORITHM_INTERFACE_NAME_ATT_NAME));
            Set<String> parameterNames = getSubgroupNames(parameterSetGroupId);
            for (String parameterName : parameterNames) {
                long parameterGroupId = H5.H5Gopen(parameterSetGroupId, parameterName, H5P_DEFAULT);
                PrimitiveHdf5Array primitiveHdf5Array = PrimitiveHdf5Array
                    .forReadingParameters(parameterGroupId, parameterName);
                primitiveHdf5Array.read(parameterGroupId);
                boolean scalar = H5.H5Aexists(parameterGroupId,
                    Hdf5AlgorithmInterface.SCALAR_PARAMETER_ATT_NAME);
                H5.H5Gclose(parameterGroupId);
                Object javaArray = primitiveHdf5Array.toJava();
                parameters.add(toParameter(javaArray, parameterName, scalar));
            }
            parameterSet.setParameters(parameters);
            algorithmParameters.getParameterSetsByName().put(parameterSet.getName(), parameterSet);
            H5.H5Gclose(parameterSetGroupId);
        }
        setArray(algorithmParameters);
    }

    /**
     * Obtains the subgroups of a specified group.
     *
     * @param fieldGroupId ID of HDF5 group to examine.
     * @return Map between HDF5 group ID and group name, for all subgroups of fieldGroupId.
     */
    Set<String> getSubgroupNames(long fieldGroupId) {
        Set<String> subGroupNames = new HashSet<>();
        int groupCount = (int) H5.H5Gn_members(fieldGroupId, ".");
        if (groupCount > 0) {
            String[] oname = new String[groupCount];
            int[] otype = new int[groupCount];
            int[] ltype = new int[groupCount];
            H5O_token_t[] orefs = new H5O_token_t[groupCount];
            H5.H5Gget_obj_info_all(fieldGroupId, ".", oname, otype, ltype, orefs,
                HDF5Constants.H5_INDEX_NAME);
            for (int i = 0; i < groupCount; i++) {
                if (otype[i] == 0) {
                    subGroupNames.add(oname[i]);
                }
            }
        }
        return subGroupNames;
    }

    /**
     * Converts a Java array from a PrimitiveHdf5Array to a {@link Parameter}.
     *
     * @param arrayObject Array contents of a PrimitiveHdf5Array.
     * @param name Parameter name.
     * @return A TypedProperty in which the contents of the arrayObject are converted to a String
     * and the data type is set based on the type of the arrayObject's contents.
     */
    Parameter toParameter(Object arrayObject, String name, boolean scalar) {

        return new Parameter(name, ZiggyArrayUtils.arrayToString(arrayObject),
            ZiggyDataType.getDataType(arrayObject), scalar);
    }

    /**
     * Data type to save. For this particular class, it's always Persistable.
     */
    @Override
    public ZiggyDataType getDataTypeToSave() {
        return ZiggyDataType.ZIGGY_PERSISTABLE;
    }
}
