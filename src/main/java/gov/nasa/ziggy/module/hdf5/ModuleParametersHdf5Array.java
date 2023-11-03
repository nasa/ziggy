package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_BYTE;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.parameters.ModuleParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5O_token_t;

/**
 * HDF5 interface for the special case of a ModuleParameters scalar instance. This is required
 * because the module parameters require two different flavors of special handling:
 * <ol>
 * <li>For typical Parameters instances that have a purpose-built Parameters subclass that provides
 * parameter definitions via its members ("classed parameter sets"), the parameter set name has to
 * be set. Ordinarily any Parameters class would be a scalar Persistable member of a class and so
 * the member name could be used, but ModuleParameters contains a List of Parameters instances
 * without any such member names.
 * <li>Members that are Parameters instances require the name management but also need to translate
 * from the name-value-type TypedProperty instances stored in the Parameters instance to HDF5 groups
 * and datasets formatted the way that the end-users expect them.
 * </ol>
 * Note that the Parameters classes must also store their fully-qualified class names in the HDF5
 * file, because the List&#60;Parameters&#62; in a Persistable instance doesn't tell HDF5 how to
 * deserialize parameters into an appropriate Parameters subclass. This infrastructure is provided
 * in the PersistableHdf5Array class.
 *
 * @author PT
 */
public class ModuleParametersHdf5Array extends AbstractHdf5Array {

    /**
     * Constructor based on a Field of a Persistable instance, used during HDF5 read operaions.
     * <p>
     * In this case, there is no special handling required over and above the superclass
     * constructor, as the field is guaranteed to be a scalar ModuleParameters field.
     */
    ModuleParametersHdf5Array(Field field) {
        super(field);
    }

    /**
     * Constructor based on an object, used during HDF5 write operations.
     * <p>
     * In this case, there is no special handling required over and above the superclass
     * constructor, as the object is guaranteed to be a scalar ModuleParameters instance.
     */
    ModuleParametersHdf5Array(Object object) {
        super(object);
    }

    /**
     * Sets the instance's Java object.
     * <p>
     * In this case, there is no special handling required because the object is guaranteed to be a
     * scalar ModuleParameters instance.
     */
    @Override
    public void setArray(Object arrayObject) {
        this.arrayObject = arrayObject;
    }

    /**
     * Returns the contents of the instance in a form ready to be inserted into the parent
     * Persistable object.
     * <p>
     * In this case, the contents of the instance are guaranteed to be a scalar ModuleParameters
     * instance and thus require no post-processing to be stored in the parent instance.
     */
    @Override
    public Object toJava() {
        return arrayObject;
    }

    /**
     * Writes the contents of the ModuleParameters to HDF5.
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

        // The contents of this object are guaranteed to be a scalar ModuleParameters instance,
        // which in turn is guaranteed to contain a List of instances of Parameters subclasses,
        // each of which is itself a scalar.

        int parameterInstanceGroupCounter = 0;
        long parametersInstanceGroupId;
        ModuleParameters parametersObject = (ModuleParameters) arrayObject;
        for (ParametersInterface parametersInstance : parametersObject.getModuleParameters()) {

            // first the easy case: a classed Parameters instance, all that needs to happen
            // is that the field name needs to be changed to the Java class simple name
            if (!parametersInstance.getClass().equals(Parameters.class)) {
                AbstractHdf5Array hdf5Array = AbstractHdf5Array.newInstance(parametersInstance);
                hdf5Array.setCreateGroupsForMissingFields(createGroupsForMissingFields);
                hdf5Array.setFieldName(parametersInstance.getClass().getSimpleName());
                parametersInstanceGroupId = H5.H5Gcreate(fieldGroupId, hdf5Array.getFieldName(),
                    H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
                subGroupIds.add(parametersInstanceGroupId);
                subGroupIds.addAll(hdf5Array.write(parametersInstanceGroupId, fieldName));
            } else {

                // Parameters:
                List<Long> parametersSubGroups = writeParametersInstance(fieldGroupId,
                    (Parameters) parametersInstance);
                parametersInstanceGroupId = parametersSubGroups.get(0);
                subGroupIds.addAll(parametersSubGroups);
            }

            writeFieldOrderAttribute(parametersInstanceGroupId, parameterInstanceGroupCounter);
            H5.H5Gclose(parametersInstanceGroupId);

            parameterInstanceGroupCounter++;
        }

        return subGroupIds;
    }

    /**
     * Write an instance of Parameters to HDF5. The instance is written in a way that will appear to
     * an algorithm user as identical in structure to the way that a classed Parameters instance
     * does (i.e., a collection of array groups, with 1 parameter per group).
     */
    private List<Long> writeParametersInstance(long fieldGroupId, Parameters parameters) {
        List<Long> subGroupIds = new ArrayList<>();
        String parameterSetName = parameters.getName();

        // We need a group name that is acceptable as a variable name in all the
        // languages Ziggy supports; get that by replacing spaces in the parameter
        // set name with underscores
        String groupName = parameterSetName.replace(" ", "_");

        // create the parent group and put it at the top of the list of sub-group IDs
        long parametersInstanceGroupId = H5.H5Gcreate(fieldGroupId, groupName, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
        subGroupIds.add(parametersInstanceGroupId);

        // write the original parameter set name so it can be recovered later
        writeStringAttribute(parametersInstanceGroupId,
            Hdf5ModuleInterface.PARAMETER_SET_ORIGINAL_NAME_ATT_NAME, parameterSetName);

        // loop over parameters and, for each one, convert it to a name-value pair and
        // write it as a primitive array subgroup
        int parameterGroupCounter = 0;
        for (TypedParameter parameter : parameters.getParameters()) {
            subGroupIds.addAll(
                writeTypedProperty(parametersInstanceGroupId, parameter, parameterGroupCounter));
            parameterGroupCounter++;
        }
        return subGroupIds;
    }

    /**
     * Write a single TypedProperty instance to HDF5 as an array in a group.
     */
    private List<Long> writeTypedProperty(long parametersInstanceGroupId,
        TypedParameter typedProperty, int parameterGroupCounter) {
        List<Long> subGroupIds = new ArrayList<>();

        // Like the name of the parameter set, the parameter names must also have their
        // whitespace replaced.
        String parName = typedProperty.getName().replace(" ", "_");
        Object parValue = typedProperty.getValueAsArray();
        AbstractHdf5Array parArray = AbstractHdf5Array.newInstance(parValue);
        parArray.setCreateGroupsForMissingFields(createGroupsForMissingFields);
        parArray.setFieldName(parName);
        long typedParGroupId = H5.H5Gcreate(parametersInstanceGroupId, parName, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
        subGroupIds.addAll(parArray.write(typedParGroupId, parName));
        Hdf5ModuleInterface.writeDataTypeAttribute(typedParGroupId, parArray.getDataTypeToSave(),
            parName);
        writeFieldOrderAttribute(typedParGroupId, parameterGroupCounter);
        if (typedProperty.isScalar()) {
            long scalarSpace = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            long scalarParameterAttribute = H5.H5Acreate(typedParGroupId,
                Hdf5ModuleInterface.SCALAR_PARAMETER_ATT_NAME, ZIGGY_BYTE.getHdf5Type(),
                scalarSpace, H5P_DEFAULT, H5P_DEFAULT);
            H5.H5Aclose(scalarParameterAttribute);
            H5.H5Sclose(scalarSpace);
        }
        H5.H5Gclose(typedParGroupId);
        subGroupIds.add(typedParGroupId);
        return subGroupIds;
    }

    /**
     * Reads the contents of a ModuleParameters instance from HDF5.
     */
    @Override
    public void read(long fieldGroupId) {

        ModuleParameters moduleParameters = new ModuleParameters();
        List<ParametersInterface> parameterSets = new ArrayList<>();

        // get all the groups under the field group
        Set<String> parameterSetNames = getSubgroupNames(fieldGroupId);
        if (parameterSetNames.isEmpty()) {
            return;
        }

        for (String parameterSetName : parameterSetNames) {

            long parameterSetGroupId = H5.H5Gopen(fieldGroupId, parameterSetName, H5P_DEFAULT);
            // check for an original parameter set name, which indicates a Parameters
            // instance
            if (!H5.H5Aexists(parameterSetGroupId,
                Hdf5ModuleInterface.PARAMETER_SET_ORIGINAL_NAME_ATT_NAME)) {

                // We need to instantiate an HDF5 array, but we don't care about the class as long
                // as it's a subclass of Parameters
                PersistableHdf5Array persistableHdf5Array = PersistableHdf5Array
                    .forReadingModuleParameterSet();
                persistableHdf5Array.setAllowMissingFields(allowMissingFields);
                persistableHdf5Array.read(parameterSetGroupId);
                missingFieldsDetected = missingFieldsDetected
                    || persistableHdf5Array.isMissingFieldsDetected();
                parameterSets.add((ParametersInterface) persistableHdf5Array.toJava());
            } else {

                // Parameters instances are more complicated.
                Set<TypedParameter> typedParameters = new HashSet<>();
                Parameters parameters = new Parameters();
                parameters.setName(readStringAttribute(parameterSetGroupId,
                    Hdf5ModuleInterface.PARAMETER_SET_ORIGINAL_NAME_ATT_NAME));
                Set<String> parameterNames = getSubgroupNames(parameterSetGroupId);
                for (String parameterName : parameterNames) {
                    long parameterGroupId = H5.H5Gopen(parameterSetGroupId, parameterName,
                        H5P_DEFAULT);
                    PrimitiveHdf5Array primitiveHdf5Array = PrimitiveHdf5Array
                        .forReadingParameters(parameterGroupId, parameterName);
                    primitiveHdf5Array.read(parameterGroupId);
                    boolean scalar = H5.H5Aexists(parameterGroupId,
                        Hdf5ModuleInterface.SCALAR_PARAMETER_ATT_NAME);
                    H5.H5Gclose(parameterGroupId);
                    Object javaArray = primitiveHdf5Array.toJava();
                    typedParameters.add(toTypedParameter(javaArray, parameterName, scalar));
                }
                parameters.setParameters(typedParameters);
                parameterSets.add(parameters);
                H5.H5Gclose(parameterSetGroupId);
            }
        } // end loop over parameter sets
        moduleParameters.setModuleParameters(parameterSets);
        setArray(moduleParameters);
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
     * Converts a Java array from a PrimitiveHdf5Array to a {@link TypedParameter}.
     *
     * @param arrayObject Array contents of a PrimitiveHdf5Array.
     * @param name Parameter name.
     * @return A TypedProperty in which the contents of the arrayObject are converted to a String
     * and the data type is set based on the type of the arrayObject's contents.
     */
    TypedParameter toTypedParameter(Object arrayObject, String name, boolean scalar) {

        return new TypedParameter(name, ZiggyArrayUtils.arrayToString(arrayObject),
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
