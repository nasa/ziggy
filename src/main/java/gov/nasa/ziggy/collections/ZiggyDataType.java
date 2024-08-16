package gov.nasa.ziggy.collections;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.primitives.Primitives;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import hdf.hdf5lib.HDF5Constants;

/**
 * Provides information about array data types used in Ziggy, including relationship to HDF5 data
 * types. Objects of the {@link ZiggyDataType} class can also be used to record the data type of
 * objects, construct arrays in the {@link ZiggyArrayUtils} class, etc.
 *
 * @see <a href="https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-4.html#jvms-4.3.2-200">
 * List of Java field type characters. </a>
 * @author PT
 */

public enum ZiggyDataType {

    ZIGGY_BOOLEAN(boolean.class, "Z", EnumSet.of(Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.BOOLEAN_TYPE_INT, ZiggyDataTypeConstants.BOOLEAN_DUMMY_TYPE,
        "H5T_NATIVE_INT8") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Boolean.valueOf(((boolean[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((boolean[]) array1d)[location] = (Boolean) arrayMember;
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((boolean[]) array1d, (Boolean) fillValue);
        }

        @Override
        public Object boxedZero() {
            return Boolean.valueOf(false);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Boolean[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((boolean[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] source, Object destinationArray) {
            throw new UnsupportedOperationException("Cannot cast numeric array to boolean array");
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Boolean.toString(((boolean[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Boolean) obj).toString() : "false";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? Boolean.valueOf(false) : Boolean.valueOf(value);
        }
    },
    ZIGGY_BYTE(byte.class, "B", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.BYTE_TYPE_INT, HDF5Constants.H5T_NATIVE_INT8, "H5T_NATIVE_INT8") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Byte.valueOf(((byte[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((byte[]) array1d)[location] = ((Number) arrayMember).byteValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((byte[]) array1d, ((Number) fillValue).byteValue());
        }

        @Override
        public Object boxedZero() {
            return Byte.valueOf((byte) 0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Byte[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((byte[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].byteValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Byte.toString(((byte[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Byte) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Byte.parseByte(value);
        }
    },
    ZIGGY_SHORT(short.class, "S", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.SHORT_TYPE_INT, HDF5Constants.H5T_NATIVE_INT16, "H5T_NATIVE_INT16") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Short.valueOf(((short[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((short[]) array1d)[location] = ((Number) arrayMember).shortValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((short[]) array1d, ((Number) fillValue).shortValue());
        }

        @Override
        public Object boxedZero() {
            return Short.valueOf((short) 0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Short[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((short[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].shortValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Short.toString(((short[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Short) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Short.parseShort(value);
        }
    },
    ZIGGY_INT(int.class, "I", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.INT_TYPE_INT, HDF5Constants.H5T_NATIVE_INT32, "H5T_NATIVE_INT32") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Integer.valueOf(((int[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((int[]) array1d)[location] = ((Number) arrayMember).intValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((int[]) array1d, ((Number) fillValue).intValue());
        }

        @Override
        public Object boxedZero() {
            return Integer.valueOf(0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Integer[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((int[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].intValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Integer.toString(((int[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Integer) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Integer.parseInt(value);
        }
    },
    ZIGGY_LONG(long.class, "J", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.LONG_TYPE_INT, HDF5Constants.H5T_NATIVE_INT64, "H5T_NATIVE_INT64") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Long.valueOf(((long[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((long[]) array1d)[location] = ((Number) arrayMember).longValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((long[]) array1d, ((Number) fillValue).longValue());
        }

        @Override
        public Object boxedZero() {
            return Long.valueOf(0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Long[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((long[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].longValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Long.toString(((long[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Long) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Long.parseLong(value);
        }
    },
    ZIGGY_FLOAT(float.class, "F", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.FLOAT_TYPE_INT, HDF5Constants.H5T_NATIVE_FLOAT, "H5T_NATIVE_FLOAT") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Float.valueOf(((float[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((float[]) array1d)[location] = ((Number) arrayMember).floatValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((float[]) array1d, ((Number) fillValue).floatValue());
        }

        @Override
        public Object boxedZero() {
            return Float.valueOf(0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Float[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((float[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].floatValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Float.toString(((float[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Float) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Float.parseFloat(value);
        }
    },
    ZIGGY_DOUBLE(double.class, "D", EnumSet.of(Attribute.NUMERIC, Attribute.PARAMETRIC),
        ZiggyDataTypeConstants.DOUBLE_TYPE_INT, HDF5Constants.H5T_NATIVE_DOUBLE,
        "H5T_NATIVE_DOUBLE") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Double.valueOf(((double[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((double[]) array1d)[location] = ((Number) arrayMember).doubleValue();
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((double[]) array1d, ((Number) fillValue).doubleValue());
        }

        @Override
        public Object boxedZero() {
            return Double.valueOf(0);
        }

        @Override
        protected Object unboxArray(Object array1d) {
            return ArrayUtils.toPrimitive((Double[]) array1d);
        }

        @Override
        protected Object boxArray(Object array1d) {
            return ArrayUtils.toObject((double[]) array1d);
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            for (int i = 0; i < sourceArray.length; i++) {
                setArrayMember(sourceArray[i].doubleValue(), destinationArray, i);
            }
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return Double.toString(((double[]) array1d)[index]);
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? ((Double) obj).toString() : "0";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? 0 : Double.parseDouble(value);
        }
    },
    ZIGGY_STRING(String.class, "String", "T", java.lang.String.class,
        EnumSet.of(Attribute.PARAMETRIC), ZiggyDataTypeConstants.STRING_TYPE_INT,
        HDF5Constants.H5T_C_S1, "H5T_C_S1") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return ((String[]) array1d)[location];
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((String[]) array1d)[location] = (String) arrayMember;
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((String[]) array1d, fillValue.toString());
        }

        @Override
        public Object boxedZero() {
            throw new UnsupportedOperationException("Strings do not support boxed zeros");
        }

        @Override
        protected Object unboxArray(Object array1d) {
            throw new UnsupportedOperationException("Strings cannot be unboxed");
        }

        @Override
        protected Object boxArray(Object array1d) {
            throw new UnsupportedOperationException("Strings cannot be boxed");
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            throw new UnsupportedOperationException("Cannot cast numeric array to string array");
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            return ((String[]) array1d)[index];
        }

        @Override
        public String scalarToString(Object obj) {
            return obj != null ? (String) obj : "";
        }

        @Override
        public Object typedValue(String value) {
            return StringUtils.isBlank(value) ? "" : value.trim();
        }
    },
    ZIGGY_PERSISTABLE(Persistable.class, "gov.nasa.ziggy.common.persistable.Persistable", "P",
        Persistable.class, new HashSet<>(), ZiggyDataTypeConstants.PERSISTABLE_TYPE_INT,
        HDF5Constants.H5T_OPAQUE, "H5T_OPAQUE") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return ((Object[]) array1d)[location];
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((Object[]) array1d)[location] = arrayMember;
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            throw new UnsupportedOperationException("Cannot fill Persistable array");
        }

        @Override
        public Object boxedZero() {
            throw new UnsupportedOperationException("Persistables do not support boxed zeros");
        }

        @Override
        protected Object unboxArray(Object array1d) {
            throw new UnsupportedOperationException("Persistables cannot be unboxed");
        }

        @Override
        protected Object boxArray(Object array1d) {
            throw new UnsupportedOperationException("Persistables cannot be boxed");
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            throw new UnsupportedOperationException(
                "Cannot cast numeric array to Persistable array");
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            throw new UnsupportedOperationException(
                "Cannot convert persistable array member to string");
        }

        @Override
        public String scalarToString(Object obj) {
            throw new UnsupportedOperationException("Cannot convert persistable object to string");
        }

        @Override
        public Object typedValue(String value) {
            throw new UnsupportedOperationException("Cannot convert string to Persistable value");
        }
    },
    ZIGGY_ENUM(Enum.class, "E", new HashSet<>(), ZiggyDataTypeConstants.ENUM_TYPE_INT,
        ZiggyDataTypeConstants.ENUM_DUMMY_TYPE, "H5T_C_S1") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return ((Enum<?>[]) array1d)[location];
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((Enum<?>[]) array1d)[location] = (Enum<?>) arrayMember;
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            throw new UnsupportedOperationException("Cannot fill Enum array");
        }

        @Override
        public Object boxedZero() {
            throw new UnsupportedOperationException("Enums do not support boxed zeros");
        }

        @Override
        protected Object unboxArray(Object array1d) {
            throw new UnsupportedOperationException("Enums cannot be unboxed");
        }

        @Override
        protected Object boxArray(Object array1d) {
            throw new UnsupportedOperationException("Enums cannot be boxed");
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            throw new UnsupportedOperationException("Cannot cast numeric array to Enum array");
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            throw new UnsupportedOperationException("Cannot convert Enum array member to string");
        }

        @Override
        public String scalarToString(Object obj) {
            throw new UnsupportedOperationException("Cannot convert Enum object to string");
        }

        @Override
        public Object typedValue(String value) {
            throw new UnsupportedOperationException("Cannot convert string to Enum value");
        }
    },
    ZIGGY_CHAR(char.class, "C", new HashSet<>(), ZiggyDataTypeConstants.CHAR_TYPE_INT,
        HDF5Constants.H5T_NATIVE_INT16, "H5T_NATIVE_INT16") {

        @Override
        protected Object getArrayMember(Object array1d, int location) {
            return Character.valueOf(((char[]) array1d)[location]);
        }

        @Override
        public void setArrayMember(Object arrayMember, Object array1d, int location) {
            ((char[]) array1d)[location] = (Character) arrayMember;
        }

        @Override
        protected void fillArray(Object array1d, Object fillValue) {
            Arrays.fill((char[]) array1d, (Character) fillValue);
        }

        @Override
        public Object boxedZero() {
            throw new UnsupportedOperationException("Chars do not support boxed zeros");
        }

        @Override
        protected Object unboxArray(Object array1d) {
            throw new UnsupportedOperationException("Chars cannot be unboxed");
        }

        @Override
        protected Object boxArray(Object array1d) {
            throw new UnsupportedOperationException("Chars cannot be boxed");
        }

        @Override
        protected void unboxAndCastNumericToNumeric(Number[] sourceArray, Object destinationArray) {
            throw new UnsupportedOperationException("Cannot cast numeric array to char array");
        }

        @Override
        public String array1dMemberToString(Object array1d, int index) {
            throw new UnsupportedOperationException("Cannot convert char array member to string");
        }

        @Override
        public String scalarToString(Object obj) {
            throw new UnsupportedOperationException("Cannot convert char to string");
        }

        @Override
        public Object typedValue(String value) {
            throw new UnsupportedOperationException("Cannot convert string to char value");
        }
    };

    private enum Attribute {
        NUMERIC, PARAMETRIC;
    }

    private final int attributeTypeInt;
    private final Class<?> javaClass;
    private final String javaClassName;
    private final String javaTypeName;
    private final String javaTypeCharacter;
    private final Class<?> javaBoxedClass;
    private final String javaBoxedClassName;
    private final long hdf5Type;
    private final String hdf5TypeName;
    private final Set<Attribute> attributes;

    ZiggyDataType(Class<?> javaClass, String javaTypeCharacter, Set<Attribute> attributes,
        int attributeTypeInt, long hdf5Type, String hdf5TypeName) {
        this(javaClass, null, javaTypeCharacter, null, attributes, attributeTypeInt, hdf5Type,
            hdf5TypeName);
    }

    ZiggyDataType(Class<?> javaClass, String javaTypeName, String javaTypeCharacter,
        Class<?> javaBoxedClass, Set<Attribute> attributes, int attributeTypeInt, long hdf5Type,
        String hdf5TypeName) {
        this.javaClass = javaClass;
        javaClassName = javaClass.getName() + ".class";
        if (javaTypeName != null) {
            this.javaTypeName = javaTypeName;
        } else {
            this.javaTypeName = javaClass.getName();
        }
        this.javaTypeCharacter = javaTypeCharacter;
        if (javaBoxedClass != null) {
            this.javaBoxedClass = javaBoxedClass;
        } else {
            this.javaBoxedClass = Primitives.wrap(javaClass);
        }
        javaBoxedClassName = this.javaBoxedClass.getName();
        this.attributes = attributes;
        this.attributeTypeInt = attributeTypeInt;
        this.hdf5Type = hdf5Type;
        this.hdf5TypeName = hdf5TypeName;
    }

    protected abstract Object getArrayMember(Object array1d, int location);

    public abstract void setArrayMember(Object arrayMember, Object array1d, int location);

    protected abstract void fillArray(Object array1d, Object fillValue);

    public abstract Object boxedZero();

    protected abstract Object unboxArray(Object array1d);

    protected abstract Object boxArray(Object array1d);

    protected abstract void unboxAndCastNumericToNumeric(Number[] sourceArray,
        Object destinationArray);

    public abstract String array1dMemberToString(Object array1d, int index);

    public abstract String scalarToString(Object obj);

    public abstract Object typedValue(String value);

    public String getJavaTypeCharacter() {
        return javaTypeCharacter;
    }

    public String getJavaClassName() {
        return javaClassName;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    public Class<?> getJavaBoxedClass() {
        return javaBoxedClass;
    }

    public String getJavaBoxedClassName() {
        return javaBoxedClassName;
    }

    public long getHdf5Type() {
        return hdf5Type;
    }

    public String getHdf5TypeName() {
        return hdf5TypeName;
    }

    public int getAttributeTypeInt() {
        return attributeTypeInt;
    }

    public String getJavaTypeName() {
        return javaTypeName;
    }

    public static ZiggyDataType getDataTypeFromAttributeTypeInt(int attributeTypeInt) {
        for (ZiggyDataType dataType : ZiggyDataType.values()) {
            if (dataType.attributeTypeInt == attributeTypeInt) {
                return dataType;
            }
        }
        return null;
    }

    public static ZiggyDataType getDataTypeFromClassSimpleName(String classSimpleName) {
        return getDataTypeFromClassSimpleName(classSimpleName, true);
    }

    public static ZiggyDataType getDataTypeFromClassSimpleName(String classSimpleName,
        boolean caseSensitive) {
        String className = classSimpleName;
        if (!caseSensitive) {
            className = classSimpleName.toLowerCase();
        }
        for (ZiggyDataType dataType : ZiggyDataType.values()) {
            String javaTypeName = dataType.javaTypeName;
            if (!caseSensitive) {
                javaTypeName = dataType.javaTypeName.toLowerCase();
            }
            if (javaTypeName.equals(className)) {
                return dataType;
            }
        }
        return null;
    }

    /**
     * A more general converter from string to {@link ZiggyDataType}. This method first attempts to
     * match the class simple name; if that fails, it attempts to match the name of the enumeration
     * element. Both of these are performed in a case-insensitive manner.
     */
    public static ZiggyDataType getDataTypeFromString(String className) {
        ZiggyDataType dataType = getDataTypeFromClassSimpleName(className, false);
        if (dataType == null) {
            dataType = ZiggyDataType.valueOf(className.toUpperCase());
        }
        return dataType;
    }

    // primitives and primitive arrays cannot be cast to Object, so...
    public static ZiggyDataType getDataType(boolean value) {
        return ZIGGY_BOOLEAN;
    }

    public static ZiggyDataType getDataType(boolean[] value) {
        return ZIGGY_BOOLEAN;
    }

    public static ZiggyDataType getDataType(byte value) {
        return ZIGGY_BYTE;
    }

    public static ZiggyDataType getDataType(byte[] value) {
        return ZIGGY_BYTE;
    }

    public static ZiggyDataType getDataType(short value) {
        return ZIGGY_SHORT;
    }

    public static ZiggyDataType getDataType(short[] value) {
        return ZIGGY_SHORT;
    }

    public static ZiggyDataType getDataType(int value) {
        return ZIGGY_INT;
    }

    public static ZiggyDataType getDataType(int[] value) {
        return ZIGGY_INT;
    }

    public static ZiggyDataType getDataType(long value) {
        return ZIGGY_LONG;
    }

    public static ZiggyDataType getDataType(long[] value) {
        return ZIGGY_LONG;
    }

    public static ZiggyDataType getDataType(float value) {
        return ZIGGY_FLOAT;
    }

    public static ZiggyDataType getDataType(float[] value) {
        return ZIGGY_FLOAT;
    }

    public static ZiggyDataType getDataType(double value) {
        return ZIGGY_DOUBLE;
    }

    public static ZiggyDataType getDataType(double[] value) {
        return ZIGGY_DOUBLE;
    }

    /**
     * Returns the ZiggyDataType object that corresponds to the class of the argument.
     *
     * @param object Data object. This can be scalar, array, or list of primitive types, boxed
     * types, String, Enum, or Persistable.
     * @return ZiggyDataType corresponding to the underlying data class in the argument, or null for
     * the case of an empty list.
     * @throws PipelineException if called with an object of non-valid class (i.e., an object in a
     * class that does not implement Persistable).
     */
    @SuppressWarnings("unchecked")
    public static ZiggyDataType getDataType(Object object) {
        ZiggyDataType hdf5Type = null;
        if (object == null) {
            return hdf5Type;
        }
        if (object instanceof List) {
            hdf5Type = getDataTypeFromList((List<? extends Object>) object);
        } else {
            hdf5Type = getDataTypeFromClass(object.getClass());
        }
        return hdf5Type;
    }

    private static ZiggyDataType getDataTypeFromList(List<? extends Object> object) {
        ZiggyDataType hdf5Type = null;
        if (object != null && object.size() != 0) {
            hdf5Type = getDataTypeFromClass(object.get(0).getClass());
        }
        return hdf5Type;
    }

    public static List<ZiggyDataType> parametricTypes() {
        List<ZiggyDataType> parametricTypes = new ArrayList<>();
        for (ZiggyDataType dataType : ZiggyDataType.values()) {
            if (dataType.attributes.contains(Attribute.PARAMETRIC)) {
                parametricTypes.add(dataType);
            }
        }
        return parametricTypes;
    }

    /**
     * Get the data type associated with a particular Java class.
     *
     * @param clazz
     * @return ZiggyDataType associated with the class.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static ZiggyDataType getDataTypeFromClass(Class<?> clazz) {
        ZiggyDataType hdf5Type = null;
        String simpleName = clazz.getSimpleName().toLowerCase();
        int bracketLocation = simpleName.indexOf("[");
        if (bracketLocation > 0) {
            simpleName = simpleName.substring(0, bracketLocation);
        }
        for (ZiggyDataType dataType : ZiggyDataType.values()) {
            if (simpleName.equals(dataType.javaTypeName.toLowerCase())) {
                return dataType;
            }
        }

        if (simpleName.equals("integer")) {
            return ZiggyDataType.ZIGGY_INT;
        }

        // for enums and Persistables, something slightly slicker is needed
        try {
            String className = truncateClassName(clazz.getName());
            Class<?> elementalClass = Class.forName(className);
            if (Enum.class.isAssignableFrom(elementalClass)) {
                hdf5Type = ZIGGY_ENUM;
            } else if (Persistable.class.isAssignableFrom(elementalClass)) {
                hdf5Type = ZIGGY_PERSISTABLE;
            }
        } catch (ClassNotFoundException e) {
            // This can never occur. The class name comes from an actual
            // instance of the class, so by definition it can't be "not
            // found."
            throw new AssertionError(e);
        }

        // if it's still null, then this isn't something we can persist...

        if (hdf5Type == null) {
            throw new PipelineException(
                "Unable to determine data type of object with class " + clazz.getName());
        }
        return hdf5Type;
    }

    /**
     * Strips the "L" and "[" leading characters, and ";" trailing character, off a class name.
     */
    public static String truncateClassName(String fullClassName) {
        while (fullClassName.startsWith("[") || fullClassName.startsWith("L")) {
            fullClassName = fullClassName.substring(1);
        }
        if (fullClassName.endsWith(";")) {
            fullClassName = fullClassName.substring(0, fullClassName.length() - 1);
        }
        return fullClassName;
    }

    /**
     * Determines the ZiggyDataType of a {@link Field}.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static ZiggyDataType getDataType(Field field) {
        field.setAccessible(true);
        Class<?> clazz = null;
        Class<?> fieldClass = field.getType();
        try {
            if (fieldClass.getName().equals("java.util.List")) {
                Type type = field.getGenericType();
                ParameterizedType pType = (ParameterizedType) type;
                String className = pType.getActualTypeArguments()[0].getTypeName();
                clazz = Class.forName(className);
            } else {
                clazz = fieldClass;
            }
            return getDataTypeFromClass(clazz);
        } catch (ClassNotFoundException e) {
            // This can never occur. The class name comes from an actual
            // instance of the class, so by definition it can't be "not
            // found."
            throw new AssertionError(e);
        }
    }

    /**
     * Returns the contents of a {@link Field}, as a {@link String}. Assumes that the type of the
     * {@link Field} is one of the {@link ZiggyDataType} types, or a 1-dimensional array of same.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static String objectToString(Object obj, Field field) {
        field.setAccessible(true);
        try {
            return objectToString(field.get(obj), ZiggyDataType.getDataType(field),
                field.getType().isArray());
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to convert field " + field.getName()
                + " to string on object of class " + obj.getClass().getName(), e);
        }
    }

    public static String objectToString(Object obj) {
        return objectToString(obj, ZiggyDataType.getDataType(obj), obj.getClass().isArray());
    }

    /**
     * Converts an {@link Object} to a {@link String}. The argument must be an instance of one of
     * the {@link ZiggyDataType} types, or an array of same. If the object is an array, a string
     * containing a comma-separated list of the array's values will be returned.
     */
    public static String objectToString(Object obj, ZiggyDataType dataType, boolean isArray) {

        if (isArray) {
            return arrayToString(obj, dataType);
        }
        return dataType.scalarToString(obj);
    }

    /**
     * Converts an array of values to a {@link String} in which the values are comma-separated.
     */
    private static String arrayToString(Object arrayObject, ZiggyDataType dataType) {
        StringBuilder sb = new StringBuilder();
        long[] arraySize = arrayObject != null ? ZiggyArrayUtils.getArraySize(arrayObject)
            : new long[] { 0 };
        if (arraySize.length > 1) {
            throw new IllegalArgumentException("Argument to arrayToString must be 1-d array");
        }
        for (int arrayIndex = 0; arrayIndex < arraySize[0]; arrayIndex++) {
            sb.append(dataType.array1dMemberToString(arrayObject, arrayIndex));
            sb.append(",");
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * Sets the value of a {@link Field} in an {@link Object}. The {@param value} argument is
     * converted from a {@link String} to a scalar or 1-d array of values of the appropriate type.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void setField(Object obj, Field field, String value) {
        ZiggyDataType dataType = getDataType(field);
        boolean isArray = field.getType().isArray();
        field.setAccessible(true);
        try {
            field.set(obj, stringToObject(value, dataType, isArray));
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to set field " + field.getName()
                + " in object of class " + obj.getClass().getName(), e);
        }
    }

    /**
     * Converts a {@link String} to a scalar value or an array of values, with the type given by an
     * instance of {@link ZiggyDataType}.
     */
    public static Object stringToObject(String value, ZiggyDataType dataType, boolean isArray) {

        if (!isArray) {
            return dataType.typedValue(value);
        }
        return ZiggyArrayUtils.stringToArray(value, dataType);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static int elementSizeBytes(ZiggyDataType dataType) {
        int elementSize = 1;
        Class<?> boxedClass = dataType.javaBoxedClass;
        for (Field field : boxedClass.getFields()) {
            try {
                if (field.getName().contentEquals("BYTES")) {
                    elementSize = field.getInt(null);
                    break;
                }
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to get size of field " + field.getName(),
                    e);
            }
        }
        return elementSize;
    }

    public boolean isNumeric() {
        return attributes.contains(Attribute.NUMERIC);
    }

    public static Object unbox1dArray(Object boxedArray) {
        return getDataType(boxedArray).unboxArray(boxedArray);
    }

    public static Object box1dArray(Object boxedArray) {
        return getDataType(boxedArray).boxArray(boxedArray);
    }

    public static void castBoxedNumericToUnboxedNumeric(Number[] sourceArray,
        Object destinationArray) {
        getDataType(destinationArray).unboxAndCastNumericToNumeric(sourceArray, destinationArray);
    }

    public static void fill1dArray(Object array1d, Object fillValue) {
        getDataType(array1d).fillArray(array1d, fillValue);
    }

    public static void set1dArrayMember(Object arrayMember, Object array1d, int location) {
        getDataType(array1d).setArrayMember(arrayMember, array1d, location);
    }

    public static Object get1dArrayMember(Object array1d, int location) {
        return getDataType(array1d).getArrayMember(array1d, location);
    }

    public static class ZiggyDataTypeConstants {

        // We need unique HDF5 type ints for some types that aren't actually used in our HDF5
        // code. To avoid conflicts with actual HDF5 type ints, we will use negatives for this
        private static final long BOOLEAN_DUMMY_TYPE = -1;
        private static final long ENUM_DUMMY_TYPE = -2;

        // We can't rely upon the integers provided by HDF5 to be the same from one system to
        // another, so we need to provide our own set of HDF5 type numbers that can be put into
        // an attribute by one application and read by another

        public static final int BOOLEAN_TYPE_INT = 1;
        public static final int BYTE_TYPE_INT = 2;
        public static final int SHORT_TYPE_INT = 3;
        public static final int INT_TYPE_INT = 4;
        public static final int LONG_TYPE_INT = 5;
        public static final int FLOAT_TYPE_INT = 6;
        public static final int DOUBLE_TYPE_INT = 7;
        public static final int STRING_TYPE_INT = 8;
        public static final int PERSISTABLE_TYPE_INT = 9;
        public static final int ENUM_TYPE_INT = 10;
        public static final int CHAR_TYPE_INT = 11;
    }
}
