package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Field;
import java.util.Objects;

import com.l2fprod.common.propertysheet.AbstractProperty;
import com.l2fprod.common.propertysheet.PropertySheet;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.util.StringUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

/**
 * Represents a parameter with a name, a value, and a type. The name and value are Strings, the type
 * is an enum that represents all valid parameter types. This allows the correct data type to be
 * stored at serialization time even though the value is stored as a String.
 * <p>
 * The main use case for this class is to store the contents of a {@link Parameters} instance. It is
 * also used in places where a name-value pair needs to be stored or managed (for example in the way
 * that the Parameters subclasses store their contents, or the way that unit of work instances store
 * their contents). In these cases, the property type is stored as {@link String}, because the Java
 * type of the class member already supplies this information.
 * <p>
 * The class extends {@link AbstractProperty} and implements {Property}, both from l2fprod, which
 * adds a bit of complexity to the class. The {@link AbstractProperty} has a private member, value,
 * which is an {@link Object}. In the interest of sanity, that value will always be kept identical
 * with the private {@link String} member {@link stringValue} in this class.
 * <p>
 * An additional complexity is that the {@link PropertySheet} use of this class wants to pass and
 * retrieve values that are in the desired storage class of the object (i.e., ints, floats, etc.),
 * while some other uses want the stored string value. To manage this complexity, the methods
 * {@link getValue} and {@link setValue} will get and set based on values in the desired storage
 * class, while {@link getString} and {@link setString} will get and set the string value.
 *
 * @author PT
 */

@Embeddable
public class TypedParameter extends AbstractProperty implements Comparable<TypedParameter> {

    private static final long serialVersionUID = 20230511L;

    private static final String ARRAY_TYPE_SUFFIX = "array";

    @Column(length = 1000)
    private String name;

    @Column(length = 1000)
    private String stringValue;

    @Enumerated(EnumType.STRING)
    @Column(length = 16)
    private ZiggyDataType dataType;

    // True indicates a single value of the specified type is present;
    // false indicates that a collection of comma-separated values is present.
    @Column
    private boolean scalar;

    // Needed by Hibernate.
    public TypedParameter() {
    }

    /**
     * Standard constructor that takes the contents of the instance as arguments.
     */
    public TypedParameter(String name, String value, ZiggyDataType type) {
        this(name, value, type, true);
    }

    /**
     * Constructor that also allows the caller to specify whether the value is a scalar (the
     * alternative is that it's a 1-d collection of some sort; array, list, etc.).
     */
    public TypedParameter(String name, String value, ZiggyDataType type, boolean scalar) {
        this.name = name;
        stringValue = trimWhitespace(value);
        dataType = type;
        this.scalar = scalar;
        validate();

        // Ensure that, for array arguments, the spacing is deterministic, and also set the
        // AbstractProperty value; also ensure that the handling of the decimal point for
        // floating point values is deterministic.
        setValue(getValue());
    }

    /**
     * Constructs a TypedProperty instance from name, value, and type strings. The type string has
     * the word "array" added as a suffix to a standard type (i.e., "stringarray", "floatarray",
     * etc.) the property is instantiated as a 1-d array of values rather than a single value.
     */
    public TypedParameter(String name, String value, String typeString) {
        this(name, value, ZiggyDataType.getDataTypeFromString(typeString(typeString)),
            isScalar(typeString));
    }

    /**
     * Constructs a TypedProperty instance that has a type of NONE.
     */
    public TypedParameter(String name, String value) {
        this(name, value, ZiggyDataType.ZIGGY_STRING);
    }

    /**
     * Constructs a {@link TypedParameter} instance from the value of a {@link Field} in an
     * {@link Object}. The resulting {@link TypedParameter} will have a type of
     * {@link ZiggyDataType#ZIGGY_STRING}.
     */
    public TypedParameter(Object obj, Field field) {
        this(field.getName(), ZiggyDataType.objectToString(obj, field), ZiggyDataType.ZIGGY_STRING);
    }

    /**
     * Copy constructor.
     */
    public TypedParameter(TypedParameter original) {
        name = original.getName();
        stringValue = original.getString();
        dataType = original.dataType;
        scalar = original.scalar;
    }

    /**
     * Removes leading and trailing whitespace from the given parameter, including whitespace around
     * commas used as value separators (i.e., " v1, v2 " becomes "v1,v2"). This should be applied to
     * all values.
     */
    private String trimWhitespace(String value) {
        return StringUtils.trimListWhitespace(value);
    }

    /**
     * Returns the value contents of the TypedProperty instance, converted to the correct type. The
     * return is always an array; for scalar values, it is an array of unit size. Commas in the
     * value are interpreted as breaks between individual values and are used to size an array for
     * return.
     */
    public Object getValueAsArray() {

        Object convertedValue = getValue();
        if (isScalar()) {
            Object returnArray = ZiggyArrayUtils.constructFullPrimitiveArray(new long[] { 1 },
                dataType);
            ZiggyArrayUtils.fill(returnArray, convertedValue);
            return returnArray;
        }
        return convertedValue;
    }

    private void validate() {
        getValueAsArray();
    }

    // Note that equals() and hashCode(), as well as compareTo(), all rely solely on the name. This
    // allows a Set of TypedProperty instances to act like a Map of name-value pairs (i.e., ensures
    // that each name is unique in the Set). Use totalEquals() to compare all fields.

    @Override
    public int compareTo(TypedParameter other) {
        return name.compareTo(other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TypedParameter other = (TypedParameter) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

    public boolean totalEquals(TypedParameter parameter) {
        if (this == parameter) {
            return true;
        }
        if (parameter == null) {
            return false;
        }
        return dataType == parameter.dataType && Objects.equals(name, parameter.name)
            && scalar == parameter.scalar && Objects.equals(stringValue, parameter.stringValue);
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getString() {
        return stringValue;
    }

    /**
     * Set the {@link stringValue} member. Once validated, the {@link AbstractProperty} method
     * {@link AbstractProperty#setValue(Object)} must be invoked to put the string value into the
     * private value member of AbstractProperty.
     */
    public void setString(String value) {
        stringValue = trimWhitespace(value);
        validate();
        super.setValue(stringValue);
    }

    /**
     * Returns the contents of the object's string value, converted to an appropriate scalar or
     * array value of the correct type. Note that this overrides the
     * {@link AbstractProperty#getValue()} method, and that it completely ignores the private value
     * member of the abstract class.
     */
    @Override
    public Object getValue() {
        if (stringValue == null || stringValue.isEmpty()) {
            Object emptyValue = dataType.typedValue("");
            if (isScalar()) {
                return emptyValue;
            }
            Object returnArray = ZiggyArrayUtils.constructFullPrimitiveArray(new long[] { 1 },
                dataType);
            ZiggyArrayUtils.fill(returnArray, emptyValue);
            return returnArray;
        }

        // split the value string at any commas and check that the result is compatible with the
        // scalar flag (NB: the exception is Strings, because a scalar String can have commas in it;
        // if it's a scalar string TypedProperty and the stringValue has commas, we assume that the
        // user is doing the right thing and doesn't want an exception thrown due to scalar-array
        // mismatch).
        String[] splitString = stringValue.split(",");
        if (splitString.length > 1 && scalar && !dataType.equals(ZiggyDataType.ZIGGY_STRING)) {
            throw new IllegalStateException("Scalar TypedProperty instances can hold only 1 value");
        }

        if (isScalar()) {
            return dataType.typedValue(stringValue);
        }
        Object returnArray = ZiggyArrayUtils
            .constructFullPrimitiveArray(new long[] { splitString.length }, dataType);
        for (int i = 0; i < splitString.length; i++) {
            String s = splitString[i].trim();
            dataType.setArrayMember(dataType.typedValue(s), returnArray, i);
        }
        return returnArray;
    }

    /**
     * Sets the contents of the {@link AbstractProperty} private value member. At the same time,
     * sets the string value of this object.
     */
    @Override
    public void setValue(Object value) {
        if (value == null) {
            setString(null);
        } else if (isScalar()) {
            setString(value.toString());
        } else {
            setString(ZiggyArrayUtils.arrayToString(value));
        }
        super.setValue(stringValue);
    }

    public ZiggyDataType getDataType() {
        return dataType;
    }

    public void setType(ZiggyDataType type) {
        dataType = type;
        validate();
    }

    public boolean isScalar() {
        return scalar;
    }

    public void setScalar(boolean scalar) {
        this.scalar = scalar;
    }

    public String getDataTypeString() {
        return isScalar() ? dataType.getJavaTypeName()
            : dataType.getJavaTypeName() + ARRAY_TYPE_SUFFIX;
    }

    private static boolean isScalar(String typeString) {
        return !typeString.toLowerCase().endsWith(ARRAY_TYPE_SUFFIX);
    }

    private static String typeString(String originalTypeString) {
        if (isScalar(originalTypeString)) {
            return originalTypeString;
        }
        return originalTypeString.substring(0,
            originalTypeString.length() - ARRAY_TYPE_SUFFIX.length());
    }

    @Override
    public String getCategory() {
        return "";
    }

    @Override
    public String getDisplayName() {
        return name;
    }

    @Override
    public String getShortDescription() {
        return "";
    }

    @Override
    public Class<?> getType() {
        if (isScalar()) {
            return dataType.getJavaClass();
        }
        return getValue().getClass();
    }

    @Override
    public boolean isEditable() {
        return true;
    }

    @Override
    public void readFromObject(Object parametersInstance) {
        if (!(parametersInstance instanceof Parameters)) {
            throw new IllegalArgumentException("Argument must be Parameters instance");
        }
        Parameters parameters = (Parameters) parametersInstance;
        TypedParameter typedProperty = parameters.getParameter(name);
        setValue(typedProperty.getValue());
    }

    @Override
    public void writeToObject(Object parametersInstance) {
        if (!(parametersInstance instanceof Parameters)) {
            throw new IllegalArgumentException("Argument must be Parameters instance, not "
                + parametersInstance.getClass().getSimpleName());
        }
        Parameters parameters = (Parameters) parametersInstance;
        TypedParameter typedProperty = parameters.getParameter(name);
        typedProperty.setValue(getValue());
    }

    @Override
    public String toString() {
        return dataType + " " + name + "=" + stringValue;
    }
}
