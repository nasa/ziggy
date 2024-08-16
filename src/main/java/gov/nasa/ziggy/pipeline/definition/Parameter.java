package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.l2fprod.common.propertysheet.AbstractProperty;
import com.l2fprod.common.propertysheet.PropertySheet;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.ZiggyStringUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

/**
 * Represents a parameter with a name, a value, and a type. The name and value are Strings, the type
 * is an enum that represents all valid parameter types. This allows the correct data type to be
 * stored at serialization time even though the value is stored as a String. It also means that
 * instances of {@link Parameter} can be stored in the database without much ado: each instance has
 * a name (String), a value (String), a data type (Enum), and a scalar vs. array flag (boolean).
 * <p>
 * {@link Parameter} instances are used by {@link ParameterSet} and {@link UnitOfWork} to store the
 * contents of each class.
 * <p>
 * In order to allow a {@link Parameter} instance to be edited from a GUI window, the class extends
 * {@link AbstractProperty} from l2fprod, which allows a {@link PropertySheet} to manage the
 * impedance matching between this class and the GUI window. AbstractProperty requires methods
 * {@link #getValue()} and {@link #setValue(Object)}, which want to get and set values that are in
 * the specified class of the {@link Parameter}. To avoid confusion with the AbstractProperty's use
 * of value, the content of a {@link Parameter} is stored in a field named stringValue (instead of
 * value, which would be more natural). It is accessed and mutated by methods {@link #getString()}
 * and {@link #setString(String)} (instead of getValue() and setValue(), which are reserved for use
 * by AbstractProperty).
 *
 * @author PT
 */

@Embeddable
public class Parameter extends AbstractProperty implements Comparable<Parameter> {

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
    public Parameter() {
    }

    /**
     * Standard constructor that takes the contents of the instance as arguments.
     */
    public Parameter(String name, String value, ZiggyDataType type) {
        this(name, value, type, true);
    }

    /**
     * Constructor that also allows the caller to specify whether the value is a scalar (the
     * alternative is that it's a 1-d collection of some sort; array, list, etc.).
     */
    public Parameter(String name, String value, ZiggyDataType type, boolean scalar) {
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
     * Constructs a {@link Parameter} instance from name, value, and type strings. The type string
     * has the word "array" added as a suffix to a standard type (i.e., "stringarray", "floatarray",
     * etc.) the parameter is instantiated as a 1-d array of values rather than a single value.
     */
    public Parameter(String name, String value, String typeString) {
        this(name, value, ZiggyDataType.getDataTypeFromString(typeString(typeString)),
            isScalar(typeString));
    }

    /** Constructs a {@link Parameter} instance that has a type of NONE. */
    public Parameter(String name, String value) {
        this(name, value, ZiggyDataType.ZIGGY_STRING);
    }

    /**
     * Constructs a {@link Parameter} instance from the value of a {@link Field} in an
     * {@link Object}. The resulting {@link Parameter} will have a type of
     * {@link ZiggyDataType#ZIGGY_STRING}.
     */
    public Parameter(Object obj, Field field) {
        this(field.getName(), ZiggyDataType.objectToString(obj, field), ZiggyDataType.ZIGGY_STRING);
    }

    /**
     * Copy constructor.
     */
    public Parameter(Parameter original) {
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
        return ZiggyStringUtils.trimListWhitespace(value);
    }

    /**
     * Returns the value contents of the {@link Parameter} instance, converted to the correct type.
     * The return is always an array; for scalar values, it is an array of unit size. Commas in the
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
    // allows a Set of Parameter instances to act like a Map of name-value pairs (i.e., ensures
    // that each name is unique in the Set). Use totalEquals() to compare all fields.

    @Override
    public int compareTo(Parameter other) {
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
        Parameter other = (Parameter) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

    public boolean totalEquals(Parameter parameter) {
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
        if (StringUtils.isBlank(stringValue)) {
            Object emptyValue = dataType.typedValue("");
            if (isScalar()) {
                return emptyValue;
            }
            Object returnArray = ZiggyArrayUtils.constructFullPrimitiveArray(new long[] { 0 },
                dataType);
            return returnArray;
        }

        // split the value string at any commas and check that the result is compatible with the
        // scalar flag (NB: the exception is Strings, because a scalar String can have commas in it;
        // if it's a scalar string TypedProperty and the stringValue has commas, we assume that the
        // user is doing the right thing and doesn't want an exception thrown due to scalar-array
        // mismatch).
        String[] splitString = stringValue.split(",");
        if (splitString.length > 1 && scalar && !dataType.equals(ZiggyDataType.ZIGGY_STRING)) {
            throw new IllegalStateException("Scalar Parameter instances can hold only 1 value");
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

    /**
     * Transfer information from a {@link Parameter} instance to its corresponding property.
     */
    @Override
    public void readFromObject(Object parametersInstance) {
        if (parametersInstance instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Parameter> parameters = (Collection<Parameter>) parametersInstance;
            Map<String, Parameter> parametersByName = parametersByName(parameters);
            Parameter parameter = parametersByName.get(name);
            setValue(parameter.getValue());
            return;
        }
        if (!(parametersInstance instanceof ParameterSet)) {
            throw new IllegalArgumentException(
                "Argument must be ParameterSet instance or Collection of Parameters, not "
                    + parametersInstance.getClass().getSimpleName());
        }
        ParameterSet parameterSet = (ParameterSet) parametersInstance;
        Parameter parameter = parameterSet.parameterByName().get(name);
        setValue(parameter.getValue());
    }

    /** Transfer information from a property to a {@link Parameter}. */
    @Override
    public void writeToObject(Object parametersInstance) {
        if (parametersInstance instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Parameter> parameters = (Collection<Parameter>) parametersInstance;

            // For typed parameters, equals() looks only at the name, so
            // the following code isn't as screwy as it looks.
            parameters.remove(this);
            parameters.add(this);
            return;
        }
        if (!(parametersInstance instanceof ParameterSet)) {
            throw new IllegalArgumentException("Argument must be ParameterSet instance, not "
                + parametersInstance.getClass().getSimpleName());
        }
        ParameterSet parameterSet = (ParameterSet) parametersInstance;

        // For typed parameters, equals() looks only at the name, so
        // the following code isn't as screwy as it looks.
        parameterSet.getParameters().remove(this);
        parameterSet.getParameters().add(this);
        return;
    }

    @Override
    public String toString() {
        return nameAndDataType() + "=" + stringValue;
    }

    public String nameAndDataType() {
        String dataTypeWithArrayFlag = scalar ? dataType.toString()
            : dataType.toString() + " " + ARRAY_TYPE_SUFFIX;
        return dataTypeWithArrayFlag + " " + name;
    }

    public static boolean identicalParameters(Collection<Parameter> parameters1,
        Collection<Parameter> parameters2) {
        if (parameters1 == null) {
            return parameters2 == null;
        }
        if (parameters2 == null) {
            return false;
        }
        if (parameters1.size() != parameters2.size()) {
            return false;
        }
        Map<String, Parameter> parameters1ByName = parametersByName(parameters1);
        Map<String, Parameter> parameters2ByName = parametersByName(parameters2);
        for (Map.Entry<String, Parameter> parameter1Entry : parameters1ByName.entrySet()) {
            Parameter parameter2 = parameters2ByName.get(parameter1Entry.getKey());
            if (parameter2 == null) {
                return false;
            }
            if (!parameter2.totalEquals(parameter1Entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static Map<String, Parameter> parametersByName(Collection<Parameter> parameters) {
        Map<String, Parameter> parametersByName = new HashMap<>();
        for (Parameter parameter : parameters) {
            parametersByName.put(parameter.getName(), parameter);
        }
        return parametersByName;
    }
}
