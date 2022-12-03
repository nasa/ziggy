package gov.nasa.ziggy.pipeline.definition;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.FetchType;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.l2fprod.common.beans.BeanUtils;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.ReflectionUtils;
import gov.nasa.ziggy.util.StringUtils;

/**
 * This class provides database persistence for an arbitrary Java class using JavaBeans semantics.
 * Persistence is provided by representing the bean's contents as a {@link Set} of
 * {@link TypedParameter} instances. A {@link TypedParameter} contains a name-value pair and also a
 * data type, which can be "none."
 * <p>
 * The class operates on two types of Java class. The typical standard use case is a Java class that
 * conforms to the JavaBean specification. The fields of such a class get mapped into
 * {@link TypedParameter} instances. The alternate use case is subclasses of
 * {@link TypedParameterCollection}. These classes uses the TypedProperty internally for storage, in
 * order to be able to store typed name-value pairs without the need to define a specialized Java
 * class to contain a specific set of such parameters. Some class methods will offer two execution
 * branches: one for instances of {@link TypedParameterCollection}, one for everything else.
 * <p>
 * This class also contains the name of the class and uses that to manage instantiation and to
 * define the valid members of the Set. For type-safety, the class can be parameterized &lt;T&gt;.
 * The constructor then enforces that the class can only be instantiated with types that extend T. T
 * must either conform to the JavaBeans specification or else must extend
 * {@link TypedParameterCollection}.
 * <p>
 * This class also provides instance management (create, populate, describe). The instance created
 * is considered to be transient; changes to the instance, either directly or through the populate()
 * method, do not affect the values that gets persisted.
 *
 * @author Todd Klaus
 * @author PT
 */
@Embeddable
public class BeanWrapper<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BeanWrapper.class);

    @Column(nullable = true)
    private String clazz = null;
    @Column(nullable = true)
    private Boolean initialized = false;

    @ElementCollection(fetch = FetchType.EAGER)
    @Fetch(value = FetchMode.SUBSELECT)
    @Embedded
    Set<TypedParameter> typedProperties = new HashSet<>();

    /** For Hibernate use only */
    BeanWrapper() {
    }

    /**
     * Construct from a Class. A new instance of the class is created, and the props map is
     * populated from the default values in the new instance.
     *
     * @param clazz
     */
    public BeanWrapper(Class<? extends T> clazz) {
        setClazz(clazz);

        // DefaultParameters don't require any additional initialization
        if (!TypedParameterCollection.class.isAssignableFrom(clazz)) {
            /* call createInstance with populate==false so that the class defaults will be used */
            T instance = createInstance(false);
            try {
                typedProperties = describeBean(instance);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new PipelineException(
                    "Unable to instantiate BeanWrapper of " + clazz.getName(), e);
            }
        }
        initialized = true;

    }

    /**
     * Construct from an existing instance
     *
     * @param <E>
     * @param instance
     * @throws PipelineException
     */
    @SuppressWarnings("unchecked")
    public <E extends T> BeanWrapper(E instance) {
        setClazz((Class<? extends T>) instance.getClass());
        if (TypedParameterCollection.class.isAssignableFrom(instance.getClass())) {
            TypedParameterCollection dInstance = (TypedParameterCollection) instance;
            typedProperties = dInstance.getParameters();
        } else

        {
            typedProperties = describe(instance);
        }
        initialized = true;
    }

    /**
     * Copy constructor
     *
     * @param otherBeanWrapper
     */
    public BeanWrapper(BeanWrapper<T> otherBeanWrapper) {
        setClazz(otherBeanWrapper.wrappedClass());

        if (otherBeanWrapper.typedProperties != null) {
            this.typedProperties = new HashSet<>();
            for (TypedParameter property : otherBeanWrapper.typedProperties) {
                this.typedProperties.add(new TypedParameter(property));
            }
        }
        this.initialized = otherBeanWrapper.initialized;
    }

    /**
     * Create a new instance of this bean. If the props Map is set, use it to populate() the
     * instance.
     *
     * @param populate if true, populate the new instance with current contents of props
     * @throws PipelineException
     */
    private T createInstance(boolean populate) throws PipelineException {
        try {
            @SuppressWarnings("unchecked")
            T instance = (T) Class.forName(clazz).getDeclaredConstructor().newInstance();
            if (populate) {
                populate(instance);
            }

            return instance;
        } catch (Exception e) {
            throw new PipelineException(
                "failed to instantiate instance with className=" + clazz + ", caught e = " + e, e);
        }
    }

    /**
     * Returns the instance of T, creating and initializing, if necessary.
     * <p>
     * Consider this object to be READ-ONLY. Changes are only persisted when calling populate() or
     * setProperty()
     *
     * @throws PipelineException
     */
    public T getInstance() throws PipelineException {
        return createInstance(true);
    }

    /**
     * Populate the classname and map from the specified instance.
     *
     * @param instance
     * @throws PipelineException
     */
    @SuppressWarnings("unchecked")
    public void populateFromInstance(T instance) throws PipelineException {
        setClazz((Class<? extends T>) instance.getClass());
        typedProperties = describe(instance);
    }

    /**
     * Returns true if new fields have been added to the class, but do not exist in the database.
     *
     * @return
     */
    public boolean hasNewUnsavedFields() {
        T instance = getInstance();
        Set<TypedParameter> newProperties = describe(instance);

        boolean sameKeys = true;
        for (TypedParameter newProperty : newProperties) {
            if (!typedProperties.contains(newProperty)) {
                sameKeys = false;
            }
        }

        return !sameKeys;
    }

    /**
     * Determines whether the values in all {@link TypedParameter} instances are compatible with the
     * data types in their respective instances.
     *
     * @return true if all {@link TypedParameter} instances are internally consistent.
     */
    public boolean checkPrimitiveDataTypes() {
        T instance = getInstance();
        Set<TypedParameter> allProperties = getTypedProperties();
        Class<?> type = null;
        String value = null;
        boolean primitiveDataTypesOkay = true;
        for (TypedParameter property : allProperties) {
            try {
                Field field = instance.getClass().getDeclaredField(property.getName());
                type = field.getType();
                value = property.getString();
                if (type.equals(byte.class)) {
                    Byte.valueOf(value);
                } else if (type.equals(short.class)) {
                    Short.valueOf(value);
                } else if (type.equals(int.class)) {
                    Integer.valueOf(value);
                } else if (type.equals(long.class)) {
                    Long.valueOf(value);
                } else if (type.equals(float.class)) {
                    Float.valueOf(value);
                } else if (type.equals(double.class)) {
                    Double.valueOf(value);
                }
            } catch (NoSuchFieldException ignore) {
                // ignore fields that no longer exist in the class
            } catch (NumberFormatException e) {
                Log.error("Typed property mismatch: value " + value
                    + " cannot be converted to type " + type, e);
                primitiveDataTypesOkay = false;
            }
        }
        return primitiveDataTypesOkay;
    }

    /**
     * Removes leading and trailing whitespace from all values, including whitespace around commas
     * used as value separators (i.e., " v1, v2 " becomes "v1,v2").
     *
     * @throws Exception
     */
    public void trimWhitespace() throws Exception {

        Set<TypedParameter> updatedTypedProperties = new HashSet<>();
        for (TypedParameter typedProperty : getTypedProperties()) {
            String name = typedProperty.getName();
            String value = typedProperty.getString();
            ZiggyDataType type = typedProperty.getDataType();
            boolean scalar = typedProperty.isScalar();

            value = StringUtils.trimListWhitespace(value);
            updatedTypedProperties.add(new TypedParameter(name, value, type, scalar));
        }
        setTypedProperties(updatedTypedProperties);
    }

    /**
     * Uses {@link BeanUtils} to populate the instance (the typedProperties Set is unaffected).
     */
    private void populate(T instance) throws PipelineException {

        if (TypedParameterCollection.class.isAssignableFrom(instance.getClass())) {
            TypedParameterCollection dInstance = (TypedParameterCollection) instance;
            Set<TypedParameter> instanceProperties = new HashSet<>();
            for (TypedParameter property : typedProperties) {
                instanceProperties.add(new TypedParameter(property));
            }
            dInstance.setParameters(instanceProperties);
            return;
        }

        if (typedProperties != null) {
            try {
                /*
                 * Get the values out of the typedProperty Set, and put them into a new Set for the
                 * instance.
                 *
                 * props does not contain nulls, so before we call populate, we need to create a new
                 * Map (allProps) with all of the properties returned by BeanUtilsBean2.describe,
                 * then override the values from props. This also initializes new (unsaved) fields
                 * to the default value specified in the class
                 */
                Map<String, TypedParameter> typedPropertyMap = typedPropertyByName(typedProperties);
                Set<TypedParameter> allProperties = describeBean(instance);
                for (TypedParameter property : allProperties) {
                    String value = null;
                    if (typedProperties.contains(property)) {
                        value = typedPropertyMap.get(property.getName()).getString().trim();
                        if (value.isEmpty()) {
                            value = null;
                        }
                        property.setValue(value);
                    }
                }
                typedPropertyMap = typedPropertyByName(allProperties);

                // Pour the values in the typed properties back into the instance, applying
                // appropriate conversions from String.
                for (Field field : ReflectionUtils.getAllFields(instance, true)) {
                    ZiggyDataType.setField(instance, field,
                        typedPropertyMap.get(field.getName()).getString());
                }

                fixNulls(instance, allProperties);
            } catch (Exception e) {
                throw new PipelineException("failed to populate bean, caught e = " + e, e);
            }
        }
    }

    /**
     * Override the behavior of BeanUtilsBean2 w.r.t. String[] and String fields with a null or
     * missing value in the Map.
     * <p>
     * For some reason, BeanUtilsBean2 sets array fields to an array with one null element instead
     * of an empty array when the value in the Map is null. This method fixes that. Strings are
     * initialized to the empty String ""
     */
    private void fixNulls(T instance, Set<TypedParameter> allProperties) throws Exception {
        // Get property descriptors for the bean, in case we need to find
        // writer methods for any properties that need to be fixed.
        Map<String, PropertyDescriptor> beanProperties = new HashMap<>();
        BeanInfo beanInfo = Introspector.getBeanInfo(instance.getClass());
        for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
            beanProperties.put(descriptor.getName(), descriptor);
        }

        for (TypedParameter typedProperty : allProperties) {
            String key = typedProperty.getName();
            String value = typedProperty.getString();
            if (value == null) {
                Class<? extends Object> instanceClass = instance.getClass();
                try {
                    Field f = instanceClass.getDeclaredField(key);
                    f.setAccessible(true);
                    Class<?> fieldClass = f.getType();
                    // Need to make the field accessible, since we may have
                    // public bean access methods to a private member.
                    f.setAccessible(true);
                    if (!fieldClass.isArray()) {
                        if (!fieldClass.isPrimitive()) {
                            f.set(instance, fieldClass.getDeclaredConstructor().newInstance());
                        }
                    } else {
                        Class<?> componentType = fieldClass.getComponentType();
                        f.set(instance, Array.newInstance(componentType, 0));
                    }
                } catch (InstantiationException ignore) {
                    // Could not instantiate a value, probably because the class
                    // does not have an empty constructor. Leave that property
                    // null.
                } catch (NoSuchFieldException ignore) {
                    // Try a bean writer method if one exists for the property.
                    PropertyDescriptor descriptor = beanProperties.get(key);
                    if (descriptor != null && descriptor.getWriteMethod() != null) {
                        try {
                            Class<?> clazz = descriptor.getPropertyType();
                            if (!clazz.isArray()) {
                                descriptor.getWriteMethod()
                                    .invoke(instance, clazz.getDeclaredConstructor().newInstance());
                            } else {
                                Class<?> componentType = clazz.getComponentType();
                                descriptor.getWriteMethod()
                                    .invoke(instance, Array.newInstance(componentType, 0));
                            }
                        } catch (InstantiationException ignore2) {
                            // Could not instantiate a value, probably because
                            // the class does not have an empty constructor.
                            // Leave that property null.
                        }
                    }
                }
            }
        }
    }

    /**
     * Uses {@link BeanUtils} to construct a Map containing all of the properties for T.
     * <p>
     * Used by the console to populate a property editor so the user can configure the properties.
     * <p>
     * Also removes the super-class properties that should not be visible to the user (why doesn't
     * BeanUtils offer this filtering capability?)
     *
     * @param instance
     * @return
     * @throws PipelineException
     */
    private Set<TypedParameter> describe(T instance) throws PipelineException {
        if (TypedParameterCollection.class.isAssignableFrom(instance.getClass())) {
            TypedParameterCollection dInstance = (TypedParameterCollection) instance;
            Set<TypedParameter> instanceParameters = new HashSet<>();
            for (TypedParameter property : dInstance.getParameters()) {
                instanceParameters.add(new TypedParameter(property));
            }
            return instanceParameters;
        } else {
            try {
                return describeBean(instance);
            } catch (Exception e) {
                throw new PipelineException("failed to describe bean, caught e = " + e, e);
            }
        }
    }

    private Set<TypedParameter> describeBean(T instance)
        throws IllegalArgumentException, IllegalAccessException {
        Set<TypedParameter> instanceParameters = new HashSet<>();
        for (Field field : ReflectionUtils.getAllFields(instance, true)) {
            instanceParameters.add(new TypedParameter(instance, field));
        }
        return instanceParameters;

    }

    public String getClassName() {
        return clazz;
    }

    public Class<?> getClazz() {
        try {
            return Class.forName(clazz);
        } catch (Exception e) {
            throw new PipelineException(
                "failed to instantiate instance with className=" + clazz + ", caught e = " + e, e);
        }
    }

    /**
     * Gets all possible properties of the bean, including those that have null values.
     *
     * @return a set of bean properties
     */
    public Set<String> getAllPropertyNames() {
        Set<String> allProps = new HashSet<>();

        if (TypedParameterCollection.class.isAssignableFrom(wrappedClass())) {
            for (TypedParameter typedProperty : typedProperties) {
                allProps.add(typedProperty.getName());
            }
        } else {
            try {
                BeanInfo beanInfo = Introspector.getBeanInfo(getClazz(), Object.class);
                for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
                    allProps.add(descriptor.getName());
                }
            } catch (IntrospectionException e) {
                LOG.warn("Error introspecting on bean class: " + clazz);
            }
        }
        return allProps;
    }

    public void setClazz(Class<? extends T> clazz) {
        this.clazz = clazz.getName();
        initialized = true;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends T> wrappedClass() {
        try {
            return (Class<? extends T>) Class.forName(clazz);
        } catch (ClassNotFoundException e) {

            // This should never happen, as the clazz string is initially set from the name of
            // an actual Class instance. Nonetheless, if it does happen we want processing to stop.
            throw new PipelineException("Class " + clazz + " cannot be found", e);
        }
    }

    public void setProperties(Map<String, String> newProps) {
        Set<TypedParameter> newProperties = new HashSet<>();
        for (String key : newProps.keySet()) {
            newProperties.add(new TypedParameter(key, newProps.get(key)));
        }
        typedProperties = newProperties;
        initialized = true;
    }

    public boolean isInitialized() {
        if (initialized == null) {
            return false;
        } else {
            return initialized;
        }
    }

    public void setInitialized(boolean set) {
        this.initialized = set;
    }

    public void setTypedProperties(Set<TypedParameter> typedProperties) {
        this.typedProperties = typedProperties;
    }

    public Set<TypedParameter> getTypedProperties() {
        return typedProperties;
    }

    public static Map<String, TypedParameter> typedPropertyByName(
        Set<TypedParameter> typedProperties) {
        Map<String, TypedParameter> props = new HashMap<>();
        for (TypedParameter typedProperty : typedProperties) {
            props.put(typedProperty.getName(), typedProperty);
        }
        return props;
    }

    public static Map<String, String> propertyValueByName(Set<TypedParameter> typedProperties) {
        Map<String, String> props = new HashMap<>();
        for (TypedParameter typedProperty : typedProperties) {
            props.put(typedProperty.getName(), typedProperty.getString());
        }
        return props;
    }

}
