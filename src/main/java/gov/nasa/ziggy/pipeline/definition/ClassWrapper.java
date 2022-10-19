package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Transient;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.parameters.Parameters;
import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * This class provides a simple wrapper around a String that contains a Java classname. The
 * constructors guarantee that the className is always the name of a valid class, and this class can
 * be parameterized to enforce that the class extends from the parameterized type
 *
 * @author Todd Klaus
 */
@Embeddable
public class ClassWrapper<T> implements Comparable<ClassWrapper<T>> {
    private String clazz = null;
    @Column(nullable = true)
    private Boolean initialized = false;

    @Transient
    private String unmangledClassName;

    /** for Hibernate use only */
    ClassWrapper() {
    }

    /**
     * Construct with a new instance
     *
     * @param clazz
     */
    public ClassWrapper(Class<? extends T> clazz) {
        this.clazz = clazz.getName();
        unmangledClassName = this.clazz;
        this.initialized = true;
    }

    /**
     * Construct from an existing instance
     *
     * @param <E>
     * @param instance
     * @throws PipelineException
     */
    public <E extends T> ClassWrapper(E instance) {
        this.clazz = instance.getClass().getName();
        unmangledClassName = clazz;
        this.initialized = true;
    }

    /**
     * Special purpose constructor for the case of a {@link ParameterSet}. this allows the
     * {@link ClassWrapper} to mangle the class name for instances of {@link DefaultParameters}, so
     * that multiple instances of class-wrapped DefaultParameters can coexist in a {@link Set} or
     * {@link Map}.
     */
    public ClassWrapper(ParameterSet paramSet) {
        @SuppressWarnings("unchecked")
        Class<? extends Parameters> actualClass = (Class<? extends Parameters>) paramSet
            .getParameters()
            .getClazz();
        unmangledClassName = actualClass.getName();
        if (actualClass != DefaultParameters.class) {
            this.clazz = unmangledClassName;
        } else {

            // In this case we append to the class name the parameter set name. The two are
            // separated by whitespace so that there's no risk of confustion with an actual
            // class name.
            this.clazz = unmangledClassName + " " + paramSet.getName().getName();
        }
        initialized = true;
    }

    /**
     * Copy constructor
     *
     * @param otherClassWrapper
     */
    public ClassWrapper(ClassWrapper<T> otherClassWrapper) {
        this.clazz = otherClassWrapper.clazz;
        this.unmangledClassName = otherClassWrapper.unmangledClassName;
        this.initialized = otherClassWrapper.initialized;
    }

    /**
     * Returns a new instance of T.
     *
     * @throws PipelineException
     */
    @SuppressWarnings("unchecked")
    public T newInstance() throws PipelineException {
        try {
			return (T) Class.forName(unmangledClassName()).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new PipelineException("failed to instantiate instance with className="
                + unmangledClassName() + ", caught e = " + e, e);
        }
    }

    /**
     * Returns a constructor with a particular signature
     *
     * @throws ClassNotFoundException
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    @SuppressWarnings("unchecked")
    public Constructor<T> constructor(Class<?>... classArguments)
        throws NoSuchMethodException, SecurityException, ClassNotFoundException {
        return (Constructor<T>) Class.forName(unmangledClassName())
            .getDeclaredConstructor(classArguments);
    }

    public Class<T> getClazz() {
        try {
            @SuppressWarnings("unchecked")
            Class<T> c = (Class<T>) Class.forName(unmangledClassName());
            return c;
        } catch (Exception e) {
            throw new PipelineException("failed to instantiate instance with className="
                + unmangledClassName() + ", caught e = " + e, e);
        }
    }

    public String unmangledClassName() {
        if (unmangledClassName == null) {
            unmangledClassName = clazz.split(" ")[0];
        }
        return unmangledClassName;
    }

    /**
     * @return the className
     */
    public String getClassName() {
        return clazz;
    }

    public boolean isInitialized() {
        if (initialized == null) {
            return false;
        } else {
            return initialized;
        }
    }

    @Override
    public String toString() {
        return clazz;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ClassWrapper<?> other = (ClassWrapper<?>) obj;
        if (!Objects.equals(clazz, other.clazz)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ClassWrapper<T> o) {
        return clazz.compareTo(o.clazz);
    }

    /**
     * {@link XmlAdapter} for {@link ClassWrapper} instances that allows them to be exchanged
     * between XML files and Ziggy.
     *
     * @author PT
     */
    public static class ClassWrapperAdapter<T> extends XmlAdapter<String, ClassWrapper<T>> {

        @SuppressWarnings("unchecked")
        @Override
        public ClassWrapper<T> unmarshal(String v) throws Exception {
            if (v == null) {
                return null;
            }
            Class<? extends T> clazz = (Class<? extends T>) Class.forName(v);
            return new ClassWrapper<>(clazz);
        }

        @Override
        public String marshal(ClassWrapper<T> v) throws Exception {
            if (v == null) {
                return null;
            }
            return v.toString();
        }

    }

}
