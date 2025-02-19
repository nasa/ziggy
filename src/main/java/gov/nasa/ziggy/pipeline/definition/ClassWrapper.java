package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Transient;
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

    @Transient
    private String unmangledClassName;

    /** for Hibernate use only */
    ClassWrapper() {
    }

    /**
     * Creates a {@code ClassWrapper} for the given type.
     */
    public ClassWrapper(Class<? extends T> clazz) {
        this.clazz = clazz.getName();
        unmangledClassName = this.clazz;
    }

    /**
     * Creates a {@code ClassWrapper} for the class of the given instance.
     */
    public <E extends T> ClassWrapper(E instance) {
        clazz = instance.getClass().getName();
        unmangledClassName = clazz;
    }

    /**
     * Creates a copy of the given {@code ClassWrapper}.
     */
    public ClassWrapper(ClassWrapper<T> otherClassWrapper) {
        clazz = otherClassWrapper.clazz;
        unmangledClassName = otherClassWrapper.unmangledClassName;
    }

    /**
     * Returns a new instance of T.
     */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public T newInstance() {
        try {
            return (T) Class.forName(unmangledClassName()).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException
            | ClassNotFoundException e) {
            throw new PipelineException("Unable to instantiate " + unmangledClassName(), e);
        }
    }

    /**
     * Returns a constructor with a particular signature.
     */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Constructor<T> constructor(Class<?>... classArguments) {
        try {
            return (Constructor<T>) Class.forName(unmangledClassName())
                .getDeclaredConstructor(classArguments);
        } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            throw new PipelineException("Cannot return constructor for " + unmangledClassName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public Class<T> getClazz() {
        try {
            return (Class<T>) Class.forName(unmangledClassName());
        } catch (ClassNotFoundException e) {
            // Can never occur. By construction, the class wrapped by an instance of ClassWrapper
            // is available to the pipeline.
            throw new AssertionError(e);
        }
    }

    public String unmangledClassName() {
        if (unmangledClassName == null) {
            unmangledClassName = clazz.split(" ")[0];
        }
        return unmangledClassName;
    }

    public String getClassName() {
        return clazz;
    }

    public boolean isInitialized() {
        return clazz != null;
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
        if (obj == null || getClass() != obj.getClass()) {
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
        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public ClassWrapper<T> unmarshal(String v) {
            if (v == null) {
                return null;
            }
            Class<? extends T> clazz;
            try {
                clazz = (Class<? extends T>) Class.forName(v);
                return new ClassWrapper<>(clazz);
            } catch (ClassNotFoundException e) {
                throw new PipelineException("Class " + v + " is not on classpath", e);
            }
        }

        @Override
        public String marshal(ClassWrapper<T> v) {
            if (v == null) {
                return null;
            }
            return v.toString();
        }
    }
}
