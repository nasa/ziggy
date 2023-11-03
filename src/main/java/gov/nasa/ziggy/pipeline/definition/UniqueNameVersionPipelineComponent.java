package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.crud.UniqueNameVersionPipelineComponentCrud;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Version;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlTransient;

/**
 * Superclass for pipeline components that have the following features in common:
 * <ol>
 * <li>Each instance has a name, which is a {@link String}.
 * <li>Each instance has a version, which is an int.
 * <li>The combination of the name and the version must be unique in the component's database table.
 * <li>Each instance has a boolean that indicates whether the instance has been used in a pipeline
 * run (in which case the instance is said to be "locked").
 * <li>Each instance has an int that is used by Hibernate for optimistic locking.
 * </ol>
 * <p>
 * The components that meet the above criteria are {@link PipelineDefinition},
 * {@link PipelineModuleDefinition}, and {@link ParameterSet}. The capabilities of this class aren't
 * particularly exciting, but they allow for a bunch of common functionality to be located in a CRUD
 * superclass.
 *
 * @see UniqueNameVersionPipelineComponentCrud .
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@MappedSuperclass
@XmlTransient
public abstract class UniqueNameVersionPipelineComponent<T extends UniqueNameVersionPipelineComponent<T>> {

    private int version;

    @XmlAttribute(required = true)
    private String name;
    private boolean locked;

    /**
     * Used by Hibernate to implement optimistic locking. This ensures that if two users are
     * simultaneously editing an instance, when the second user goes to save values it won't allow
     * that user's stale values to overwrite the changes made by the first user.
     */
    @Version
    private int optimisticLockValue = 0;

    public int getVersion() {
        return version;
    }

    public boolean isLocked() {
        return locked;
    }

    /**
     * Locks the present instance. A locked instance is one that has been used in a pipeline. In
     * order to preserve data accountability, a locked instance may not be edited or modified by the
     * pipeline infrastructure. This method is used to lock an instance when it is first provided to
     * a pipeline instance.
     */
    public void lock() {
        locked = true;
    }

    public int getOptimisticLockValue() {
        return optimisticLockValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    void setVersion(int version) {
        this.version = version;
    }

    void setLocked(boolean locked) {
        this.locked = locked;
    }

    public T rename(String name) {
        T renamedInstance = newInstance();
        renamedInstance.setName(name);
        return renamedInstance;
    }

    @SuppressWarnings("unchecked")
    /**
     * Returns an unlocked version of the object. If the object is already unlocked, the object is
     * returned. If it is locked, a shallow copy is returned that has the following properties:
     * <ol>
     * <li>The value of locked is set to false.
     * <li>The database ID is null.
     * <li>The version number is 1 greater than the version number of the current object.
     * </ol>
     * This provides a new version of the object that can be persisted as such, which leaves the
     * existing, locked database instance untouched.
     *
     * @return
     */
    public T unlockedVersion() {
        if (!isLocked()) {
            return (T) this;
        }
        T unlockedVersion = newInstance();
        unlockedVersion.setName(name);
        unlockedVersion.setVersion(version + 1);
        return unlockedVersion;
    }

    protected abstract void clearDatabaseId();

    /**
     * Determines whether two objects are identical. This is used to determine whether it is
     * necessary for the CRUD merge() method to do anything at all. This is in contrast to
     * {@link #equals}, which might limit the fields considered to ensure that a component of a
     * given name, for example, is present in a set but once.
     */
    public abstract boolean totalEquals(Object other);

    /**
     * Constructs a renamed copy of the original object. Because the object is renamed, its version
     * number is set to 0, its optimistic lock value is set to 0, and its locked status is false.
     * This combination of features ensures that the copy doesn't get accidentally saved to the
     * database as a new version of the original instance.
     * <p>
     * The method requires that the subclass of {@link UniqueNameVersionPipelineComponent} have a
     * no-arg constructor and that its fields can be copied by the {@link Field#set(Object, Object)}
     * method.
     * <p>
     * Note that the resulting copy is "shallow" in that, for fields that are class instances,
     * collections, Maps, etc., the field in the copy will be a reference to the field in the
     * original. Therefore, it is the responsibility of the user to ensure that when changes made to
     * the copy are persisted, they do not result in unwanted changes to the original getting
     * persisted.
     */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public T newInstance() {

        try {
            T copy = (T) this.getClass()
                .getDeclaredConstructor((Class<?>[]) null)
                .newInstance((Object[]) null);
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isFinal(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                field.set(copy, field.get(this));
            }
            copy.setName("Copy of " + this.getName());
            copy.setVersion(0);
            copy.clearDatabaseId();
            copy.setLocked(false);
            return copy;
        } catch (IllegalAccessException | InstantiationException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException("Unable to construct copy of " + getClass().getName()
                + " instance with name '" + getName(), e);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
