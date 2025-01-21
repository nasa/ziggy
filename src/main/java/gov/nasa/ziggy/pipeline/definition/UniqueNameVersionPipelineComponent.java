package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.database.UniqueNameVersionPipelineComponentCrud;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ReflectionUtils;
import jakarta.persistence.Embedded;
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

    // Fields that must not be updated by the upateContents method.
    private static final List<String> FIELD_NAMES_NOT_UPDATED = List.of("name", "version", "locked",
        "id");
    private static final String OPTIMISTIC_LOCK_VALUE_FIELD_NAME = "optimisticLockValue";
    private int version;

    @XmlAttribute(required = true)
    private String name;
    private boolean locked;

    @Embedded
    private AuditInfo auditInfo = new AuditInfo();

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

    @SuppressWarnings("unchecked")
    /**
     * Returns an object that is unlocked and suitable for persistence in the database, which
     * contains the content of the object in the argument. Assumes that the current object (i.e.,
     * this) is a detached entity.
     * <p>
     * If the detached version is unlocked, then we need to merge the detached version in order to
     * satisfy the database name-version uniqueness constraint. In this case, the content of the
     * argument version is copied to the detached version.
     * <p>
     * If the detached version is locked, we need to return a transient entity with the content of
     * the argument version, an incremented ID, and its lock flag set to false. Because the argument
     * version might be transient or might be detached, it's necessary to copy the content of the
     * content version into a new, transient object.
     */
    public T unlockedVersion(T contentVersion) {

        // If this version is locked, we can persist the content version with a new
        // version number.
        if (isLocked()) {
            T unlockedVersion = contentVersion.newInstance();
            unlockedVersion.setName(contentVersion.getName());
            unlockedVersion.setVersion(getVersion() + 1);
            unlockedVersion.setLocked(false);
            unlockedVersion.resetOptimisticLockValue();
            return unlockedVersion;
        }

        // Otherwise, we want to persist the new content but do so in this object,
        // which is a detached entity.
        updateContents(contentVersion);
        return (T) this;
    }

    /**
     * Replaces the content of the current instance with the content of the instance in the method
     * argument.
     * <p>
     * The point of this is to take a detached database entity and fill it with new values. The
     * resulting entity can then be merged and the changes will be implemented under the name and
     * version number of the database entity. This, in turn, is necessary in order to make changes
     * to an unlocked entity, which preserves the same name and version number as currently used in
     * the database; to do this, the entity to be merged must be a detached entity (not transient)
     * with the correct name and version.
     */
    public void updateContents(T newContentInstance) {
        List<Field> fields = ReflectionUtils.getAllFields(this, true);
        for (Field field : fields) {
            if (Modifier.isFinal(field.getModifiers())) {
                continue;
            }
            if (FIELD_NAMES_NOT_UPDATED.contains(field.getName())) {
                continue;
            }

            if (field.getAnnotation(jakarta.persistence.Transient.class) != null) {
                continue;
            }

            // If the newContentInstance was not imported from a file, we need to preserve
            // the new instance's optimistic lock value. If it was imported from a file,
            // we need to keep the optimistic lock value of this instance when all the rest of the
            // content from the newContentInstance gets copied over.
            if (field.getName().equals(OPTIMISTIC_LOCK_VALUE_FIELD_NAME)
                && newContentInstance.getId() == null) {
                continue;
            }
            field.setAccessible(true);
            try {
                field.set(this, field.get(newContentInstance));
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new PipelineException("Unable to update content of " + getClass().getName()
                    + " instance with name '" + getName(), e);
            }
        }
    }

    protected abstract void clearDatabaseId();

    public abstract Long getId();

    /**
     * Determines whether two objects are identical. This is used to determine whether it is
     * necessary the CRUD merge() to first update the version number, in combination with the lock
     * status of the object to merge. This is in contrast to {@link #equals}, which might limit the
     * fields considered to ensure that a component of a given name, for example, is present in a
     * set but once.
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
            T copy = (T) getClass().getDeclaredConstructor((Class<?>[]) null)
                .newInstance((Object[]) null);
            copy.updateContents((T) this);
            copy.setName("Copy of " + getName());
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

    protected void resetOptimisticLockValue() {
        optimisticLockValue = 0;
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void updateAuditInfo() {
        auditInfo = new AuditInfo();
    }

    @Override
    public String toString() {
        return name;
    }
}
