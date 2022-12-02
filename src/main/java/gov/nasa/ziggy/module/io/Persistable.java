package gov.nasa.ziggy.module.io;

/**
 * This interface is used by the ClassWalker and Persistable input/output stream classes to
 * determine whether a class is persistable. Attempts to persist classes or generate code for
 * classes that are not Persistable will result in an error. The intent is to catch bugs where
 * unintended classes are included in the persistence hierarchy.
 * <p>
 * The Persistable interface has no methods or fields and serves only to identify the semantics of
 * being persistable.
 * <p>
 * Persistable classes must provide a default constructor so that the serialization code can
 * instantiate them.
 *
 * @author Todd Klaus
 */
public interface Persistable {
}
