package gov.nasa.ziggy.crud;

/**
 * A canonicalizable interface. Objects that implement this interface have the normal
 * {@link Object#toString()} method, of course, as well as a
 * {@link Canonicalizable#canonicalize(String)} method which returns a fully-qualified name for the
 * object. For example, database column names might return just the column name from
 * {@link Object#toString()}, but add the table name (or given alias) in
 * {@link Canonicalizable#canonicalize(String)}.
 * <p>
 * This interface also contains a {@link Canonicalizable#getObjectClass()} method for returning the
 * class of the object or its value. For example, database queries need to quote strings.
 *
 * @author Bill Wohler
 */
public interface Canonicalizable {
    /**
     * Returns the canonical name for this object.
     *
     * @param alias if non-<code>null</code>, the implementation may use this as a replacement for a
     * table name, for example.
     * @return the canonical name.
     */
    String canonicalize(String alias);

    /**
     * Returns the class of this object (or more typically its corresponding value).
     *
     * @return the object's class.
     */
    Class<?> getObjectClass();
}
