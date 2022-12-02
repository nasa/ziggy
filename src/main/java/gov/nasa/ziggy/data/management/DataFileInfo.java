package gov.nasa.ziggy.data.management;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * A data file that is transferred between the datastore and task directory. Subclasses must
 * implement the {@link #getPattern()} method to identify files in the datastore, as well as the
 * constructors, if only to call super().
 * <p>
 * N.B. This class is intended to replace DatastoreId. If it doesn't, it should be deleted.
 *
 * @author PT
 * @author Bill Wohler
 */
public abstract class DataFileInfo implements Comparable<DataFileInfo> {

    private Path name;

    /**
     * Creates an empty DataFileInfo object. The only thing that can be done with this object is to
     * call {@link #pathValid(Path)} on it.
     */
    protected DataFileInfo() {
    }

    /**
     * Creates a DataFileInfo object with the given name
     *
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if the name doesn't match the pattern defined by this class
     */
    protected DataFileInfo(String name) {
        this.name = Paths.get(checkNotNull(name, "name"));
        checkArgument(pathValid(this.name),
            "Data file " + name + " does not match required pattern");
    }

    /**
     * Creates a DataFileInfo object with the given name
     *
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if the name doesn't match the pattern defined by this class
     */
    protected DataFileInfo(Path name) {
        checkArgument(pathValid(checkNotNull(name, "name")),
            "File " + name.toString() + " does not match required pattern");
        this.name = checkNotNull(name, "name");
    }

    /**
     * Returns the pattern that all file names stored within the given DataFileInfo subclass must
     * match.
     */
    protected abstract Pattern getPattern();

    /**
     * Returns true if the given path matches the pattern defined by this class.
     *
     * @return false if the given path is null or doesn't match the pattern
     */
    public boolean pathValid(Path path) {
        return path == null ? false : getPattern().matcher(path.getFileName().toString()).matches();
    }

    /**
     * Returns the non-null relative path to the data file.
     *
     * @throws IllegalStateException if the default constructor was used
     */
    public Path getName() {
        checkState(name != null, "Default constructor was used");
        return name;
    }

    /**
     * Implements comparison of DataFileInfo instances by alphabetizing of their names.
     */
    @Override
    public int compareTo(DataFileInfo other) {
        return name.toString().compareTo(other.getName().toString());
    }

    @Override
    public String toString() {
        return name.toString();
    }

}
