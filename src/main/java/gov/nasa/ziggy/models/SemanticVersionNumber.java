package gov.nasa.ziggy.models;

import java.util.Objects;

/**
 * Manages semantic version numbers for models that use them.
 * <p>
 * A semantic version number is a number of the form M.n.p, where M is the major version number, n
 * is the minor version number, and p is the patch number, with M, n, and p all positive integers or
 * zero.
 *
 * @author PT
 */
public class SemanticVersionNumber implements Comparable<SemanticVersionNumber> {

    private int major;
    private int minor;
    private int patch;

    public SemanticVersionNumber(String versionString) {
        String[] versionSplit = versionString.split("\\.");
        if (versionSplit.length != 3) {
            throw new IllegalArgumentException("Version number " + versionString
                + " does not match M.n.p format of semantic version number");
        }
        major = Integer.parseInt(versionSplit[0]);
        minor = Integer.parseInt(versionSplit[1]);
        patch = Integer.parseInt(versionSplit[2]);
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("Major, minor, and patch numbers must be >= 0");
        }
    }

    public SemanticVersionNumber(int major, int minor, int patch) {
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("Major, minor, and patch numbers must be >= 0");
        }
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    @Override
    public String toString() {
        return Integer.toString(major) + "." + Integer.toString(minor) + "."
            + Integer.toString(patch);
    }

    /**
     * Performs comparison between two semantic version numbers to determine which is greater. In
     * this comparison, any difference between major versions takes precedence; if the major
     * versions are the same, the minor version difference is used; if these are the same, the patch
     * versions are compared.
     * <p>
     * Note that while the compareTo can compare any two SemanticVersionNumber instances, it cannot
     * give the absolute positions of SemanticVersionNumbers in a list of same. In other words: both
     * (9.2.0).compareTo(8.20.0) and (8.21.0).compareTo(8.20.0) will return +1, so both 9.2.0 and
     * 8.21.0 are greater than 8.20.0; however, even though both compareTo operations give a result
     * of +1, that does not mean that 9.2.0 and 8.21.0 are equivalent in compareTo terms.
     */
    @Override
    public int compareTo(SemanticVersionNumber o) {
        if (major != o.major) {
            return major - o.major;
        }
        if (minor != o.minor) {
            return minor - o.minor;
        }
        return patch - o.patch;
    }

    public int getMajor() {
        return major;
    }

    public void setMajor(int major) {
        this.major = major;
    }

    public int getMinor() {
        return minor;
    }

    public void setMinor(int minor) {
        this.minor = minor;
    }

    public int getPatch() {
        return patch;
    }

    public void setPatch(int patch) {
        this.patch = patch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SemanticVersionNumber other = (SemanticVersionNumber) obj;
        if (major != other.major || minor != other.minor || patch != other.patch) {
            return false;
        }
        return true;
    }
}
