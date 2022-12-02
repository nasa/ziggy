package gov.nasa.ziggy.module.hdf5;

import java.util.Arrays;
import java.util.List;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;

/**
 * Provides a class that implements Persistable that, in turn, makes use of scalars, arrays, and
 * lists of other Persistable classes. This supports testing of the HDF5 readers and writers.
 *
 * @author PT
 */
public class PersistableSample2 implements Persistable {

    public List<PersistableSample1> persistableList;
    public PersistableSample1[][] persistableArray2;
    public PersistableSample1 persistableScalar1;
    public PersistableSample1 persistableScalar2;
    public int intScalar;

    @ProxyIgnore
    public double ignoreThisField = 11.5;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(ignoreThisField);
        result = prime * result + (int) (temp ^ temp >>> 32);
        result = prime * result + intScalar;
        result = prime * result + Arrays.deepHashCode(persistableArray2);
        result = prime * result + (persistableList == null ? 0 : persistableList.hashCode());
        result = prime * result + (persistableScalar1 == null ? 0 : persistableScalar1.hashCode());
        result = prime * result + (persistableScalar2 == null ? 0 : persistableScalar2.hashCode());
        return result;
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
        PersistableSample2 other = (PersistableSample2) obj;
        if (Double.doubleToLongBits(ignoreThisField) != Double
            .doubleToLongBits(other.ignoreThisField)) {
            return false;
        }
        if (intScalar != other.intScalar) {
            return false;
        }
        if (!Arrays.deepEquals(persistableArray2, other.persistableArray2)) {
            return false;
        }
        if (persistableList == null) {
            if (other.persistableList != null) {
                return false;
            }
        } else if (!persistableList.equals(other.persistableList)) {
            return false;
        }
        if (persistableScalar1 == null) {
            if (other.persistableScalar1 != null) {
                return false;
            }
        } else if (!persistableScalar1.equals(other.persistableScalar1)) {
            return false;
        }
        if (persistableScalar2 == null) {
            if (other.persistableScalar2 != null) {
                return false;
            }
        } else if (!persistableScalar2.equals(other.persistableScalar2)) {
            return false;
        }
        return true;
    }

}
