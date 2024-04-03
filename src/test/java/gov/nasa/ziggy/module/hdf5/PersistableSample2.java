package gov.nasa.ziggy.module.hdf5;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
        return prime * result + (persistableScalar2 == null ? 0 : persistableScalar2.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PersistableSample2 other = (PersistableSample2) obj;
        if (Double.doubleToLongBits(ignoreThisField) != Double
            .doubleToLongBits(other.ignoreThisField) || intScalar != other.intScalar
            || !Arrays.deepEquals(persistableArray2, other.persistableArray2)
            || !Objects.equals(persistableList, other.persistableList)) {
            return false;
        }
        if (!Objects.equals(persistableScalar1, other.persistableScalar1)
            || !Objects.equals(persistableScalar2, other.persistableScalar2)) {
            return false;
        }
        return true;
    }
}
