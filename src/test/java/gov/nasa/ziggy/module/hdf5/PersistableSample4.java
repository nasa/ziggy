package gov.nasa.ziggy.module.hdf5;

import java.util.Arrays;

public class PersistableSample4 {

    private PersistableSample2 persistableTest2;
    private PersistableSample3 persistableTest3;
    private PersistableSample3[] persistableTest3Array;

    public PersistableSample2 getPersistableTest2() {
        return persistableTest2;
    }

    public void setPersistableTest2(PersistableSample2 persistableTest2) {
        this.persistableTest2 = persistableTest2;
    }

    public PersistableSample3 getPersistableTest3() {
        return persistableTest3;
    }

    public void setPersistableTest3(PersistableSample3 persistableTest3) {
        this.persistableTest3 = persistableTest3;
    }

    public PersistableSample3[] getPersistableTest3Array() {
        return persistableTest3Array;
    }

    public void setPersistableTest3Array(PersistableSample3[] persistableTest3Array) {
        this.persistableTest3Array = persistableTest3Array;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (persistableTest2 == null ? 0 : persistableTest2.hashCode());
        result = prime * result + (persistableTest3 == null ? 0 : persistableTest3.hashCode());
        result = prime * result + Arrays.hashCode(persistableTest3Array);
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
        PersistableSample4 other = (PersistableSample4) obj;
        if (persistableTest2 == null) {
            if (other.persistableTest2 != null) {
                return false;
            }
        } else if (!persistableTest2.equals(other.persistableTest2)) {
            return false;
        }
        if (persistableTest3 == null) {
            if (other.persistableTest3 != null) {
                return false;
            }
        } else if (!persistableTest3.equals(other.persistableTest3)) {
            return false;
        }
        if (!Arrays.equals(persistableTest3Array, other.persistableTest3Array)) {
            return false;
        }
        return true;
    }

}
