package gov.nasa.ziggy.module.hdf5;

import java.util.Arrays;
import java.util.Objects;

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
        return Objects.hash(persistableTest2, persistableTest3,
            Arrays.hashCode(persistableTest3Array));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PersistableSample4 other = (PersistableSample4) obj;
        if (!Objects.equals(persistableTest2, other.persistableTest2)) {
            return false;
        }
        if (!Objects.equals(persistableTest3, other.persistableTest3)) {
            return false;
        }
        if (!Arrays.equals(persistableTest3Array, other.persistableTest3Array)) {
            return false;
        }
        return true;
    }

}
