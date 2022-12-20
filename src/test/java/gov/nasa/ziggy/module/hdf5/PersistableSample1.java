package gov.nasa.ziggy.module.hdf5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import gov.nasa.ziggy.module.io.Persistable;

/**
 * Provides a simple class that implements Persistable for use in HDF5 testing.
 *
 * @author PT
 */
public class PersistableSample1 implements Persistable {

    public List<Integer> intList;
    public float[] floatArray1;
    public long[][] longArray2;
    public boolean[][][] booleanArray3;
    public double doubleScalar;
    public Float boxedFloatScalar;
    public EnumTest enumScalar = EnumTest.FIRST;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.deepHashCode(booleanArray3);
        result = prime * result + (boxedFloatScalar == null ? 0 : boxedFloatScalar.hashCode());
        long temp;
        temp = Double.doubleToLongBits(doubleScalar);
        result = prime * result + (int) (temp ^ temp >>> 32);
        result = prime * result + (enumScalar == null ? 0 : enumScalar.hashCode());
        result = prime * result + Arrays.hashCode(floatArray1);
        result = prime * result + (intList == null ? 0 : intList.hashCode());
        return prime * result + Arrays.deepHashCode(longArray2);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PersistableSample1 other = (PersistableSample1) obj;
        if (!Arrays.deepEquals(booleanArray3, other.booleanArray3)) {
            return false;
        }
        if (!Objects.equals(boxedFloatScalar, other.boxedFloatScalar)) {
            return false;
        }
        if (Double.doubleToLongBits(doubleScalar) != Double.doubleToLongBits(other.doubleScalar)) {
            return false;
        }
        if (enumScalar != other.enumScalar) {
            return false;
        }
        if (!Arrays.equals(floatArray1, other.floatArray1)) {
            return false;
        }
        if (!Objects.equals(intList, other.intList)) {
            return false;
        }
        if (!Arrays.deepEquals(longArray2, other.longArray2)) {
            return false;
        }
        return true;
    }

    /**
     * Generates a PersistableTest1 object with caller-specified dimensions of its methods and
     * randomly generated values.
     *
     * @param integerSize length of the Integer list
     * @param float1Size1 length of the 1-d float array
     * @param long2size1 length of dim 1 of the 2-d long array
     * @param long2size2 length of dim 2 of the 2-d long array
     * @param bool3size1 length of dim 1 of the 3-d boolean array
     * @param bool3size2 length of dim 2 of the 3-d boolean array
     * @param bool3size3 length of dim 3 of the 3-d boolean array
     * @return populated object with the dimensions specified by the caller
     */
    public static PersistableSample1 newInstance(int integerSize, int float1Size1, int long2size1,
        int long2size2, int bool3size1, int bool3size2, int bool3size3) {

        PersistableSample1 persistableObject = new PersistableSample1();

        Random rng = new Random();

        // start with the scalars
        persistableObject.doubleScalar = rng.nextDouble();
        persistableObject.boxedFloatScalar = rng.nextFloat();

        // now the 1-d arrays
        float[] floatArray1 = new float[float1Size1];
        persistableObject.floatArray1 = floatArray1;
        for (int i = 0; i < float1Size1; i++) {
            floatArray1[i] = rng.nextFloat();
        }

        // now the 2-d array
        long[][] longArray2 = new long[long2size1][long2size2];
        persistableObject.longArray2 = longArray2;
        for (int i = 0; i < long2size1; i++) {
            for (int j = 0; j < long2size2; j++) {
                longArray2[i][j] = rng.nextLong();
            }
        }

        // now the 3-d array
        boolean[][][] booleanArray3 = new boolean[bool3size1][bool3size2][bool3size3];
        persistableObject.booleanArray3 = booleanArray3;
        for (int i = 0; i < bool3size1; i++) {
            for (int j = 0; j < bool3size2; j++) {
                for (int k = 0; k < bool3size3; k++) {
                    booleanArray3[i][j][k] = rng.nextBoolean();
                }
            }
        }

        // now the list
        List<Integer> intList = new ArrayList<>();
        persistableObject.intList = intList;
        for (int i = 0; i < integerSize; i++) {
            intList.add(rng.nextInt());
        }

        return persistableObject;
    }

}
