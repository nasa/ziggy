package gov.nasa.ziggy.module.hdf5;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Provides unit tests for the {@link AbstractHdf5Array} static methods that produce a new instance.
 *
 * @author PT
 */
public class AbstractHdf5ArrayTest {

    @Test
    public void testPersistableObject() {
        PersistableSample1 pTest1 = PersistableSample1.newInstance(1, 1, 1, 1, 1, 1, 1);
        AbstractHdf5Array a1 = AbstractHdf5Array.newInstance(pTest1);
        assertTrue(a1 instanceof PersistableHdf5Array);
    }

    @Test
    public void testPrimitiveObject() {
        int[][] intArray = new int[3][4];
        AbstractHdf5Array a1 = AbstractHdf5Array.newInstance(intArray);
        assertTrue(a1 instanceof PrimitiveHdf5Array);
    }

    @Test
    public void testPersistableField() throws NoSuchFieldException, SecurityException {
        AbstractHdf5Array a1 = AbstractHdf5Array
            .newInstance(PersistableSample2.class.getDeclaredField("persistableScalar1"));
        assertTrue(a1 instanceof PersistableHdf5Array);
    }

    @Test
    public void testPrimitiveField() throws NoSuchFieldException, SecurityException {
        AbstractHdf5Array a1 = AbstractHdf5Array
            .newInstance(PersistableSample1.class.getDeclaredField("floatArray1"));
        assertTrue(a1 instanceof PrimitiveHdf5Array);
    }

    @Test
    public void testNull() {
        AbstractHdf5Array a1 = AbstractHdf5Array.newInstance(null);
        assertNull(a1);

        List<Integer> emptyList = new ArrayList<>();
        AbstractHdf5Array a2 = AbstractHdf5Array.newInstance(emptyList);
        assertNull(a2);
    }
}
