package gov.nasa.ziggy.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Test;

/**
 * Unit tests for the HyperRectangleIterator class.
 *
 * @author PT
 */
public class HyperRectangleIteratorTest {

    /**
     * Basic constructor test
     */
    @Test
    public void testConstructor() {
        HyperRectangleIterator hi = new HyperRectangleIterator(new int[] { 6, 5, 6 }, 17);
        assertTrue(Arrays.equals(new int[] { 6, 5, 6 }, hi.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hi.getCounter()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 6 }, hi.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 6 }, hi.getLastSize()));
        assertTrue(Arrays.equals(new int[] { 6, 3, 1 }, hi.getnRectangles()));
        assertEquals(17, hi.getMaxElementsPerHyperRectangle());
    }

    /**
     * Test that array size with a dimension of zero length throws an exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidArraySize() {
        new HyperRectangleIterator(new int[] { 6, 5, 0 }, 17);
    }

    /**
     * Test that max elements == 0 throws an exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxElements() {
        new HyperRectangleIterator(new int[] { 6, 5, 6 }, 0);
    }

    /**
     * Test the hasNext() and next() methods.
     */
    @Test
    public void testNext() {
        HyperRectangleIterator hi = new HyperRectangleIterator(new int[] { 2, 5, 6 }, 17);

        assertTrue(hi.hasNext());
        HyperRectangle hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 0, 0, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 0, 1, 0 }, hi.getCounter()));

        assertTrue(hi.hasNext());
        hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 0, 2, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 0, 2, 0 }, hi.getCounter()));

        assertTrue(hi.hasNext());
        hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 0, 4, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 1, 0, 0 }, hi.getCounter()));

        assertTrue(hi.hasNext());
        hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 0, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 0 }, hi.getCounter()));

        assertTrue(hi.hasNext());
        hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 1, 2, 0 }, hi.getCounter()));

        assertTrue(hi.hasNext());
        hr = hi.next();
        assertTrue(Arrays.equals(new int[] { 2, 5, 6 }, hr.getFullArraySize()));
        assertTrue(Arrays.equals(new int[] { 1, 1, 6 }, hr.getSize()));
        assertTrue(Arrays.equals(new int[] { 1, 4, 0 }, hr.getOffset()));
        assertTrue(Arrays.equals(new int[] { 2, 0, 0 }, hi.getCounter()));

        assertFalse(hi.hasNext());
    }

    /**
     * Ensure that when there is no next HyperRectangle, and next() is called, the correct exception
     * is thrown
     */
    @Test(expected = NoSuchElementException.class)
    public void testNextFailure() {
        HyperRectangleIterator hi = new HyperRectangleIterator(new int[] { 2, 5, 6 }, 17);
        for (int i = 0; i < 7; i++) {
            hi.next();
        }

    }
}
