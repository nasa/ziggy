package gov.nasa.ziggy.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

/**
 * Unit tests for HyperRectangle class.
 *
 * @author PT
 */
public class HyperRectangleTest {

    /**
     * Basic constructor test.
     */
    @Test
    public void testConstructor() {
        int[] aSize = new int[] { 3, 4, 6 };
        int[] size = new int[] { 1, 2, 6 };
        int[] offset = new int[] { 1, 2, 0 };
        HyperRectangle hr = new HyperRectangle(aSize, size, offset);
        assertEquals(aSize, hr.getFullArraySize());
        assertEquals(size, hr.getSize());
        assertEquals(offset, hr.getOffset());
    }

    // the rest of the tests exercise assorted conditions that cause the constructor's illegal
    // argument exception.

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSizeDims() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2 }, new int[] { 1, 2, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOffsetDims() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 6 }, new int[] { 1, 2 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFullArraySize() {
        new HyperRectangle(new int[] { 3, 4, 0 }, new int[] { 1, 2, 6 }, new int[] { 1, 2, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSize() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 0 }, new int[] { 1, 2, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOffset() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 6 }, new int[] { 1, 2, -1 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExcessiveSize() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 7 }, new int[] { 1, 2, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExcessiveOffset() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 6 }, new int[] { 1, 5, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidShape() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 1 }, new int[] { 1, 2, 0 });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTrailingOffsets() {
        new HyperRectangle(new int[] { 3, 4, 6 }, new int[] { 1, 2, 6 }, new int[] { 1, 2, 1 });

    }

    @Test
    public void testOffsetCombination() {
        int[] aSize = new int[] { 3, 4, 6 };
        int[] size = new int[] { 1, 2, 6 };
        int[] offset = new int[] { 1, 2, 0 };
        HyperRectangle hr = new HyperRectangle(aSize, size, offset);
        int[] o2 = hr.getOffset(new int[] { 0, 1, 2 });
        assertTrue(Arrays.equals(new int[] { 1, 3, 2 }, o2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOffsetCombinationError() {
        int[] aSize = new int[] { 3, 4, 6 };
        int[] size = new int[] { 1, 2, 6 };
        int[] offset = new int[] { 1, 2, 0 };
        HyperRectangle hr = new HyperRectangle(aSize, size, offset);
        hr.getOffset(new int[] { 1, 2 });
    }

}
