package gov.nasa.ziggy.collections;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for HyperRectangle. This allows iteration of hyper-rectangular sections of a
 * multi-dimensional non-ragged array.
 *
 * @author PT
 */
public class HyperRectangleIterator implements Iterator<HyperRectangle> {

    /**
     * Size of the standard hyper-rectangle
     */
    private final int[] size;

    /**
     * Size of the last hyper-rectangle -- this can be different from the standard one if the array
     * size cannot be divided into a whole number of standard hyper-rectangles
     */
    private final int[] lastSize;

    /**
     * Number of hyper-rectangles along each dimension
     */
    private final int[] nRectangles;

    /**
     * Size of the full array that gets subdivided into hyper-rectangles
     */
    private final int[] fullArraySize;

    /**
     * Maximum number of array elements that are permitted per hyper-rectangle
     */
    private final int maxElementsPerHyperRectangle;

    /**
     * Counts the # of slabs per dimension as we proceed through the array
     */
    private int[] counter;

    public HyperRectangleIterator(int[] fullArraySize, int maxElementsPerHyperRectangle) {
        this.fullArraySize = fullArraySize;
        for (int element : fullArraySize) {
            if (element <= 0) {
                throw new IllegalArgumentException(
                    "Arrays with zero-length dimensions are not permitted.");
            }
        }
        this.maxElementsPerHyperRectangle = maxElementsPerHyperRectangle;
        if (maxElementsPerHyperRectangle <= 0) {
            throw new IllegalArgumentException(
                "Number of elements per hyper-rectangle must be positive");
        }
        size = size();
        nRectangles = nRectangles();
        lastSize = lastSize();
        counter = new int[lastSize.length];
        Arrays.fill(counter, 0);
    }

    /**
     * Determine the size of the nominal hyper-rectangle for this array, ensuring that (a) the
     * maximum number of elements per hyper-rectangle is less than or equal to
     * maxElementsPerHyperRectangle; (b) the hyper-rectangle divides the array along one and only
     * one dimension; (c) all dimensions lower than the division dimension have 1 "row" per
     * hyper-rectangle; (d) all dimensions higher than the division dimension have a number of
     * "columns" per hyper-rectangle equal to that of the parent array. For example, for a 3 x 4 x 5
     * array, where the max # of elements is 10, will have a standard hyper-rectangle size of 1 x 2
     * x 5. If the max # of elements is 2, then the standard hyper-rectangle will be 1 x 1 x 2.
     */
    private int[] size() {
        int[] nominalSize = new int[fullArraySize.length];

        // pre-initialize the nominal slab size to be the full array size
        for (int iDim = 0; iDim < fullArraySize.length; iDim++) {
            nominalSize[iDim] = fullArraySize[iDim];
        }

        // loop over dimensions to find the dimension where we need to apply the division into
        // slabs, if any

        for (int iDim = 0; iDim < fullArraySize.length; iDim++) {

            // determine the number of elements in the array below this level (i.e., the number
            // of elements per "row" in a generalized sense)
            long nElemPerRow = 1;
            for (int jDim = iDim + 1; jDim < fullArraySize.length; jDim++) {
                nElemPerRow *= fullArraySize[jDim];
            }

            // determine the number of rows that can be accommodated within the desired max
            // number of elements
            long nRows = Math.min(maxElementsPerHyperRectangle / nElemPerRow, fullArraySize[iDim]);

            // if nRows is zero, it means that the # of bytes per row is too large, so we have
            // to perform the subdivision into hyperslabs at a higher dimension, and this dimension
            // we should do 1 per hyperslab; alternately, if nRows > 0, we are done with this
            // activity and can return
            if (nRows != 0) {
                nominalSize[iDim] = (int) nRows;
                break;
            }
            nominalSize[iDim] = 1;
        }
        return nominalSize;
    }

    /**
     * Determine the number of hyper-rectangles in each dimension needed to read/write the array.
     *
     * @return number of slabs in each dimension needed to write the array, this is the dimension
     * size / slab size, plus 1 if there is a remainder.
     */
    private int[] nRectangles() {
        int[] nSlabsPerDim = new int[fullArraySize.length];
        for (int iDim = 0; iDim < fullArraySize.length; iDim++) {

            // # of slabs is size of the dimension / size of the slab
            nSlabsPerDim[iDim] = fullArraySize[iDim] / size[iDim];

            // if there's some remainder, then we need 1 more slab
            if (fullArraySize[iDim] % size[iDim] != 0) {
                nSlabsPerDim[iDim] += 1;
            }
        }
        return nSlabsPerDim;
    }

    /**
     * Determine the size of the last hyper-rectangle in each dimension -- this can be different
     * from the size of the nominal one if the size of the nominal one does not evenly divide the
     * array size (for example if array is 3 x 4 x 5 and nominal hyper-rectangle is 1 x 3 x 5, then
     * the last slab in dim 1 has size 1, the last in dim 2 has size 1, and the last in dim 3 has
     * size 5)
     *
     * @return size of the final slab in each dimension
     */
    int[] lastSize() {
        int[] lastSlabSize = new int[fullArraySize.length];
        for (int iDim = 0; iDim < fullArraySize.length; iDim++) {
            int remainder = fullArraySize[iDim] % size[iDim];
            lastSlabSize[iDim] = remainder == 0 ? size[iDim] : remainder;
        }
        return lastSlabSize;
    }

    @Override
    public boolean hasNext() {

        // when every hyper-rectangle has been processed, the first entry in nRectangles
        // will be equal to the # of hyper-rectangles in that direction, thanks to
        // zero-based indexing
        return counter[0] < nRectangles[0];
    }

    @Override
    public HyperRectangle next() {

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // construct arrays for use in instantiating a HyperRectangle:
        int[] slabSize = new int[counter.length];
        int[] location = new int[counter.length];

        // populate the arrays
        for (int iDim = 0; iDim < counter.length; iDim++) {
            location[iDim] = counter[iDim] * size[iDim];
            if (counter[iDim] == nRectangles[iDim] - 1) {// last slab in this direction
                slabSize[iDim] = lastSize[iDim];
            } else {
                slabSize[iDim] = size[iDim];
            }
        }

        // construct the HyperRectangle object
        HyperRectangle slab = new HyperRectangle(fullArraySize, slabSize, location);

        // update the counters -- update the fastest-moving index until we run out
        // of values, at that point set it to zero and update the next-fastest-moving; when
        // the zeroth index is up to its max value, stop updating (this will signal hasNext()
        // to return false)
        for (int iDim = counter.length - 1; iDim >= 0; iDim--) {
            counter[iDim]++;
            if (counter[iDim] != nRectangles[iDim] || iDim <= 0) {
                break;
            }
            counter[iDim] = 0;
        }
        return slab;
    }

    // getters
    public int[] getSize() {
        return size;
    }

    public int[] getLastSize() {
        return lastSize;
    }

    public int[] getnRectangles() {
        return nRectangles;
    }

    public int[] getFullArraySize() {
        return fullArraySize;
    }

    public int getMaxElementsPerHyperRectangle() {
        return maxElementsPerHyperRectangle;
    }

    public int[] getCounter() {
        return counter;
    }
}
