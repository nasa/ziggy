package gov.nasa.ziggy.collections;

import java.util.Arrays;

/**
 * General representation of a region within a non-ragged, multi-dimensional array.
 *
 * @author PT
 */
public class HyperRectangle {

    /**
     * Size of the parent array.
     */
    final int[] fullArraySize;

    /**
     * Size of the hyper-rectangle.
     */
    final int[] size;

    /**
     * Location of the hyper-rectangle within the parent array.
     */
    final int[] offset;

    public HyperRectangle(int[] fullArraySize, int[] size, int[] offset) {
        this.fullArraySize = fullArraySize;
        this.size = size;
        this.offset = offset;
        if (!checkParameters()) {
            throw new IllegalArgumentException("Unable to obtain hyperslab of size "
                + Arrays.toString(size) + " and location " + Arrays.toString(offset)
                + " from array of size " + Arrays.toString(fullArraySize));
        }
    }

    /**
     * Tests that requested hyper-rectangle parameters are valid. Validity means the following: the
     * requested hyper-rectangle has the same number of parameters as the array; the size and
     * location requested fit into the array in all dimensions; the requested dimensions consist of
     * a series of singletons, followed by a dimension that is <= the corresponding size in the
     * source array, followed by dimensions that are == the corresponding sizes in the source array
     * (i.e., for an array that is [3][4][5][6], size of [1 1 2 6] is valid but not [1 1 2 1],
     * because all dimensions past the 3rd must be equal in the array and the hyper-rectangle).
     *
     * @return true if all conditions described above hold
     */
    private boolean checkParameters() {
        boolean parametersOkay = true;
        if (size.length != fullArraySize.length || size.length != offset.length) {
            return false;
        }

        boolean pastReducedDimensions = false;
        for (int iDim = 0; iDim < size.length; iDim++) {

            // super-basic -- dims must all be >= 1, locations must all be >= 0
            parametersOkay = parametersOkay && size[iDim] >= 1 && fullArraySize[iDim] >= 1
                && offset[iDim] >= 0;

            if (pastReducedDimensions) {
                parametersOkay = parametersOkay && size[iDim] == fullArraySize[iDim]
                    && offset[iDim] == 0;
            } else {
                parametersOkay = parametersOkay && size[iDim] <= fullArraySize[iDim]
                    && offset[iDim] + size[iDim] <= fullArraySize[iDim] && size[iDim] >= 1;
                if (size[iDim] > 1) {
                    pastReducedDimensions = true;
                }
            }
        }
        return parametersOkay;
    }

    /**
     * Combine the offset array of this HyperRectangle with an additional offset. The method does
     * not check that the sum of the combined offset + the HyperRectangle length is less than the
     * full array size, so that the HyperRectangle can be used to read data into a sub-region of an
     * array that is larger in total than the fullArraySize in the HyperRectangle.
     *
     * @param additionalOffset array of additional offset values
     * @return Sum of additionalOffset and the HyperRectangle's offset
     */
    public int[] getOffset(int[] additionalOffset) {
        if (additionalOffset.length != offset.length) {
            throw new IllegalArgumentException("Array of length " + additionalOffset.length
                + " cannot be argument for HyperRectangle with offset array of length "
                + offset.length);
        }
        int[] combinedOffset = new int[offset.length];
        for (int i = 0; i < combinedOffset.length; i++) {
            combinedOffset[i] = offset[i] + additionalOffset[i];
        }
        return combinedOffset;
    }

    // getters
    public int[] getSize() {
        return size;
    }

    public int[] getOffset() {
        return offset;
    }

    public int[] getFullArraySize() {
        return fullArraySize;
    }
}
