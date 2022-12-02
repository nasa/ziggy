package gov.nasa.ziggy.module.hdf5;

import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

import java.util.Arrays;
import java.util.Random;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class HyperslabReadWriteTest {

    // dimensions
    private final static int N_ROWS = 3000;
    private final static int N_COLS = 25391;
    private final static int HYPERSLAB_ROWS = (int) (N_ROWS * (float) 0.6);
    private final static String HDF5_FILE_NAME = "hdf5-test-file.h5";
    private final static String GROUP_NAME = "dataArray";

    private final static long[] arrayDims = { N_ROWS, N_COLS };

    public static void main(String[] args) {

        // construct a 2-d float array with some actual contents
        float[][] dataArray = new float[N_ROWS][N_COLS];
        Random r = new Random();
        for (int i = 0; i < N_ROWS; i++) {
            for (int j = 0; j < N_COLS; j++) {
                dataArray[i][j] = r.nextFloat();
            }
        }

        // construct the arrays for the hyperslabs
        long[] h1Start = { 0, 0 };
        long[] h1Count = { 1, 1 };
        long[] h1Block = { HYPERSLAB_ROWS, N_COLS };

        long[] h2Start = { HYPERSLAB_ROWS, 0 };
        long[] h2Count = { 1, 1 };
        long[] h2Block = { N_ROWS - HYPERSLAB_ROWS, N_COLS };

        long[] stride = { 1, 1 };
        long startTimeNanoseconds, endTimeNanoseconds;
        long elapsedTimeNanoseconds;
        float elapsedTimeSeconds;

        // create the file, the group, the dataspace
        H5.H5Pset_libver_bounds(HDF5Constants.H5P_FILE_ACCESS_DEFAULT, HDF5Constants.H5F_LIBVER_V18,
            HDF5Constants.H5F_LIBVER_V18);
        long fileId = H5.H5Fcreate(HDF5_FILE_NAME, HDF5Constants.H5F_ACC_TRUNC, H5P_DEFAULT,
            H5P_DEFAULT);
        long groupId = H5.H5Gcreate(fileId, GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        long dataSpaceId = H5.H5Screate_simple(arrayDims.length, arrayDims, null);

        // create the property for chunking and compression
        long deflateProperty = H5.H5Pcreate(HDF5Constants.H5P_DATASET_CREATE);
        H5.H5Pset_chunk(deflateProperty, arrayDims.length, arrayDims);
        // H5.H5Pset_deflate(deflateProperty, 2);

        // create the dataset
        long dataSetId = H5.H5Dcreate(groupId, GROUP_NAME, HDF5Constants.H5T_NATIVE_FLOAT,
            dataSpaceId, H5P_DEFAULT, deflateProperty, H5P_DEFAULT);

        // hyperslab and memory space -- slab1
        H5.H5Sselect_hyperslab(dataSpaceId, HDF5Constants.H5S_SELECT_SET, h1Start, stride, h1Count,
            h1Block);
        long memSpaceId = H5.H5Screate_simple(h1Block.length, h1Block, null);

        // array -- slab1
        float[][] dataSlab1 = new float[HYPERSLAB_ROWS][N_COLS];
        for (int i = 0; i < HYPERSLAB_ROWS; i++) {
            dataSlab1[i] = dataArray[i];
        }

        // write slab 1 and close the slab1-specific HDF5 objects
        startTimeNanoseconds = System.nanoTime();
        System.out.print("Writing hyperslab1 ... ");
        H5.H5Dwrite(dataSetId, HDF5Constants.H5T_NATIVE_FLOAT, memSpaceId, dataSpaceId, H5P_DEFAULT,
            dataSlab1);
        H5.H5Sclose(memSpaceId);
        endTimeNanoseconds = System.nanoTime();
        elapsedTimeNanoseconds = endTimeNanoseconds - startTimeNanoseconds;
        elapsedTimeSeconds = elapsedTimeNanoseconds / (float) 1e9;
        System.out.println("done after " + elapsedTimeSeconds + " seconds");

        // hyperslab and memory space -- slab2
        H5.H5Sselect_hyperslab(dataSpaceId, HDF5Constants.H5S_SELECT_SET, h2Start, stride, h2Count,
            h2Block);
        memSpaceId = H5.H5Screate_simple(h2Block.length, h2Block, null);

        // array -- slab2
        float[][] dataSlab2 = new float[N_ROWS - HYPERSLAB_ROWS][N_COLS];
        for (int i = HYPERSLAB_ROWS; i < N_ROWS; i++) {
            dataSlab2[i - HYPERSLAB_ROWS] = dataArray[i];
        }

        // write slab 2 and close the slab1-specific HDF5 objects
        startTimeNanoseconds = System.nanoTime();
        System.out.print("Writing hyperslab2 ... ");
        H5.H5Dwrite(dataSetId, HDF5Constants.H5T_NATIVE_FLOAT, memSpaceId, dataSpaceId, H5P_DEFAULT,
            dataSlab2);
        H5.H5Sclose(memSpaceId);
        endTimeNanoseconds = System.nanoTime();
        elapsedTimeNanoseconds = endTimeNanoseconds - startTimeNanoseconds;
        elapsedTimeSeconds = elapsedTimeNanoseconds / (float) 1e9;
        System.out.println("done after " + elapsedTimeSeconds + " seconds");

        // close everything else
        H5.H5Dclose(dataSetId);
        H5.H5Sclose(dataSpaceId);
        H5.H5Pclose(deflateProperty);
        H5.H5Gclose(groupId);
        H5.H5Fclose(fileId);

        // part 2: read the hyperslabs back in
        fileId = H5.H5Fopen(HDF5_FILE_NAME, HDF5Constants.H5F_ACC_RDONLY, H5P_DEFAULT);
        groupId = H5.H5Gopen(fileId, GROUP_NAME, H5P_DEFAULT);
        dataSetId = H5.H5Dopen(groupId, GROUP_NAME, H5P_DEFAULT);
        long hdf5TypeInt = H5.H5Dget_type(dataSetId);
        dataSpaceId = H5.H5Dget_space(dataSetId);
        int nDims = H5.H5Sget_simple_extent_ndims(dataSpaceId);
        long[] dimensions = new long[nDims];
        long[] maxDimensions = new long[nDims];
        H5.H5Sget_simple_extent_dims(dataSpaceId, dimensions, maxDimensions);

        // hyperslab and memory space -- slab1
        H5.H5Sselect_hyperslab(dataSpaceId, HDF5Constants.H5S_SELECT_SET, h1Start, stride, h1Count,
            h1Block);
        memSpaceId = H5.H5Screate_simple(h1Block.length, h1Block, null);

        // array -- slab1
        float[][] dataSlab3 = new float[HYPERSLAB_ROWS][N_COLS];

        // read and close the memspace
        startTimeNanoseconds = System.nanoTime();
        System.out.print("Reading hyperslab1 ... ");
        H5.H5Dread(dataSetId, hdf5TypeInt, memSpaceId, dataSpaceId, H5P_DEFAULT, dataSlab3);
        H5.H5Sclose(memSpaceId);
        endTimeNanoseconds = System.nanoTime();
        elapsedTimeNanoseconds = endTimeNanoseconds - startTimeNanoseconds;
        elapsedTimeSeconds = elapsedTimeNanoseconds / (float) 1e9;
        System.out.println("done after " + elapsedTimeSeconds + " seconds");
        boolean goodCopy = Arrays.deepEquals(dataSlab1, dataSlab3);
        System.out.println("Copy status of slab 1:" + goodCopy);

        // hyperslab and memory space -- slab2
        H5.H5Sselect_hyperslab(dataSpaceId, HDF5Constants.H5S_SELECT_SET, h2Start, stride, h2Count,
            h2Block);
        memSpaceId = H5.H5Screate_simple(h2Block.length, h2Block, null);
        // array -- slab2
        float[][] dataSlab4 = new float[N_ROWS - HYPERSLAB_ROWS][N_COLS];

        // read and close the memspace
        startTimeNanoseconds = System.nanoTime();
        System.out.print("Reading hyperslab2 ... ");
        H5.H5Dread(dataSetId, hdf5TypeInt, memSpaceId, dataSpaceId, H5P_DEFAULT, dataSlab4);
        H5.H5Sclose(memSpaceId);
        endTimeNanoseconds = System.nanoTime();
        elapsedTimeNanoseconds = endTimeNanoseconds - startTimeNanoseconds;
        elapsedTimeSeconds = elapsedTimeNanoseconds / (float) 1e9;
        System.out.println("done after " + elapsedTimeSeconds + " seconds");
        goodCopy = Arrays.deepEquals(dataSlab2, dataSlab4);
        System.out.println("Copy status of slab 2:" + goodCopy);

        // close everything else
        H5.H5Dclose(dataSetId);
        H5.H5Sclose(dataSpaceId);
        H5.H5Gclose(groupId);
        H5.H5Fclose(fileId);
    }

}
