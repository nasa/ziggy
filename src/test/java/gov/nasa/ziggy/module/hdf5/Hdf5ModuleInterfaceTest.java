package gov.nasa.ziggy.module.hdf5;

import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.module.PipelineException;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Unit test class for the {@link Hdf5ModuleInterface} class.
 *
 * @author PT
 */
public class Hdf5ModuleInterfaceTest {

    public long fileId;
    public Hdf5ModuleInterface moduleInterface = new Hdf5ModuleInterface();
    public File hdf5File;
    int[][][] intArrayTestField;
    boolean[] booleanArrayTestField;
    String[] stringArrayTestField;
    String stringTestField;
    double doubleScalar;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() throws HDF5LibraryException, NullPointerException, IOException {

        // create an HDF5 file in the temporary folder
        hdf5File = directoryRule.directory().resolve("hdf5TestFile.h5").toFile();
        fileId = H5.H5Fcreate(hdf5File.getAbsolutePath(), HDF5Constants.H5F_ACC_TRUNC, H5P_DEFAULT,
            H5P_DEFAULT);
    }

    @After
    public void tearDown() throws HDF5LibraryException {

        // close the file
        H5.H5Fclose(fileId);
    }

    public PersistableSample2 generatePersistableTest2Object() {
        PersistableSample2 persistableTest2 = new PersistableSample2();
        persistableTest2.intScalar = 500;
        persistableTest2.persistableArray2 = new PersistableSample1[3][2];
        for (int i = 0; i < persistableTest2.persistableArray2.length; i++) {
            persistableTest2.persistableArray2[i][0] = PersistableSample1.newInstance(5, 6, 7, 8, 9,
                10, 11);
            persistableTest2.persistableArray2[i][1] = PersistableSample1.newInstance(12, 13, 14,
                15, 16, 17, 18);
        }
        persistableTest2.persistableList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            persistableTest2.persistableList
                .add(PersistableSample1.newInstance(1, 2, 3, 4, 5, 6, 7));
        }
        persistableTest2.persistableScalar1 = PersistableSample1.newInstance(2, 2, 2, 2, 2, 2, 2);
        return persistableTest2;
    }

    /**
     * Tests the top-level HDF5 file reader and writer method.
     *
     * @throws HDF5LibraryException
     * @throws IOException
     */
    @Test
    public void testWriteAndReadFile() throws HDF5LibraryException, IOException {

        hdf5File = directoryRule.directory().resolve("hdf5WriteTestFile.h5").toFile();
        PersistableSample2 persistableTest2 = generatePersistableTest2Object();
        moduleInterface.writeFile(hdf5File, persistableTest2, false);
        PersistableSample2 recoveredTestValues = new PersistableSample2();
        recoveredTestValues.ignoreThisField = 0;
        boolean missingFieldsDetected = moduleInterface.readFile(hdf5File, recoveredTestValues,
            true);
        assertTrue(missingFieldsDetected);

        // there should be one field from the original that's not in the read-in version, because
        // of the ProxyIgnore annotation

        assertEquals(0.0, recoveredTestValues.ignoreThisField, 0);

        recoveredTestValues.ignoreThisField = 11.5;
        assertTrue(persistableTest2.equals(recoveredTestValues));

        // now do it with arg 3 of the writer set to true

        hdf5File = directoryRule.directory().resolve("hdf5WriteTestFile2.h5").toFile();
        persistableTest2 = generatePersistableTest2Object();
        moduleInterface.writeFile(hdf5File, persistableTest2, true);
        recoveredTestValues = new PersistableSample2();
        recoveredTestValues.ignoreThisField = 0;
        missingFieldsDetected = moduleInterface.readFile(hdf5File, recoveredTestValues, true);
        assertFalse(missingFieldsDetected);
        assertEquals(0.0, recoveredTestValues.ignoreThisField, 0);

        recoveredTestValues.ignoreThisField = 11.5;
        assertTrue(persistableTest2.equals(recoveredTestValues));
    }

    @Test(expected = PipelineException.class)
    public void testErrorOnFileWithMissingField() throws IOException {
        hdf5File = directoryRule.directory().resolve("hdf5WriteTestFile.h5").toFile();
        PersistableSample2 persistableTest2 = generatePersistableTest2Object();
        moduleInterface.writeFile(hdf5File, persistableTest2, false);
        PersistableSample2 recoveredTestValues = new PersistableSample2();
        recoveredTestValues.ignoreThisField = 0;
        moduleInterface.readFile(hdf5File, recoveredTestValues, false);
    }
}
