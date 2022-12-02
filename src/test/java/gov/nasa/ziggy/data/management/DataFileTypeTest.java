package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit test class for DataFileType class.
 *
 * @author PT
 */
public class DataFileTypeTest {

    private DataFileType d;

    @Before
    public void setUp() {
        d = new DataFileType();
        d.setName("calibrated-pixels");
        d.setFileNameRegexForTaskDir(
            "sector-([0-9]{4})-readout-([ABCD])-ccd-([1234]:[1234])-calibrated-pixels.h5");
        d.setFileNameWithSubstitutionsForDatastore(
            "sector-$1/ccd-$3/cal/sector-$1/readout-$2/calibrated-pixels.h5");
    }

    @Test
    public void testFileNamePatternForTaskDir() {
        Pattern p = d.fileNamePatternForTaskDir();
        assertEquals("sector-([0-9]{4})-readout-([ABCD])-ccd-([1234]:[1234])-calibrated-pixels.h5",
            p.pattern());
    }

    @Test
    public void testFileNameRegexForDatastore() {
        String s = d.fileNameRegexForDatastore();
        assertEquals(
            "sector-([0-9]{4})/ccd-([1234]:[1234])/cal/sector-\\1/readout-([ABCD])/calibrated-pixels.h5",
            s);

        d = new DataFileType();
        d.setName("has backslashes");
        d.setFileNameRegexForTaskDir("(\\S+)-(set-[0-9])-(file-[0-9]).png");
        d.setFileNameWithSubstitutionsForDatastore("$2/L0/$1-$3.png");
        assertEquals("(set-[0-9])/L0/(\\S+)-(file-[0-9]).png", d.fileNameRegexForDatastore());
    }

    @Test
    public void testFileNamePatternForDatastore() {
        Pattern p = d.fileNamePatternForDatastore();
        assertEquals(
            "sector-([0-9]{4})/ccd-([1234]:[1234])/cal/sector-\\1/readout-([ABCD])/calibrated-pixels.h5",
            p.pattern());
    }

    @Test
    public void testFileNameInTaskDirMatches() {
        String goodMatch = "sector-1234-readout-A-ccd-1:2-calibrated-pixels.h5";
        assertTrue(d.fileNameInTaskDirMatches(goodMatch));
        String badMatch = "sector-123-readout-A-ccd-1:2-calibrated-pixels.h5";
        assertFalse(d.fileNameInTaskDirMatches(badMatch));

        d = new DataFileType();
        d.setName("has backslashes");
        d.setFileNameRegexForTaskDir("perm-(\\S+)-(set-[0-9])-(file-[0-9]).png");
        d.setFileNameWithSubstitutionsForDatastore("$2/L0/$1-$3.png");
        assertTrue(d.fileNameInTaskDirMatches("perm-nasa_logo-set-1-file-0.png"));

    }

    @Test
    public void testFileNameInDatastoreMatches() {
        String goodMatch = "sector-1234/ccd-1:2/cal/sector-1234/readout-A/calibrated-pixels.h5";
        assertTrue(d.fileNameInDatastoreMatches(goodMatch));
        String badMatch = "sector-123/ccd-1:2/cal/sector-1234/readout-A/calibrated-pixels.h5";
        assertFalse(d.fileNameInDatastoreMatches(badMatch));
    }

    @Test
    public void testDatastoreFileNameFromTaskDirFileName() {
        String s = d.datastoreFileNameFromTaskDirFileName(
            "sector-1234-readout-A-ccd-1:2-calibrated-pixels.h5");
        assertEquals("sector-1234/ccd-1:2/cal/sector-1234/readout-A/calibrated-pixels.h5", s);
    }

    @Test
    public void testTaskDirFileNameFromDatastoreFileName() {
        String s = d.taskDirFileNameFromDatastoreFileName(
            "sector-1234/ccd-1:2/cal/sector-1234/readout-A/calibrated-pixels.h5");
        assertEquals("sector-1234-readout-A-ccd-1:2-calibrated-pixels.h5", s);
    }

    @Test
    public void testGetDatastorePatternTruncatedToLevel() {
        Pattern p = d.getDatastorePatternTruncatedToLevel(2);
        assertEquals("sector-([0-9]{4})/ccd-([1234]:[1234])", p.pattern());
    }

    @Test
    public void testGetDatastorePatternWithLowLevelsTruncated() {
        Pattern p = d.getDatastorePatternWithLowLevelsTruncated(3);
        assertEquals("sector-([0-9]{4})/ccd-([1234]:[1234])/cal", p.pattern());
    }

    // Now for some tests that should cause exceptions to be thrown

    @Test(expected = IllegalStateException.class)
    public void testNonContiguousSubstitions() {
        d.setFileNameWithSubstitutionsForDatastore(
            "sector-$1/ccd-$4/cal/readout-$2/calibrated-pixels.h5");
        d.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testBadSubstitutionValues() {
        d.setFileNameWithSubstitutionsForDatastore(
            "sector-$2/ccd-$3/cal/readout-$4/calibrated-pixels.h5");
        d.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testRegexSubstitutionMismatch() {
        d.setFileNameWithSubstitutionsForDatastore("sector-$1/cal/readout-$2/calibrated-pixels.h5");
        d.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoName() {
        d.setName("");
        d.validate();
    }

}
