package gov.nasa.ziggy.data.datastore;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Unit tests for {@link DataFileType} class.
 *
 * @author PT
 */
public class DataFileTypeTest {

    @Rule
    public final ZiggyPropertyRule ziggyPropertyRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, "");

    @Test
    public void testGetConvenienceFields() {

        // First case: the fileNameRegexp has no location parts in it.
        Map<String, DataFileType> dataFileTypesByName = DatastoreTestUtils.dataFileTypesByName();
        DataFileType dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertEquals(dataFileType.getFileNameRegexp(),
            DatastoreWalker.fileNameRegexpBaseName(dataFileType));
        assertEquals(dataFileType.getLocation(), DatastoreWalker.fullLocation(dataFileType));

        // Second case: the fileNameRegexp has location parts in it.
        dataFileTypesByName = DatastoreTestUtils.dataFileTypesByNameRegexpsInFileName();
        dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertEquals(dataFileType.getLocation() + "/pixelType$science/channel",
            DatastoreWalker.fullLocation(dataFileType));
        assertEquals("(uncalibrated-pixels-[0-9]+)\\.science\\.nc",
            DatastoreWalker.fileNameRegexpBaseName(dataFileType));
    }
}
