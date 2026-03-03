package gov.nasa.ziggy.pipeline.step.remote.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

public class SupportedBatchSystemTest {

    @Test
    public void testValidValues() {
        List<SupportedBatchSystem> validValues = SupportedBatchSystem.validValues();
        assertTrue(validValues.contains(SupportedBatchSystem.PBS));
        assertFalse(validValues.contains(SupportedBatchSystem.NONE));

        // If you've added a new entry to SupportedBatchSystem, you'll need to update this
        // assertion. While you're at it, figure out whether you need to add the new entry
        // to the excluded SupportedBatchSystem enums in the validValues() method.
        assertEquals(1, validValues.size());
    }
}
