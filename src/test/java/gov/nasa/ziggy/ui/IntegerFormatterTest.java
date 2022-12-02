package gov.nasa.ziggy.ui;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.util.IntegerFormatter;

public class IntegerFormatterTest {

    @Test
    public void testEngineeringNotation() {
        assertEquals("14", IntegerFormatter.engineeringNotation(14));
        assertEquals("-233", IntegerFormatter.engineeringNotation(-233));
        assertEquals("1.23 k", IntegerFormatter.engineeringNotation(1230));
        assertEquals("-22.5 k", IntegerFormatter.engineeringNotation(-22500));
        assertEquals("223 k", IntegerFormatter.engineeringNotation(223000));
        assertEquals("223 M", IntegerFormatter.engineeringNotation(223000000));
        assertEquals("223 G", IntegerFormatter.engineeringNotation(223000000000L));
        assertEquals("223 T", IntegerFormatter.engineeringNotation(223000000000000L));
        assertEquals("223 P", IntegerFormatter.engineeringNotation(223000000000000000L));
        assertEquals("2.23 E", IntegerFormatter.engineeringNotation(2230000000000000000L));

    }
}
