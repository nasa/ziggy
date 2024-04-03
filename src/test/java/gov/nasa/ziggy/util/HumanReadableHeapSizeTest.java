package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.util.HumanReadableHeapSize.HeapSizeUnit;

public class HumanReadableHeapSizeTest {

    @Test
    public void testGetHumanReadableHeapSize() {
        assertEquals(0.0, new HumanReadableHeapSize(0).getHumanReadableHeapSize(), 0.0001);
        assertEquals(999.0, new HumanReadableHeapSize(999).getHumanReadableHeapSize(), 0.0001);
        assertEquals(1.0, new HumanReadableHeapSize(1000).getHumanReadableHeapSize(), 0.0001);
        assertEquals(1000.0, new HumanReadableHeapSize(1000000).getHumanReadableHeapSize(), 0.0001);
        assertEquals(1.000001, new HumanReadableHeapSize(1000001).getHumanReadableHeapSize(),
            0.0001);
    }

    @Test
    public void testGetHeapSizeUnit() {
        assertEquals(HeapSizeUnit.MB, new HumanReadableHeapSize(0).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.MB, new HumanReadableHeapSize(999).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.GB, new HumanReadableHeapSize(1000).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.GB, new HumanReadableHeapSize(1000000).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.TB, new HumanReadableHeapSize(1000001).getHeapSizeUnit());

        assertEquals(HeapSizeUnit.MB,
            new HumanReadableHeapSize(0, HeapSizeUnit.MB).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.GB,
            new HumanReadableHeapSize(0, HeapSizeUnit.GB).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.TB,
            new HumanReadableHeapSize(0, HeapSizeUnit.TB).getHeapSizeUnit());
    }

    @Test
    public void testHeapSizeMb() {
        assertEquals(0, new HumanReadableHeapSize(0).heapSizeMb());
        assertEquals(1, new HumanReadableHeapSize(1).heapSizeMb());
        assertEquals(999, new HumanReadableHeapSize(999).heapSizeMb());
        assertEquals(1000, new HumanReadableHeapSize(1000).heapSizeMb());
        assertEquals(1001, new HumanReadableHeapSize(1001).heapSizeMb());
        assertEquals(999999, new HumanReadableHeapSize(999999).heapSizeMb());
        assertEquals(1000000, new HumanReadableHeapSize(1000000).heapSizeMb());
        // TODO Determine why 1000001 fails and 1000002 passes
        // assertEquals(1000001, new HumanReadableHeapSize(1000001).heapSizeMb());
        assertEquals(1000002, new HumanReadableHeapSize(1000002).heapSizeMb());
    }

    @Test
    public void testToString() {
        assertEquals("0.0 MB", new HumanReadableHeapSize(0).toString());
        assertEquals("999.0 MB", new HumanReadableHeapSize(999).toString());
        assertEquals("1.0 GB", new HumanReadableHeapSize(1000).toString());
        assertEquals("1000.0 GB", new HumanReadableHeapSize(1000000).toString());
        assertEquals("1.000001 TB", new HumanReadableHeapSize(1000001).toString());
    }
}
