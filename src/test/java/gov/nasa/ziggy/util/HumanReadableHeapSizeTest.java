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
        assertEquals(1000.001, new HumanReadableHeapSize(1000001).getHumanReadableHeapSize(),
            0.0001);
    }

    @Test
    public void testGetHeapSizeUnit() {
        assertEquals(HeapSizeUnit.MB, new HumanReadableHeapSize(0).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.MB, new HumanReadableHeapSize(0.1F).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.GB, new HumanReadableHeapSize(999).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.TB, new HumanReadableHeapSize(1000).getHeapSizeUnit());
        assertEquals(HeapSizeUnit.TB, new HumanReadableHeapSize(1000000).getHeapSizeUnit());
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
        assertEquals(0, new HumanReadableHeapSize(0).heapSizeGigabytes(), 1e-9);
        assertEquals(1, new HumanReadableHeapSize(1).heapSizeGigabytes(), 1e-6);
        assertEquals(999, new HumanReadableHeapSize(999).heapSizeGigabytes(), 1e-6);
        assertEquals(1000, new HumanReadableHeapSize(1000).heapSizeGigabytes(), 1e-6);
        assertEquals(1001, new HumanReadableHeapSize(1001).heapSizeGigabytes(), 1e-4);
        assertEquals(999999, new HumanReadableHeapSize(999999).heapSizeGigabytes(), 1e-3);
        assertEquals(1000000, new HumanReadableHeapSize(1000000).heapSizeGigabytes(), 1e-3);
        // TODO Determine why 1000001 fails and 1000002 passes
        // assertEquals(1000001, new HumanReadableHeapSize(1000001).heapSizeMb());
        assertEquals(1000002, new HumanReadableHeapSize(1000002).heapSizeGigabytes(), 1e-3);
    }

    @Test
    public void testToString() {
        assertEquals("0.0 MB", new HumanReadableHeapSize(0).toString());
        assertEquals("100.0 MB", new HumanReadableHeapSize(0.1F).toString());
        assertEquals("999.0 GB", new HumanReadableHeapSize(999).toString());
        assertEquals("1.0 TB", new HumanReadableHeapSize(1000).toString());
        assertEquals("1000.0 TB", new HumanReadableHeapSize(1000000).toString());
        assertEquals("1000.001 TB", new HumanReadableHeapSize(1000001).toString());
    }
}
