package gov.nasa.ziggy.report;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the {@link TopNList} class.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TopNListTest {
    private final static int MAX_LIST_LENGTH = 5;

    @Test
    public void testShortUnorderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 3, 2, 1);
        String actual = actualList.toString();
        String expected = "[3.0, 2.0, 1.0]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testShortOrderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 1, 2, 3);
        String actual = actualList.toString();
        String expected = "[3.0, 2.0, 1.0]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testLongUnorderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 7, 3, 4, 1, 9, 2, 5, 8, 6);
        String actual = actualList.toString();
        String expected = "[9.0, 8.0, 7.0, 6.0, 5.0]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testLongOrderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        String actual = actualList.toString();
        String expected = "[10.0, 9.0, 8.0, 7.0, 6.0]";

        assertEquals("list", expected, actual);
    }

    private TopNList generateList(int listMaxLength, int... values) {
        TopNList l = new TopNList(listMaxLength);
        for (int i : values) {
            l.add(i, "i=" + i);
        }
        return l;
    }

    @Test
    public void testMillisToHumanReadable() {
        TopNList topTen = createTopNList(new double[] { 1, 2, 3 });
        TopNList humanReadableTopTen = topTen.toHumanReadable(1000);
        assertEquals("[0.003, 0.002, 0.001]", humanReadableTopTen.toString());

        topTen = createTopNList(new double[] { 1000, 2000, 3000 });
        humanReadableTopTen = topTen.toHumanReadable(1000);
        assertEquals("[3.0, 2.0, 1.0]", humanReadableTopTen.toString());

        topTen = createTopNList(new double[] { 60000, 120000, 180000 });
        humanReadableTopTen = topTen.toHumanReadable(60000);
        assertEquals("[3.0, 2.0, 1.0]", humanReadableTopTen.toString());

        topTen = createTopNList(new double[] { 3600000, 7200000, 10800000 });
        humanReadableTopTen = topTen.toHumanReadable(3600000);
        assertEquals("[3.0, 2.0, 1.0]", humanReadableTopTen.toString());
    }

    private TopNList createTopNList(double[] values) {
        TopNList topTen = new TopNList(values.length);
        int i = 0;
        for (double value : values) {
            topTen.add((long) value, Integer.toString(i++));
        }
        return topTen;
    }
}
