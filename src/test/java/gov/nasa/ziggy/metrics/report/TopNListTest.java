package gov.nasa.ziggy.metrics.report;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Todd Klaus
 */
public class TopNListTest {
    private final static int MAX_LIST_LENGTH = 5;

    @Test
    public void testShortUnorderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 3, 2, 1);
        String actual = actualList.toString();
        String expected = "[3, 2, 1]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testShortOrderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 1, 2, 3);
        String actual = actualList.toString();
        String expected = "[3, 2, 1]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testLongUnorderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 7, 3, 4, 1, 9, 2, 5, 8, 6);
        String actual = actualList.toString();
        String expected = "[9, 8, 7, 6, 5]";

        assertEquals("list", expected, actual);
    }

    @Test
    public void testLongOrderedList() {
        TopNList actualList = generateList(MAX_LIST_LENGTH, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        String actual = actualList.toString();
        String expected = "[10, 9, 8, 7, 6]";

        assertEquals("list", expected, actual);
    }

    private TopNList generateList(int listMaxLength, int... values) {
        TopNList l = new TopNList(listMaxLength);
        for (int i : values) {
            l.add(i, "i=" + i);
        }
        return l;
    }
}
