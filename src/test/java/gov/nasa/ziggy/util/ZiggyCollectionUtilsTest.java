package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/** Unit tests for {@link ZiggyCollectionUtils}. */
public class ZiggyCollectionUtilsTest {

    @Test
    public void testElementsNotInSuperset() {

        Set<Integer> elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(null, null);
        assertTrue(elementsNotInSuperset.isEmpty());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(new HashSet<>(), null);
        assertTrue(elementsNotInSuperset.isEmpty());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(new HashSet<>(),
            new HashSet<>());
        assertTrue(elementsNotInSuperset.isEmpty());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(null, Set.of(1));
        assertTrue(elementsNotInSuperset.contains(1));
        assertEquals(1, elementsNotInSuperset.size());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(new HashSet<>(),
            Set.of(1));
        assertTrue(elementsNotInSuperset.contains(1));
        assertEquals(1, elementsNotInSuperset.size());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(Set.of(1, 2, 3),
            Set.of(1));
        assertTrue(elementsNotInSuperset.isEmpty());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(Set.of(1, 2, 3),
            Set.of(1, 4));
        assertTrue(elementsNotInSuperset.contains(4));
        assertEquals(1, elementsNotInSuperset.size());

        elementsNotInSuperset = ZiggyCollectionUtils.elementsNotInSuperset(Set.of(1, 2, 3),
            Set.of(5, 4));
        assertTrue(elementsNotInSuperset.containsAll(Set.of(5, 4)));
        assertEquals(2, elementsNotInSuperset.size());
    }
}
