package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class CollectionFiltersTest {

    @Test
    public void testFilterToList() {
        List<Object> list = List.of(new Object(), new Date(), Double.valueOf(1.0),
            Integer.valueOf(1));
        assertEquals(List.of(new Date()), CollectionFilters.filterToList(list, Date.class));
        assertEquals(List.of(Double.valueOf(1.0), Integer.valueOf(1)),
            CollectionFilters.filterToList(list, Number.class));
    }

    @Test
    public void testFilterToSet() {
        Set<Object> set = Set.of(new Object(), new Date(), Double.valueOf(1.0), Integer.valueOf(1));
        assertEquals(Set.of(new Date()), CollectionFilters.filterToSet(set, Date.class));
        assertEquals(Set.of(Double.valueOf(1.0), Integer.valueOf(1)),
            CollectionFilters.filterToSet(set, Number.class));
    }

    @Test
    public void testRemoveTypeFromCollection() {
        List<Object> list = new ArrayList<>();
        Object object = new Object();
        list.add(object);
        Date date = new Date();
        list.add(date);
        list.add(Double.valueOf(1.0));
        list.add(Integer.valueOf(1));
        CollectionFilters.removeTypeFromCollection(list, Number.class);
        assertEquals(List.of(object, date), list);
        CollectionFilters.removeTypeFromCollection(list, Date.class);
        assertEquals(List.of(object), list);
    }
}
