package gov.nasa.ziggy.util;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides utility methods for managing collections that contain objects of more than one type.
 *
 * @author PT
 */
public class CollectionFilters {

    /**
     * Returns the contents of a {@link Collection} that are of a specified type, as a {@link List}.
     */
    public static <T> List<T> filterToList(Collection<? extends Object> objects,
        Class<? extends T> clazz) {
        return filteredAndCastStream(objects, clazz).collect(Collectors.toList());
    }

    /**
     * Returns the contents of a {@link Collection} that are of a specified type, as a {@link Set}.
     */
    public static <T> Set<T> filterToSet(Collection<? extends Object> objects,
        Class<? extends T> clazz) {
        return filteredAndCastStream(objects, clazz).collect(Collectors.toSet());
    }

    private static <T> Stream<? extends T> filteredAndCastStream(
        Collection<? extends Object> objects, Class<? extends T> clazz) {
        return objects.stream().filter(s -> clazz.isInstance(s)).map(s -> clazz.cast(s));
    }

    /**
     * Removes from a {@link Collection} all the objects of a specified type. Note that the
     * collection is altered in place.
     */
    public static <T> void removeTypeFromCollection(Collection<? extends Object> objects,
        Class<? extends T> clazz) {
        objects.removeAll(filterToList(objects, clazz));
    }
}
