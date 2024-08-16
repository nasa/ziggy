package gov.nasa.ziggy.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

/**
 * Utilities for use with Java collections.
 *
 * @author PT
 */
public class ZiggyCollectionUtils {

    /**
     * Returns a {@link Set} of elements from a (nominal) subset that are not present in the
     * superset. If the returned set is empty, it means that the subset is really a subset of the
     * superset. Otherwise, not so much.
     */
    public static <T> Set<T> elementsNotInSuperset(Collection<T> superset, Collection<T> subset) {
        if (CollectionUtils.isEmpty(subset)) {
            return new HashSet<>();
        }
        if (CollectionUtils.isEmpty(superset)) {
            return new HashSet<>(subset);
        }
        return subset.stream().filter(s -> !superset.contains(s)).collect(Collectors.toSet());
    }

    /**
     * Returns a mutable {@link Set} of the arguments. This is similar to Set.of except that it
     * returns a mutable Set. Shame on the Java Ruling Class for making everyone reinvent this
     * particular wheel.
     */
    @SafeVarargs
    public static <T> Set<T> mutableSetOf(T... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    /**
     * Returns a mutable {@link List} of the arguments. This is similar to List.of except that it
     * returns a mutable List. Shame on the Java Ruling Class for making everyone reinvent this
     * particular wheel.
     */
    @SafeVarargs
    public static <T> List<T> mutableListOf(T... values) {
        return new ArrayList<>(Arrays.asList(values));
    }
}
