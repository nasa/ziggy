package gov.nasa.ziggy.crud;

import java.util.Collection;
import java.util.List;

/**
 * @author Miles Cote
 */
public interface AbstractCrudInterface<U> {

    <T, R> ZiggyQuery<T, R> createZiggyQuery(Class<T> databaseClass, Class<R> resultClass);

    <R> ZiggyQuery<R, R> createZiggyQuery(Class<R> databaseClass);

    <T, R> List<R> list(ZiggyQuery<T, R> query);

    <T, R> R uniqueResult(ZiggyQuery<T, R> query);

    void persist(Object o);

    void persist(Collection<?> collection);

    Class<U> componentClass();

    default U finalize(U result) {
        return result;
    }

    default List<U> finalize(List<U> result) {
        return result;
    }
}
